from __future__ import annotations

import asyncio
import json
from typing import Any

from openai import AsyncOpenAI

from evaluator.config import EvalSettings
from evaluator.models import JudgeScores, QueryRecord, ScoredResult, ServiceResult
from evaluator.utils import slugify_tag


SYSTEM_PROMPT = """You are a strict QA judge for a business assistant chatbot.

You must score the assistant reply against the business profile only.
Do not reward plausible guesses. Penalize contradictions, unsupported claims,
missing details, wrong language, and invented policies or prices.
Order management is out of scope for this app. For order status, changes, cancellations,
or complaints, a clear support handoff should be rewarded when it explicitly says the app
does not manage the request directly and provides the correct support path.

Return valid JSON only with these keys:
- relevance: integer 1-5
- accuracy: integer 1-5
- language_match: integer 1-5
- completeness: integer 1-5
- tone: integer 1-5
- hallucination_risk: integer 1-5
- failure_tags: array of short snake_case strings
- reasoning: short string

Score 5 on hallucination_risk when the answer stays grounded and invents nothing.
Score 1 on hallucination_risk when the answer clearly invents unsupported facts.
"""


class ResponseScorer:
    def __init__(self, settings: EvalSettings) -> None:
        self.settings = settings
        self._client: AsyncOpenAI | None = None

    def _get_client(self) -> AsyncOpenAI:
        if self.settings.openai_api_key is None:
            raise RuntimeError("OPENAI_API_KEY is required to score responses with the judge model.")
        if self._client is None:
            self._client = AsyncOpenAI(api_key=self.settings.openai_api_key.get_secret_value())
        return self._client

    async def run(
        self,
        queries: list[QueryRecord],
        results: list[ServiceResult],
        business_profile: dict[str, Any],
    ) -> list[ScoredResult]:
        query_map = {query.id: query for query in queries}
        semaphore = asyncio.Semaphore(self.settings.judge_max_concurrency)
        tasks = [
            self._score_result(semaphore, query_map[result.query_id], result, business_profile)
            for result in results
        ]
        return await asyncio.gather(*tasks)

    async def _score_result(
        self,
        semaphore: asyncio.Semaphore,
        query: QueryRecord,
        result: ServiceResult,
        business_profile: dict[str, Any],
    ) -> ScoredResult:
        if result.status != "success" or not result.ai_response:
            scores = JudgeScores(
                relevance=1,
                accuracy=1,
                language_match=1,
                completeness=1,
                tone=2 if result.status in {"timeout", "http_error"} else 1,
                hallucination_risk=1,
                failure_tags=[slugify_tag(result.status)],
                reasoning=(
                    "The backend did not return a usable AI reply, so the evaluation defaults "
                    "to a failing score."
                ),
            )
            return ScoredResult(query=query, result=result, scores=scores)

        async with semaphore:
            payload = {
                "business_profile": business_profile,
                "query": query.model_dump(),
                "service_result": result.model_dump(),
                "instructions": {
                    "ground_truth_source": "business_profile",
                    "language_rule": "The reply should match the query language.",
                    "scoring_scale": "1 is poor, 5 is excellent.",
                    "order_scope_rule": (
                        "Order-management questions should be treated as support handoff cases, "
                        "not as in-app procedural flows."
                    ),
                },
            }
            response = await self._get_client().chat.completions.create(
                model=self.settings.judge_model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
            )

        content = response.choices[0].message.content if response.choices else None
        if not content:
            raise RuntimeError(f"Judge model returned an empty payload for query {query.id}.")

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Judge model returned invalid JSON for query {query.id}.") from exc

        parsed["failure_tags"] = [slugify_tag(tag) for tag in parsed.get("failure_tags", [])]
        scores = JudgeScores.model_validate(parsed)
        return ScoredResult(query=query, result=result, scores=scores)
