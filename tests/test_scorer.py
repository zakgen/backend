import pytest

from evaluator.config import EvalSettings
from evaluator.models import QueryRecord, ServiceResult
from evaluator.scorer import ResponseScorer


@pytest.mark.asyncio
async def test_scorer_assigns_failure_scores_without_calling_judge() -> None:
    settings = EvalSettings(openai_api_key=None)
    scorer = ResponseScorer(settings)
    queries = [
        QueryRecord(
            id="en_orders_001",
            language="english",
            topic="orders",
            query_text="Where is my order?",
            expected_intent="order_status",
        )
    ]
    results = [
        ServiceResult(
            query_id="en_orders_001",
            query_text="Where is my order?",
            language="english",
            topic="orders",
            expected_intent="order_status",
            status="timeout",
            latency_ms=2000.0,
            error="Request timed out.",
        )
    ]

    scored = await scorer.run(queries=queries, results=results, business_profile={"business": {}})

    assert len(scored) == 1
    assert scored[0].scores.passed is False
    assert scored[0].scores.relevance == 1
    assert "timeout" in scored[0].scores.failure_tags

