from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from evaluator.config import EvalSettings
from evaluator.models import QueryRecord, ServiceResult


class ServiceCaller:
    def __init__(self, settings: EvalSettings) -> None:
        self.settings = settings

    async def run(self, queries: list[QueryRecord]) -> list[ServiceResult]:
        semaphore = asyncio.Semaphore(self.settings.max_concurrency)
        timeout = httpx.Timeout(self.settings.request_timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            tasks = [self._call_query(client, semaphore, query) for query in queries]
            return await asyncio.gather(*tasks)

    async def _call_query(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        query: QueryRecord,
    ) -> ServiceResult:
        url = (
            f"{self.settings.normalized_base_url}/business/"
            f"{self.settings.business_id}/ai/reply"
        )
        payload = {
            "message": query.query_text,
            "phone": self._synthetic_phone(query.id),
        }
        start = time.perf_counter()

        async with semaphore:
            try:
                response = await client.post(url, json=payload)
                latency_ms = round((time.perf_counter() - start) * 1000, 2)
            except httpx.TimeoutException:
                latency_ms = round((time.perf_counter() - start) * 1000, 2)
                return self._build_error_result(query, "timeout", latency_ms, "Request timed out.")
            except Exception as exc:
                latency_ms = round((time.perf_counter() - start) * 1000, 2)
                return self._build_error_result(query, "exception", latency_ms, str(exc))

        try:
            raw_payload = response.json()
        except ValueError:
            raw_payload = {"raw_text": response.text}

        ai_response = self._extract_reply_text(raw_payload)
        status = "success"
        error = None
        if response.is_error:
            status = "http_error"
            error = f"HTTP {response.status_code}"
        elif ai_response is None:
            status = "invalid_response"
            error = "Reply text not found in response payload."

        return ServiceResult(
            query_id=query.id,
            query_text=query.query_text,
            language=query.language,
            topic=query.topic,
            expected_intent=query.expected_intent,
            status=status,
            http_status=response.status_code,
            latency_ms=latency_ms,
            ai_response=ai_response,
            raw_payload=raw_payload if isinstance(raw_payload, dict) else {"payload": raw_payload},
            error=error,
            service_language=self._extract_optional_str(raw_payload, "language"),
            service_intent=self._extract_optional_str(raw_payload, "intent"),
            service_decision=self._extract_optional_str(raw_payload, "decision"),
        )

    def _build_error_result(
        self,
        query: QueryRecord,
        status: str,
        latency_ms: float,
        error: str,
    ) -> ServiceResult:
        return ServiceResult(
            query_id=query.id,
            query_text=query.query_text,
            language=query.language,
            topic=query.topic,
            expected_intent=query.expected_intent,
            status=status,
            latency_ms=latency_ms,
            error=error,
        )

    @staticmethod
    def _extract_reply_text(payload: Any) -> str | None:
        if isinstance(payload, dict):
            for key in ("reply_text", "response", "answer", "message"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            structured = payload.get("structured_reply")
            if isinstance(structured, dict):
                value = structured.get("reply_text")
                if isinstance(value, str) and value.strip():
                    return value.strip()
        if isinstance(payload, str) and payload.strip():
            return payload.strip()
        return None

    @staticmethod
    def _extract_optional_str(payload: Any, key: str) -> str | None:
        if isinstance(payload, dict):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _synthetic_phone(query_id: str) -> str:
        suffix = "".join(char for char in query_id if char.isdigit())[-6:].rjust(6, "0")
        return f"+212600{suffix}"
