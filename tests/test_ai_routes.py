from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi.testclient import TestClient

from app.main import app
from app.routers import ai as ai_router
from app.services.database import get_session


class DummySession:
    async def commit(self) -> None:
        return None


async def fake_session() -> AsyncIterator[DummySession]:
    yield DummySession()


def test_ai_preview_route_returns_structured_response(monkeypatch) -> None:
    class FakeAIReplyService:
        def __init__(self, *, session) -> None:
            self.session = session

        async def generate_preview(self, business_id: int, payload):
            return {
                "run_id": "12",
                "business_id": business_id,
                "phone": payload.phone,
                "customer_message": payload.message,
                "reply_text": "Oui, livraison disponible a Rabat.",
                "intent": "livraison",
                "language": "french",
                "grounded": True,
                "needs_human": False,
                "confidence": 0.9,
                "reason_code": "grounded_answer",
                "follow_up_question": None,
                "decision": "send",
                "used_sources": [
                    {"type": "faq", "id": 4, "name": "Shipping", "score": 0.92, "metadata": {}}
                ],
                "retrieved_sources": [
                    {"type": "faq", "id": 4, "name": "Shipping", "score": 0.92, "metadata": {}}
                ],
                "sent": False,
                "outbound_message": None,
                "created_at": "2026-03-30T12:00:00Z",
                "updated_at": "2026-03-30T12:00:00Z",
            }

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(ai_router, "AIReplyService", FakeAIReplyService)

    with TestClient(app) as client:
        response = client.post(
            "/business/2/ai/reply",
            json={"message": "Kayn livraison l Rabat ?", "phone": "+212600000001"},
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    body = response.json()
    assert body["decision"] == "send"
    assert body["intent"] == "livraison"
    assert body["used_sources"][0]["type"] == "faq"


def test_ai_runs_routes_return_saved_runs(monkeypatch) -> None:
    class FakeAIReplyService:
        def __init__(self, *, session) -> None:
            self.session = session

        async def list_runs(self, business_id: int, limit: int = 50):
            assert business_id == 3
            assert limit == 25
            return [
                {
                    "id": "21",
                    "business_id": business_id,
                    "phone": "+212600000001",
                    "status": "sent",
                    "customer_message": "prix ?",
                    "reply_text": "299 MAD.",
                    "language": "french",
                    "intent": "prix",
                    "needs_human": False,
                    "confidence": 0.95,
                    "fallback_reason": None,
                    "created_at": "2026-03-30T12:00:00Z",
                    "updated_at": "2026-03-30T12:00:01Z",
                }
            ]

        async def get_run(self, business_id: int, run_id: int):
            assert business_id == 3
            assert run_id == 21
            return {
                "id": "21",
                "business_id": business_id,
                "phone": "+212600000001",
                "status": "sent",
                "customer_message": "prix ?",
                "reply_text": "299 MAD.",
                "language": "french",
                "intent": "prix",
                "needs_human": False,
                "confidence": 0.95,
                "fallback_reason": None,
                "created_at": "2026-03-30T12:00:00Z",
                "updated_at": "2026-03-30T12:00:01Z",
                "provider": "openai",
                "model": "gpt-4.1-mini",
                "prompt_version": "zakbot-grounded-reply-v1",
                "inbound_chat_message_id": "44",
                "outbound_chat_message_id": "45",
                "retrieval_summary": {"selected_sources": []},
                "request_payload": {"prompt_version": "zakbot-grounded-reply-v1"},
                "response_payload": {"structured_reply": {"reply_text": "299 MAD."}},
            }

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(ai_router, "AIReplyService", FakeAIReplyService)

    with TestClient(app) as client:
        list_response = client.get("/business/3/ai/runs?limit=25")
        detail_response = client.get("/business/3/ai/runs/21")

    app.dependency_overrides.clear()
    assert list_response.status_code == 200
    assert detail_response.status_code == 200
    assert list_response.json()[0]["status"] == "sent"
    assert detail_response.json()["provider"] == "openai"
