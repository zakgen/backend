from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi.testclient import TestClient

from app.main import app
from app.routers import messaging as messaging_router
from app.services.auth import AuthenticatedUser, require_business_access
from app.services.database import get_session


class DummySession:
    async def commit(self) -> None:
        return None


async def fake_session() -> AsyncIterator[DummySession]:
    yield DummySession()


async def fake_current_user() -> AuthenticatedUser:
    return AuthenticatedUser(auth_user_id="user-1", email="owner@example.com")


def test_reply_route_returns_conversation_message(monkeypatch) -> None:
    class FakeMessagingService:
        def __init__(self, *, session, provider) -> None:
            self.session = session
            self.provider = provider

        async def send_reply(self, business_id: int, phone: str, payload):
            return {
                "id": "55",
                "phone": phone,
                "text": payload.text,
                "direction": "outbound",
                "timestamp": "2026-03-30T10:00:00Z",
                "intent": payload.intent,
                "needs_human": payload.needs_human,
            }

    app.dependency_overrides[get_session] = fake_session
    app.dependency_overrides[require_business_access] = fake_current_user
    monkeypatch.setattr(messaging_router, "MessagingService", FakeMessagingService)

    with TestClient(app) as client:
        response = client.post(
            "/business/2/chats/+212600000001/reply",
            json={"text": "Salam, livraison dispo.", "intent": "livraison"},
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    body = response.json()
    assert body["direction"] == "outbound"
    assert body["phone"] == "+212600000001"
    assert body["intent"] == "livraison"


def test_inbound_webhook_route_returns_accepted(monkeypatch) -> None:
    class FakeMessagingService:
        def __init__(self, *, session, provider) -> None:
            self.session = session
            self.provider = provider

        async def handle_inbound_webhook(self, *, url, headers, params):
            assert params["MessageSid"] == "SM123"
            return {"id": 99}

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(messaging_router, "MessagingService", FakeMessagingService)

    with TestClient(app) as client:
        response = client.post(
            "/webhooks/twilio/whatsapp/inbound",
            data={"MessageSid": "SM123", "From": "whatsapp:+2126", "To": "whatsapp:+2127"},
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json() == {"status": "accepted", "id": "99"}


def test_status_webhook_route_ignores_unknown_message(monkeypatch) -> None:
    class FakeMessagingService:
        def __init__(self, *, session, provider) -> None:
            self.session = session
            self.provider = provider

        async def handle_status_webhook(self, *, url, headers, params):
            assert params["MessageSid"] == "SM404"
            return None

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(messaging_router, "MessagingService", FakeMessagingService)

    with TestClient(app) as client:
        response = client.post(
            "/webhooks/twilio/whatsapp/status",
            data={"MessageSid": "SM404", "MessageStatus": "delivered"},
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json() == {"status": "ignored", "reason": "unknown_message_sid"}
