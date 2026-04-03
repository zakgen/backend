from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi.testclient import TestClient

from app.main import app
from app.routers import order_confirmations as order_confirmations_router
from app.services.database import get_session


class DummySession:
    async def commit(self) -> None:
        return None


async def fake_session() -> AsyncIterator[DummySession]:
    yield DummySession()


def _detail_payload() -> dict:
    return {
        "id": 21,
        "order_id": 10,
        "business_id": 2,
        "phone": "+212600000001",
        "customer_name": "Lina",
        "preferred_language": "french",
        "status": "awaiting_customer",
        "needs_human": False,
        "last_detected_intent": "order_confirmation_pending",
        "started_at": "2026-04-03T12:00:00Z",
        "last_customer_message_at": None,
        "confirmed_at": None,
        "declined_at": None,
        "updated_at": "2026-04-03T12:00:00Z",
        "structured_snapshot": {},
        "order": {
            "id": 10,
            "business_id": 2,
            "source_store": "generic",
            "external_order_id": "WC-1001",
            "customer_name": "Lina",
            "customer_phone": "+212600000001",
            "preferred_language": "french",
            "total_amount": 3499,
            "currency": "MAD",
            "payment_method": None,
            "delivery_city": "Casablanca",
            "delivery_address": "Maarif",
            "order_notes": None,
            "status": "pending_confirmation",
            "confirmation_status": "awaiting_customer",
            "items": [{"product_name": "Redmi Note 13", "quantity": 1}],
            "metadata": {},
            "created_at": "2026-04-03T12:00:00Z",
            "updated_at": "2026-04-03T12:00:00Z",
        },
        "events": [],
    }


def test_ingest_order_route_returns_session(monkeypatch) -> None:
    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def ingest_store_order(self, business_id: int, payload):
            return {
                "order": _detail_payload()["order"],
                "session": _detail_payload(),
                "confirmation_message_sent": True,
            }

        async def get_session_detail(self, business_id: int, session_id: int):
            return _detail_payload()

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(
        order_confirmations_router,
        "OrderConfirmationService",
        FakeOrderConfirmationService,
    )

    with TestClient(app) as client:
        response = client.post(
            "/business/2/order-confirmations/orders",
            json={
                "source_store": "generic",
                "external_order_id": "WC-1001",
                "customer_name": "Lina",
                "customer_phone": "+212600000001",
                "preferred_language": "french",
                "total_amount": 3499,
                "currency": "MAD",
                "delivery_city": "Casablanca",
                "delivery_address": "Maarif",
                "items": [{"product_name": "Redmi Note 13", "quantity": 1}],
            },
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    body = response.json()
    assert body["confirmation_message_sent"] is True
    assert body["session"]["status"] == "awaiting_customer"


def test_list_sessions_route_returns_rows(monkeypatch) -> None:
    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def list_sessions(self, business_id: int, *, status_value=None, limit=50):
            return [_detail_payload()]

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(
        order_confirmations_router,
        "OrderConfirmationService",
        FakeOrderConfirmationService,
    )

    with TestClient(app) as client:
        response = client.get("/business/2/order-confirmations/sessions")

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json()["total"] == 1


def test_apply_action_route_returns_detail(monkeypatch) -> None:
    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def apply_action(self, business_id: int, session_id: int, payload):
            detail = _detail_payload()
            detail["status"] = "confirmed"
            detail["order"]["confirmation_status"] = "confirmed"
            return detail

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(
        order_confirmations_router,
        "OrderConfirmationService",
        FakeOrderConfirmationService,
    )

    with TestClient(app) as client:
        response = client.post(
            "/business/2/order-confirmations/sessions/21/actions",
            json={"action": "confirm"},
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json()["status"] == "confirmed"

