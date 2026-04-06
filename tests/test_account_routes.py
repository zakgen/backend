from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi.testclient import TestClient

from app.main import app
from app.routers import account as account_router
from app.services.auth import AuthenticatedUser, require_authenticated_user
from app.services.database import get_session


class DummySession:
    async def commit(self) -> None:
        return None


async def fake_session() -> AsyncIterator[DummySession]:
    yield DummySession()


async def fake_current_user() -> AuthenticatedUser:
    return AuthenticatedUser(auth_user_id="user-1", email="owner@example.com")


def test_list_my_businesses_returns_empty_state(monkeypatch) -> None:
    class FakeAccountService:
        def __init__(self, *, session) -> None:
            self.session = session

        async def list_businesses(self, current_user):
            return [], None

    app.dependency_overrides[get_session] = fake_session
    app.dependency_overrides[require_authenticated_user] = fake_current_user
    monkeypatch.setattr(account_router, "AccountService", FakeAccountService)

    with TestClient(app) as client:
        response = client.get("/me/businesses")

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json() == {"businesses": [], "current_business_id": None}


def test_create_my_business_returns_created_business(monkeypatch) -> None:
    class FakeAccountService:
        def __init__(self, *, session) -> None:
            self.session = session

        async def create_business(self, *, current_user, payload):
            return {
                "id": 7,
                "name": payload.name,
                "description": payload.description,
                "city": payload.city,
                "shipping_policy": payload.shipping_policy,
                "delivery_zones": payload.delivery_zones,
                "payment_methods": payload.payment_methods,
                "profile_metadata": payload.profile_metadata,
                "updated_at": None,
            }

    app.dependency_overrides[get_session] = fake_session
    app.dependency_overrides[require_authenticated_user] = fake_current_user
    monkeypatch.setattr(account_router, "AccountService", FakeAccountService)

    with TestClient(app) as client:
        response = client.post(
            "/me/businesses",
            json={
                "name": "Atlas Gadget Hub",
                "description": "Phones and gadgets",
                "city": "Casablanca",
                "delivery_zones": ["Casablanca"],
                "payment_methods": ["cash_on_delivery"],
            },
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json()["id"] == 7
    assert response.json()["name"] == "Atlas Gadget Hub"


def test_get_my_business_requires_authentication() -> None:
    with TestClient(app) as client:
        response = client.get("/me/business")

    assert response.status_code == 401
