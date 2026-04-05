from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi.testclient import TestClient

from app.main import app
from app.routers import shopify as shopify_router
from app.services.database import get_session


class DummySession:
    async def commit(self) -> None:
        return None


async def fake_session() -> AsyncIterator[DummySession]:
    yield DummySession()


def test_connect_shopify_route_redirects(monkeypatch) -> None:
    class FakeShopifyService:
        def __init__(self, *, session, **kwargs) -> None:
            self.session = session

        async def begin_oauth_install(self, *, business_id: int, shop_domain: str, return_to: str | None = None):
            assert business_id == 2
            assert shop_domain == "demo-shop.myshopify.com"
            assert return_to == "http://localhost:3000/integrations"
            return "https://demo-shop.myshopify.com/admin/oauth/authorize?state=abc"

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(shopify_router, "ShopifyService", FakeShopifyService)

    with TestClient(app) as client:
        response = client.get(
            "/business/2/integrations/shopify/connect",
            params={
                "shop": "demo-shop.myshopify.com",
                "return_to": "http://localhost:3000/integrations",
            },
            follow_redirects=False,
        )

    app.dependency_overrides.clear()
    assert response.status_code == 307
    assert response.headers["location"].startswith("https://demo-shop.myshopify.com")


def test_shopify_callback_route_redirects_to_frontend(monkeypatch) -> None:
    class FakeShopifyService:
        def __init__(self, *, session, **kwargs) -> None:
            self.session = session

        async def handle_oauth_callback(self, query_params):
            assert query_params["shop"] == "demo-shop.myshopify.com"
            return "http://localhost:3000/integrations?shopify_status=connected"

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(shopify_router, "ShopifyService", FakeShopifyService)

    with TestClient(app) as client:
        response = client.get(
            "/integrations/shopify/callback",
            params={"shop": "demo-shop.myshopify.com", "code": "abc", "hmac": "xyz", "state": "123"},
            follow_redirects=False,
        )

    app.dependency_overrides.clear()
    assert response.status_code == 307
    assert response.headers["location"].startswith("http://localhost:3000/integrations")


def test_shopify_order_create_webhook_route_returns_status(monkeypatch) -> None:
    class FakeShopifyService:
        def __init__(self, *, session, **kwargs) -> None:
            self.session = session

        async def handle_orders_create(self, *, headers, body):
            assert body == b'{"id": 10}'
            return {"status": "accepted"}

    app.dependency_overrides[get_session] = fake_session
    monkeypatch.setattr(shopify_router, "ShopifyService", FakeShopifyService)

    with TestClient(app) as client:
        response = client.post(
            "/webhooks/shopify/orders/create",
            content=b'{"id": 10}',
            headers={"x-shopify-topic": "orders/create"},
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json() == {"status": "accepted"}
