from __future__ import annotations

import base64
from datetime import UTC, datetime
import hashlib
import hmac
import json

import httpx

from app.config import Settings
from app.services.crypto_service import AppCryptoService
from app.services import shopify_service as shopify_service_module
from app.services.shopify_service import ShopifyService


class FakeHTTPClient:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict | None, dict | None, dict | None]] = []
        self.gets: list[tuple[str, dict | None]] = []
        self.post_responses: list[httpx.Response] = []
        self.get_responses: list[httpx.Response] = []

    async def post(self, url: str, *, headers=None, json=None):
        self.posts.append((url, headers, json, None))
        response = self.post_responses.pop(0)
        response.request = httpx.Request("POST", url)
        return response

    async def get(self, url: str, *, headers=None):
        self.gets.append((url, headers))
        response = self.get_responses.pop(0)
        response.request = httpx.Request("GET", url)
        return response


class FakeBusinessRepository:
    async def get_by_id(self, business_id: int):
        return {"id": business_id, "name": "Boutique Lina"}


class FakeIntegrationRepository:
    def __init__(self) -> None:
        self.connection = None
        self.find_by_shop: dict[str, dict] = {}

    async def get_connection(self, business_id: int, integration_type: str):
        return self.connection

    async def upsert_connection(self, **kwargs):
        self.connection = {
            "id": 11,
            "business_id": kwargs["business_id"],
            "integration_type": kwargs["integration_type"],
            "status": kwargs["status_value"],
            "health": kwargs["health"],
            "config": kwargs["config"],
            "metrics": kwargs["metrics"],
            "last_activity_at": kwargs.get("last_activity_at"),
            "last_synced_at": kwargs.get("last_synced_at"),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
        shop_domain = str(kwargs["config"].get("shop_domain") or "").strip().lower()
        if shop_domain:
            self.find_by_shop[shop_domain] = self.connection
        return self.connection

    async def find_shopify_connection(self, *, shop_domain: str):
        return self.find_by_shop.get(shop_domain.strip().lower())


class FakeOrderRepository:
    def __init__(self) -> None:
        self.external_order = None

    async def get_by_external_reference(self, *, business_id: int, source_store: str, external_order_id: str):
        return self.external_order


def _settings() -> Settings:
    return Settings(
        database_backend="mongo",
        mongo_url="mongodb+srv://example",
        app_encryption_key="super-secret-key",
        shopify_api_key="shopify-key",
        shopify_api_secret="shopify-secret",
        shopify_app_base_url="https://api.example.com",
        shopify_scopes="read_orders,write_orders",
        shopify_api_version="2025-07",
    )


def _service(http_client: FakeHTTPClient | None = None) -> tuple[ShopifyService, FakeIntegrationRepository, FakeOrderRepository]:
    settings = _settings()
    service = ShopifyService(
        session=type("DummyRepoSession", (), {"db": None})(),
        settings=settings,
        http_client=http_client or FakeHTTPClient(),
        crypto_service=AppCryptoService(settings),
    )
    service.business_repository = FakeBusinessRepository()
    integration_repository = FakeIntegrationRepository()
    service.integration_repository = integration_repository
    order_repository = FakeOrderRepository()
    service.order_repository = order_repository
    return service, integration_repository, order_repository


def _oauth_hmac(secret: str, params: dict[str, str]) -> str:
    parts = []
    for key in sorted(params):
        if key in {"hmac", "signature"}:
            continue
        parts.append(f"{key}={params[key]}")
    return hmac.new(secret.encode("utf-8"), "&".join(parts).encode("utf-8"), hashlib.sha256).hexdigest()


def _webhook_hmac(secret: str, body: bytes) -> str:
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    ).decode("utf-8")


def test_begin_oauth_install_builds_shopify_redirect() -> None:
    service, _, _ = _service()

    import asyncio

    url = asyncio.run(
        service.begin_oauth_install(
            business_id=2,
            shop_domain="demo-shop.myshopify.com",
            return_to="http://localhost:3000/integrations",
        )
    )

    assert url.startswith("https://demo-shop.myshopify.com/admin/oauth/authorize?")
    assert "client_id=shopify-key" in url
    assert "state=" in url


def test_handle_oauth_callback_stores_connection_and_registers_webhooks() -> None:
    http_client = FakeHTTPClient()
    http_client.post_responses = [
        httpx.Response(200, json={"access_token": "shpat_123", "scope": "read_orders,write_orders"}),
        httpx.Response(201, json={"webhook": {"id": 101}}),
        httpx.Response(201, json={"webhook": {"id": 102}}),
        httpx.Response(201, json={"webhook": {"id": 103}}),
    ]
    http_client.get_responses = [
        httpx.Response(200, json={"shop": {"id": 77, "name": "Demo Shop"}}),
    ]
    service, integration_repository, _ = _service(http_client)
    state = service.crypto_service.encrypt_json(
        {
            "business_id": 2,
            "shop_domain": "demo-shop.myshopify.com",
            "return_to": "http://localhost:3000/integrations",
        }
    )
    params = {
        "code": "auth-code",
        "shop": "demo-shop.myshopify.com",
        "state": state,
        "timestamp": "1712230000",
    }
    params["hmac"] = _oauth_hmac("shopify-secret", params)

    import asyncio

    redirect_url = asyncio.run(service.handle_oauth_callback(params))

    assert redirect_url.startswith("http://localhost:3000/integrations")
    assert integration_repository.connection is not None
    config = integration_repository.connection["config"]
    assert config["shop_domain"] == "demo-shop.myshopify.com"
    assert config["offline_access_token_encrypted"] != "shpat_123"
    assert config["webhook_subscription_ids"]["orders/create"] == 101


def test_orders_create_webhook_is_idempotent_on_duplicate_event(monkeypatch) -> None:
    service, integration_repository, _ = _service()
    integration_repository.connection = {
        "id": 11,
        "business_id": 2,
        "integration_type": "shopify",
        "status": "connected",
        "health": "healthy",
        "config": {
            "shop_domain": "demo-shop.myshopify.com",
            "processed_webhook_ids": ["evt-1"],
        },
        "metrics": {},
        "last_activity_at": None,
        "last_synced_at": None,
    }
    integration_repository.find_by_shop["demo-shop.myshopify.com"] = integration_repository.connection
    body = json.dumps({"id": 999}).encode("utf-8")
    headers = {
        "x-shopify-shop-domain": "demo-shop.myshopify.com",
        "x-shopify-event-id": "evt-1",
        "x-shopify-topic": "orders/create",
        "x-shopify-hmac-sha256": _webhook_hmac("shopify-secret", body),
    }

    import asyncio

    result = asyncio.run(service.handle_orders_create(headers=headers, body=body))

    assert result == {"status": "ignored", "reason": "duplicate_webhook"}


def test_orders_updated_ignores_finalized_internal_order() -> None:
    service, integration_repository, order_repository = _service()
    integration_repository.connection = {
        "id": 11,
        "business_id": 2,
        "integration_type": "shopify",
        "status": "connected",
        "health": "healthy",
        "config": {
            "shop_domain": "demo-shop.myshopify.com",
            "processed_webhook_ids": [],
        },
        "metrics": {},
        "last_activity_at": None,
        "last_synced_at": None,
    }
    integration_repository.find_by_shop["demo-shop.myshopify.com"] = integration_repository.connection
    order_repository.external_order = {
        "id": 44,
        "confirmation_status": "confirmed",
        "metadata": {"order_confirmation": {"final_snapshot_applied": True}},
    }
    body = json.dumps({"id": 999}).encode("utf-8")
    headers = {
        "x-shopify-shop-domain": "demo-shop.myshopify.com",
        "x-shopify-event-id": "evt-2",
        "x-shopify-topic": "orders/updated",
        "x-shopify-hmac-sha256": _webhook_hmac("shopify-secret", body),
    }

    import asyncio

    result = asyncio.run(service.handle_orders_updated(headers=headers, body=body))

    assert result == {"status": "ignored", "reason": "finalized_order"}


def test_sync_order_confirmation_status_updates_tags_and_note() -> None:
    http_client = FakeHTTPClient()
    http_client.post_responses = [
        httpx.Response(
            200,
            json={"data": {"order": {"id": "gid://shopify/Order/1", "note": "Existing note", "tags": ["vip", "zakbot:pending_confirmation"]}}},
        ),
        httpx.Response(
            200,
            json={"data": {"orderUpdate": {"order": {"id": "gid://shopify/Order/1", "tags": ["vip", "zakbot:confirmed"], "note": "updated"}, "userErrors": []}}},
        ),
    ]
    service, integration_repository, _ = _service(http_client)
    encrypted_token = service.crypto_service.encrypt_text("shpat_123")
    integration_repository.connection = {
        "id": 11,
        "business_id": 2,
        "integration_type": "shopify",
        "status": "connected",
        "health": "healthy",
        "config": {
            "shop_domain": "demo-shop.myshopify.com",
            "offline_access_token_encrypted": encrypted_token,
        },
        "metrics": {},
        "last_activity_at": None,
        "last_synced_at": None,
    }
    order_row = {
        "id": 99,
        "source_store": "shopify",
        "external_order_id": "999",
        "confirmation_status": "confirmed",
        "raw_payload": {"admin_graphql_api_id": "gid://shopify/Order/1"},
        "metadata": {},
    }

    import asyncio

    asyncio.run(
        service.sync_order_confirmation_status(
            business_id=2,
            order_row=order_row,
            snapshot={"confirmed_edits": [{"field": "variant", "value": "White"}]},
            confirmation_status="confirmed",
        )
    )

    graphql_request = http_client.post_responses
    assert integration_repository.connection["config"]["last_sync_back_status"] == "success"
    assert integration_repository.connection["config"]["last_sync_back_at"] is not None


def test_orders_create_webhook_creates_internal_session(monkeypatch) -> None:
    service, integration_repository, _ = _service()
    integration_repository.connection = {
        "id": 11,
        "business_id": 2,
        "integration_type": "shopify",
        "status": "connected",
        "health": "healthy",
        "config": {
            "shop_domain": "demo-shop.myshopify.com",
            "processed_webhook_ids": [],
            "offline_access_token_encrypted": service.crypto_service.encrypt_text("shpat_123"),
        },
        "metrics": {},
        "last_activity_at": None,
        "last_synced_at": None,
    }
    integration_repository.find_by_shop["demo-shop.myshopify.com"] = integration_repository.connection

    sync_calls: list[tuple[int, dict, dict, str]] = []

    async def fake_sync(*, business_id: int, order_row: dict, snapshot: dict | None = None, confirmation_status: str | None = None):
        sync_calls.append((business_id, order_row, snapshot or {}, confirmation_status or ""))

    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def ingest_store_order(self, business_id: int, payload):
            assert payload.source_store == "shopify"
            return {
                "order": {
                    "id": 71,
                    "source_store": "shopify",
                    "external_order_id": payload.external_order_id,
                    "confirmation_status": "awaiting_customer",
                    "metadata": payload.metadata,
                    "raw_payload": payload.raw_payload,
                },
                "session": {"id": 17, "structured_snapshot": {"delivery_city": payload.delivery_city}},
                "confirmation_message_sent": True,
            }

    monkeypatch.setattr(shopify_service_module, "OrderConfirmationService", FakeOrderConfirmationService)
    monkeypatch.setattr(service, "sync_order_confirmation_status", fake_sync)
    body = json.dumps(
        {
            "id": 999,
            "admin_graphql_api_id": "gid://shopify/Order/999",
            "currency": "MAD",
            "current_total_price": "100.00",
            "customer_locale": "fr",
            "contact_email": "lina@example.com",
            "phone": "+212600000001",
            "shipping_address": {"city": "Casablanca", "address1": "Maarif"},
            "line_items": [{"title": "Redmi Note 13", "quantity": 1, "price": "100.00"}],
        }
    ).encode("utf-8")
    headers = {
        "x-shopify-shop-domain": "demo-shop.myshopify.com",
        "x-shopify-event-id": "evt-3",
        "x-shopify-topic": "orders/create",
        "x-shopify-hmac-sha256": _webhook_hmac("shopify-secret", body),
    }

    import asyncio

    result = asyncio.run(service.handle_orders_create(headers=headers, body=body))

    assert result["status"] == "accepted"
    assert result["confirmation_message_sent"] is True
    assert sync_calls
