from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import base64
import hashlib
import hmac
import json
import logging
import urllib.parse
from typing import Any

import httpx
from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.schemas.order_confirmation import StoreOrderIngestRequest
from app.services.crypto_service import AppCryptoService
from app.services.dashboard_service import to_iso
from app.services.order_confirmation_service import OrderConfirmationService
from app.services.repository_factory import RepositoryFactory
from app.services.twilio_provider import TwilioMessagingProvider


logger = logging.getLogger(__name__)

PENDING_CONFIRMATION_STATUSES = {"pending_send", "awaiting_customer", "edit_requested"}
WEBHOOK_TOPICS = {
    "orders/create": "/webhooks/shopify/orders/create",
    "orders/updated": "/webhooks/shopify/orders/updated",
    "app/uninstalled": "/webhooks/shopify/app/uninstalled",
}


class ShopifyService:
    def __init__(
        self,
        *,
        session: AsyncSession,
        settings: Settings | None = None,
        http_client: httpx.AsyncClient | None = None,
        crypto_service: AppCryptoService | None = None,
    ) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.http_client = http_client or httpx.AsyncClient(timeout=20.0)
        self.crypto_service = crypto_service or AppCryptoService(self.settings)
        factory = RepositoryFactory(session, self.settings)
        self.business_repository = factory.business()
        self.integration_repository = factory.integrations()
        self.order_repository = factory.orders()
        self.order_confirmation_repository = factory.order_confirmations()

    async def begin_oauth_install(
        self,
        *,
        business_id: int,
        shop_domain: str,
        return_to: str | None = None,
    ) -> str:
        await self.business_repository.get_by_id(business_id)
        shop = self._normalize_shop_domain(shop_domain)
        state = self.crypto_service.encrypt_json(
            {
                "business_id": business_id,
                "shop_domain": shop,
                "return_to": return_to,
            }
        )
        params = {
            "client_id": self._require_shopify_api_key(),
            "scope": self.settings.shopify_scopes,
            "redirect_uri": self._callback_url(),
            "state": state,
        }
        return f"https://{shop}/admin/oauth/authorize?{urllib.parse.urlencode(params)}"

    async def handle_oauth_callback(self, query_params: Mapping[str, str]) -> str:
        self._verify_oauth_hmac(query_params)
        if "error" in query_params:
            return self._build_callback_redirect(
                None,
                status_value="error",
                message=str(query_params.get("error_description") or query_params["error"]),
            )

        encrypted_state = str(query_params.get("state") or "").strip()
        if not encrypted_state:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing Shopify OAuth state.",
            )
        state = self.crypto_service.decrypt_json(encrypted_state, ttl_seconds=900)
        business_id = int(state["business_id"])
        shop_domain = self._normalize_shop_domain(str(query_params.get("shop") or ""))
        if shop_domain != self._normalize_shop_domain(str(state.get("shop_domain") or "")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Shopify OAuth shop domain mismatch.",
            )

        code = str(query_params.get("code") or "").strip()
        if not code:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing Shopify OAuth code.",
            )

        token_payload = await self._exchange_code_for_token(shop_domain=shop_domain, code=code)
        access_token = str(token_payload["access_token"])
        scopes = sorted(
            {
                scope.strip()
                for scope in str(token_payload.get("scope") or "").split(",")
                if scope.strip()
            }
        )
        shop_info = await self._fetch_shop_info(
            shop_domain=shop_domain,
            access_token=access_token,
        )
        webhook_ids = await self._register_webhooks(
            shop_domain=shop_domain,
            access_token=access_token,
        )
        existing = await self.integration_repository.get_connection(business_id, "shopify")
        config = dict((existing or {}).get("config") or {})
        config.update(
            {
                "shop_domain": shop_domain,
                "offline_access_token_encrypted": self.crypto_service.encrypt_text(
                    access_token
                ),
                "scopes": scopes,
                "shop_id": shop_info.get("id"),
                "shop_name": shop_info.get("name"),
                "install_status": "connected",
                "oauth_completed_at": to_iso(datetime.now(UTC)),
                "webhook_status": "connected",
                "webhook_subscription_ids": webhook_ids,
                "processed_webhook_ids": list(config.get("processed_webhook_ids") or []),
            }
        )
        metrics = dict((existing or {}).get("metrics") or {})
        metrics.setdefault("imported_products", 0)
        metrics.setdefault("received_orders", 0)
        await self.integration_repository.upsert_connection(
            business_id=business_id,
            integration_type="shopify",
            status_value="connected",
            health="healthy",
            config=config,
            metrics=metrics,
            last_activity_at=datetime.now(UTC),
            last_synced_at=datetime.now(UTC),
        )
        return self._build_callback_redirect(
            str(state.get("return_to") or "").strip() or None,
            status_value="connected",
            message=shop_domain,
            business_id=business_id,
            shop_domain=shop_domain,
        )

    async def handle_orders_create(
        self,
        *,
        headers: Mapping[str, str],
        body: bytes,
    ) -> dict[str, Any]:
        return await self._handle_order_webhook(
            headers=headers,
            body=body,
            event_kind="create",
        )

    async def handle_orders_updated(
        self,
        *,
        headers: Mapping[str, str],
        body: bytes,
    ) -> dict[str, Any]:
        return await self._handle_order_webhook(
            headers=headers,
            body=body,
            event_kind="updated",
        )

    async def handle_app_uninstalled(
        self,
        *,
        headers: Mapping[str, str],
        body: bytes,
    ) -> dict[str, Any]:
        self._verify_webhook_hmac(headers=headers, body=body)
        shop_domain = self._normalize_shop_domain(headers.get("x-shopify-shop-domain") or "")
        connection = await self.integration_repository.find_shopify_connection(
            shop_domain=shop_domain
        )
        if connection is None:
            return {"status": "ignored", "reason": "unknown_shop"}
        config = dict(connection.get("config") or {})
        config.update(
            {
                "install_status": "disconnected",
                "webhook_status": "disconnected",
                "offline_access_token_encrypted": None,
                "webhook_subscription_ids": {},
                "last_uninstalled_at": to_iso(datetime.now(UTC)),
            }
        )
        await self.integration_repository.upsert_connection(
            business_id=int(connection["business_id"]),
            integration_type="shopify",
            status_value="disconnected",
            health="attention",
            config=config,
            metrics=dict(connection.get("metrics") or {}),
            last_activity_at=datetime.now(UTC),
            last_synced_at=connection.get("last_synced_at"),
        )
        return {"status": "accepted", "business_id": int(connection["business_id"])}

    async def sync_order_confirmation_status(
        self,
        *,
        business_id: int,
        order_row: dict[str, Any],
        snapshot: dict[str, Any] | None = None,
        confirmation_status: str | None = None,
    ) -> None:
        if str(order_row.get("source_store") or "") != "shopify":
            return
        resolved_status = confirmation_status or str(order_row.get("confirmation_status") or "")
        if resolved_status not in {"confirmed", "human_requested", "declined"}:
            logger.info(
                "Skipping Shopify sync-back for non-terminal status business_id=%s order_id=%s status=%s",
                business_id,
                order_row.get("id"),
                resolved_status,
            )
            return
        connection = await self.integration_repository.get_connection(business_id, "shopify")
        if connection is None or connection.get("status") != "connected":
            return

        config = dict(connection.get("config") or {})
        encrypted_token = str(config.get("offline_access_token_encrypted") or "").strip()
        shop_domain = str(config.get("shop_domain") or "").strip()
        graphql_order_id = self._extract_graphql_order_id(order_row)
        if not encrypted_token or not shop_domain or not graphql_order_id:
            return

        try:
            access_token = self.crypto_service.decrypt_text(encrypted_token)
            current = await self._fetch_order_note_and_tags(
                shop_domain=shop_domain,
                access_token=access_token,
                graphql_order_id=graphql_order_id,
            )
            merged_tags = self._merge_zakbot_tags(
                current_tags=list(current.get("tags") or []),
                confirmation_status=resolved_status,
            )
            note = self._build_shopify_order_note(
                current_note=str(current.get("note") or ""),
                order_row=order_row,
                snapshot=snapshot or {},
                confirmation_status=resolved_status,
            )
            await self._update_shopify_order(
                shop_domain=shop_domain,
                access_token=access_token,
                graphql_order_id=graphql_order_id,
                tags=merged_tags,
                note=note,
            )
            config["last_sync_back_at"] = to_iso(datetime.now(UTC))
            config["last_sync_back_status"] = "success"
            config["last_sync_back_error"] = None
            await self.integration_repository.upsert_connection(
                business_id=business_id,
                integration_type="shopify",
                status_value=str(connection["status"]),
                health="healthy",
                config=config,
                metrics=dict(connection.get("metrics") or {}),
                last_activity_at=connection.get("last_activity_at"),
                last_synced_at=datetime.now(UTC),
            )
        except Exception as exc:
            logger.exception(
                "Shopify sync-back failed for business %s order %s",
                business_id,
                order_row.get("external_order_id"),
                exc_info=exc,
            )
            config["last_sync_back_at"] = to_iso(datetime.now(UTC))
            config["last_sync_back_status"] = "failed"
            config["last_sync_back_error"] = str(exc)
            await self.integration_repository.upsert_connection(
                business_id=business_id,
                integration_type="shopify",
                status_value=str(connection["status"]),
                health="attention",
                config=config,
                metrics=dict(connection.get("metrics") or {}),
                last_activity_at=connection.get("last_activity_at"),
                last_synced_at=connection.get("last_synced_at"),
            )

    async def _handle_order_webhook(
        self,
        *,
        headers: Mapping[str, str],
        body: bytes,
        event_kind: str,
    ) -> dict[str, Any]:
        self._verify_webhook_hmac(headers=headers, body=body)
        shop_domain = self._normalize_shop_domain(headers.get("x-shopify-shop-domain") or "")
        connection = await self.integration_repository.find_shopify_connection(
            shop_domain=shop_domain
        )
        if connection is None:
            return {"status": "ignored", "reason": "unknown_shop"}

        config = dict(connection.get("config") or {})
        webhook_id = str(
            headers.get("x-shopify-event-id")
            or headers.get("x-shopify-webhook-id")
            or ""
        ).strip()
        processed_ids = list(config.get("processed_webhook_ids") or [])
        if webhook_id and webhook_id in processed_ids:
            return {"status": "ignored", "reason": "duplicate_webhook"}

        payload = json.loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Shopify webhook payload must be an object.",
            )
        business_id = int(connection["business_id"])
        event_topic = str(headers.get("x-shopify-topic") or "").strip()
        now = datetime.now(UTC)
        config["last_order_webhook_at"] = to_iso(now)
        config["last_order_webhook_topic"] = event_topic
        config["webhook_status"] = "connected"

        existing_order = await self.order_repository.get_by_external_reference(
            business_id=business_id,
            source_store="shopify",
            external_order_id=str(payload.get("id") or ""),
        )
        existing_session = None
        if existing_order is not None:
            existing_session = await self.order_confirmation_repository.find_latest_by_order(
                business_id,
                int(existing_order["id"]),
            )
        if event_kind == "updated" and existing_order is not None and (
            existing_order.get("confirmation_status") not in PENDING_CONFIRMATION_STATUSES
            or dict(existing_order.get("metadata") or {})
            .get("order_confirmation", {})
            .get("final_snapshot_applied")
        ):
            logger.info(
                "Ignoring Shopify orders/updated for finalized order business_id=%s external_order_id=%s status=%s",
                business_id,
                payload.get("id"),
                existing_order.get("confirmation_status"),
            )
            await self._touch_shopify_connection(
                connection=connection,
                config=config,
                processed_webhook_id=webhook_id,
                received_order_delta=0,
                last_synced_at=connection.get("last_synced_at"),
            )
            return {"status": "ignored", "reason": "finalized_order"}

        order_payload = self._map_shopify_order_to_ingest(payload)
        if order_payload is None:
            config["last_webhook_error"] = "Missing customer phone for Shopify order."
            await self._touch_shopify_connection(
                connection=connection,
                config=config,
                processed_webhook_id=webhook_id,
                received_order_delta=0,
                last_synced_at=connection.get("last_synced_at"),
            )
            return {"status": "ignored", "reason": "missing_phone"}
        should_send_confirmation = self._should_send_confirmation(
            event_kind=event_kind,
            existing_order=existing_order,
            existing_session=existing_session,
        )
        logger.info(
            "Processing Shopify order webhook business_id=%s topic=%s external_order_id=%s existing_order=%s existing_session_status=%s send_confirmation=%s phone=%s",
            business_id,
            event_topic,
            order_payload.external_order_id,
            existing_order is not None,
            None if existing_session is None else existing_session.get("status"),
            should_send_confirmation,
            order_payload.customer_phone,
        )
        if not should_send_confirmation:
            order_payload = order_payload.model_copy(update={"send_confirmation": False})

        result = await OrderConfirmationService(
            session=self.session,
            messaging_provider=TwilioMessagingProvider(),
        ).ingest_store_order(business_id, order_payload)
        logger.info(
            "Shopify order webhook ingested business_id=%s order_id=%s session_id=%s confirmation_message_sent=%s confirmation_status=%s",
            business_id,
            result["order"].get("id"),
            result["session"].get("id"),
            result["confirmation_message_sent"],
            result["order"].get("confirmation_status"),
        )
        await self._touch_shopify_connection(
            connection=connection,
            config=config,
            processed_webhook_id=webhook_id,
            received_order_delta=1 if event_kind == "create" and existing_order is None else 0,
            last_synced_at=datetime.now(UTC),
        )
        return {
            "status": "accepted",
            "business_id": business_id,
            "order_id": int(result["order"]["id"]),
            "confirmation_message_sent": bool(result["confirmation_message_sent"]),
        }

    def _should_send_confirmation(
        self,
        *,
        event_kind: str,
        existing_order: dict[str, Any] | None,
        existing_session: dict[str, Any] | None,
    ) -> bool:
        if existing_order is None:
            return True
        if existing_session is None:
            return str(existing_order.get("confirmation_status") or "") == "pending_send"
        if existing_session.get("status") == "pending_send" and not existing_session.get(
            "last_outbound_message_sid"
        ):
            return True
        return False

    def _map_shopify_order_to_ingest(
        self, payload: dict[str, Any]
    ) -> StoreOrderIngestRequest | None:
        customer = dict(payload.get("customer") or {})
        shipping_address = dict(payload.get("shipping_address") or {})
        phone = (
            shipping_address.get("phone")
            or payload.get("phone")
            or customer.get("phone")
            or ""
        )
        customer_name = (
            shipping_address.get("name")
            or payload.get("contact_email")
            or "Shopify customer"
        )
        if not str(phone).strip():
            return None
        line_items = []
        for row in payload.get("line_items") or []:
            item = dict(row or {})
            line_items.append(
                {
                    "product_name": str(item.get("title") or item.get("name") or "Product"),
                    "quantity": int(item.get("quantity") or 1),
                    "variant": str(item.get("variant_title") or "").strip() or None,
                    "unit_price": float(item.get("price") or 0),
                    "sku": str(item.get("sku") or "").strip() or None,
                }
            )

        locale = str(
            payload.get("customer_locale")
            or customer.get("locale")
            or ""
        ).lower()
        preferred_language = "english"
        if locale.startswith("fr"):
            preferred_language = "french"
        elif locale.startswith("ar"):
            preferred_language = "darija"

        raw_total = payload.get("current_total_price") or payload.get("total_price") or 0
        metadata = {
            "shopify_order_gid": payload.get("admin_graphql_api_id"),
            "shopify_order_name": payload.get("name"),
            "shopify_financial_status": payload.get("financial_status"),
            "shopify_fulfillment_status": payload.get("fulfillment_status"),
        }
        return StoreOrderIngestRequest(
            source_store="shopify",
            external_order_id=str(payload.get("id") or ""),
            customer_name=str(customer_name),
            customer_phone=str(phone),
            preferred_language=preferred_language,
            total_amount=float(raw_total or 0),
            currency=str(payload.get("currency") or "MAD"),
            payment_method=self._normalize_payment_method(payload),
            delivery_city=str(shipping_address.get("city") or "").strip() or None,
            delivery_address=self._build_shipping_address(shipping_address),
            order_notes=str(payload.get("note") or "").strip() or None,
            items=line_items,
            metadata=metadata,
            raw_payload=payload,
            send_confirmation=True,
        )

    def _normalize_payment_method(self, payload: dict[str, Any]) -> str:
        gateways = [
            str(value).strip().lower()
            for value in payload.get("payment_gateway_names") or []
            if str(value).strip()
        ]
        if any("cash" in gateway or "cod" in gateway for gateway in gateways):
            return "cash_on_delivery"
        if gateways:
            return gateways[0].replace(" ", "_")
        return "cash_on_delivery"

    def _build_shipping_address(self, address: dict[str, Any]) -> str | None:
        parts = [
            str(address.get("address1") or "").strip(),
            str(address.get("address2") or "").strip(),
            str(address.get("company") or "").strip(),
            str(address.get("zip") or "").strip(),
        ]
        joined = ", ".join(part for part in parts if part)
        return joined or None

    def _extract_graphql_order_id(self, order_row: dict[str, Any]) -> str | None:
        metadata = dict(order_row.get("metadata") or {})
        raw_payload = dict(order_row.get("raw_payload") or {})
        return (
            str(metadata.get("shopify_order_gid") or "").strip()
            or str(raw_payload.get("admin_graphql_api_id") or "").strip()
            or None
        )

    def _merge_zakbot_tags(
        self,
        *,
        current_tags: list[str],
        confirmation_status: str,
    ) -> list[str]:
        kept = [tag for tag in current_tags if not tag.startswith("zakbot:")]
        status_tag = {
            "pending_send": "zakbot:pending_confirmation",
            "awaiting_customer": "zakbot:pending_confirmation",
            "edit_requested": "zakbot:pending_confirmation",
            "confirmed": "zakbot:confirmed",
            "human_requested": "zakbot:needs_review",
            "declined": "zakbot:cancelled_by_customer",
        }.get(confirmation_status, "zakbot:pending_confirmation")
        kept.append(status_tag)
        return sorted({tag for tag in kept if tag})

    def _build_shopify_order_note(
        self,
        *,
        current_note: str,
        order_row: dict[str, Any],
        snapshot: dict[str, Any],
        confirmation_status: str,
    ) -> str:
        stamp = to_iso(datetime.now(UTC))
        status_label = {
            "pending_send": "pending confirmation",
            "awaiting_customer": "pending confirmation",
            "edit_requested": "pending confirmation",
            "confirmed": "confirmed",
            "human_requested": "needs review",
            "declined": "cancelled by customer",
        }.get(confirmation_status, confirmation_status)
        confirmed_edits = list(
            snapshot.get("confirmed_edits")
            or snapshot.get("latest_detected_edits")
            or []
        )
        summary = [
            f"[ZakBot WhatsApp confirmation] {stamp}",
            f"Outcome: {status_label}",
            f"Order reference: {order_row.get('external_order_id')}",
        ]
        if confirmed_edits:
            changes = ", ".join(
                f"{edit.get('field')}: {edit.get('value')}" for edit in confirmed_edits[:5]
            )
            summary.append(f"Confirmed edits: {changes}")
        block = "\n".join(summary)
        if current_note.strip():
            return f"{current_note.rstrip()}\n\n{block}"
        return block

    async def _fetch_order_note_and_tags(
        self,
        *,
        shop_domain: str,
        access_token: str,
        graphql_order_id: str,
    ) -> dict[str, Any]:
        response = await self._shopify_graphql(
            shop_domain=shop_domain,
            access_token=access_token,
            query="""
                query FetchOrder($id: ID!) {
                  order(id: $id) {
                    id
                    note
                    tags
                  }
                }
            """,
            variables={"id": graphql_order_id},
        )
        order = dict(response.get("order") or {})
        return {
            "note": order.get("note") or "",
            "tags": list(order.get("tags") or []),
        }

    async def _update_shopify_order(
        self,
        *,
        shop_domain: str,
        access_token: str,
        graphql_order_id: str,
        tags: list[str],
        note: str,
    ) -> None:
        response = await self._shopify_graphql(
            shop_domain=shop_domain,
            access_token=access_token,
            query="""
                mutation UpdateOrder($input: OrderInput!) {
                  orderUpdate(input: $input) {
                    order {
                      id
                      tags
                      note
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }
            """,
            variables={
                "input": {
                    "id": graphql_order_id,
                    "tags": tags,
                    "note": note,
                }
            },
        )
        user_errors = list((response.get("orderUpdate") or {}).get("userErrors") or [])
        if user_errors:
            message = "; ".join(str(row.get("message") or "Unknown Shopify error") for row in user_errors)
            raise RuntimeError(message)

    async def _shopify_graphql(
        self,
        *,
        shop_domain: str,
        access_token: str,
        query: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        response = await self.http_client.post(
            f"https://{shop_domain}/admin/api/{self.settings.shopify_api_version}/graphql.json",
            headers={
                "X-Shopify-Access-Token": access_token,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(json.dumps(payload["errors"]))
        return dict(payload.get("data") or {})

    async def _exchange_code_for_token(
        self, *, shop_domain: str, code: str
    ) -> dict[str, Any]:
        response = await self.http_client.post(
            f"https://{shop_domain}/admin/oauth/access_token",
            json={
                "client_id": self._require_shopify_api_key(),
                "client_secret": self._require_shopify_api_secret(),
                "code": code,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not payload.get("access_token"):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Shopify token exchange returned an invalid response.",
            )
        return payload

    async def _fetch_shop_info(
        self, *, shop_domain: str, access_token: str
    ) -> dict[str, Any]:
        response = await self.http_client.get(
            f"https://{shop_domain}/admin/api/{self.settings.shopify_api_version}/shop.json",
            headers={"X-Shopify-Access-Token": access_token},
        )
        response.raise_for_status()
        payload = response.json()
        return dict(payload.get("shop") or {})

    async def _register_webhooks(
        self,
        *,
        shop_domain: str,
        access_token: str,
    ) -> dict[str, int]:
        subscription_ids: dict[str, int] = {}
        for topic, path in WEBHOOK_TOPICS.items():
            response = await self.http_client.post(
                f"https://{shop_domain}/admin/api/{self.settings.shopify_api_version}/webhooks.json",
                headers={"X-Shopify-Access-Token": access_token},
                json={
                    "webhook": {
                        "topic": topic,
                        "address": f"{self._public_base_url().rstrip('/')}{path}",
                        "format": "json",
                    }
                },
            )
            response.raise_for_status()
            payload = response.json()
            webhook = dict(payload.get("webhook") or {})
            subscription_ids[topic] = int(webhook.get("id") or 0)
        return subscription_ids

    async def _touch_shopify_connection(
        self,
        *,
        connection: dict[str, Any],
        config: dict[str, Any],
        processed_webhook_id: str | None,
        received_order_delta: int,
        last_synced_at: Any,
    ) -> None:
        metrics = dict(connection.get("metrics") or {})
        metrics["received_orders"] = int(metrics.get("received_orders") or 0) + received_order_delta
        processed_ids = list(config.get("processed_webhook_ids") or [])
        if processed_webhook_id:
            processed_ids.append(processed_webhook_id)
        config["processed_webhook_ids"] = processed_ids[-100:]
        await self.integration_repository.upsert_connection(
            business_id=int(connection["business_id"]),
            integration_type="shopify",
            status_value=str(connection["status"]),
            health="healthy",
            config=config,
            metrics=metrics,
            last_activity_at=datetime.now(UTC),
            last_synced_at=last_synced_at,
        )

    def _build_callback_redirect(
        self,
        return_to: str | None,
        *,
        status_value: str,
        message: str,
        business_id: int | None = None,
        shop_domain: str | None = None,
    ) -> str:
        target = str(return_to or "").strip()
        if not target:
            base = self.settings.shopify_app_base_url or self.settings.public_webhook_base_url
            if base:
                target = base.rstrip("/")
        if not target:
            return f"about:blank?shopify_status={urllib.parse.quote(status_value)}"
        parsed = urllib.parse.urlsplit(target)
        existing = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        existing.extend(
            [
                ("shopify_status", status_value),
                ("shopify_message", message),
            ]
        )
        if business_id is not None:
            existing.append(("business_id", str(business_id)))
        if shop_domain:
            existing.append(("shop", shop_domain))
        return urllib.parse.urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urllib.parse.urlencode(existing),
                parsed.fragment,
            )
        )

    def _verify_oauth_hmac(self, query_params: Mapping[str, str]) -> None:
        received_hmac = str(query_params.get("hmac") or "").strip()
        if not received_hmac:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing Shopify OAuth hmac.",
            )
        encoded = []
        for key in sorted(query_params):
            if key in {"hmac", "signature"}:
                continue
            encoded.append(
                f"{key}={query_params[key]}"
            )
        message = "&".join(encoded)
        digest = hmac.new(
            self._require_shopify_api_secret().encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(digest, received_hmac):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid Shopify OAuth hmac.",
            )

    def _verify_webhook_hmac(self, *, headers: Mapping[str, str], body: bytes) -> None:
        received = str(headers.get("x-shopify-hmac-sha256") or "").strip()
        if not received:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing Shopify webhook signature.",
            )
        digest = base64.b64encode(
            hmac.new(
                self._require_shopify_api_secret().encode("utf-8"),
                body,
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        if not hmac.compare_digest(digest, received):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid Shopify webhook signature.",
            )

    def _public_base_url(self) -> str:
        base = self.settings.shopify_app_base_url or self.settings.public_webhook_base_url
        if not base:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="SHOPIFY_APP_BASE_URL or PUBLIC_WEBHOOK_BASE_URL must be configured.",
            )
        return base.rstrip("/")

    def _callback_url(self) -> str:
        return f"{self._public_base_url()}/integrations/shopify/callback"

    def _require_shopify_api_key(self) -> str:
        if not self.settings.shopify_api_key:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="SHOPIFY_API_KEY is not configured.",
            )
        return self.settings.shopify_api_key

    def _require_shopify_api_secret(self) -> str:
        if self.settings.shopify_api_secret is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="SHOPIFY_API_SECRET is not configured.",
            )
        return self.settings.shopify_api_secret.get_secret_value()

    def _normalize_shop_domain(self, shop_domain: str) -> str:
        normalized = shop_domain.strip().lower()
        if normalized.startswith("https://"):
            normalized = normalized.removeprefix("https://")
        if normalized.startswith("http://"):
            normalized = normalized.removeprefix("http://")
        normalized = normalized.strip("/ ")
        if not normalized.endswith(".myshopify.com"):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Shop domain must end with .myshopify.com.",
            )
        label = normalized.removesuffix(".myshopify.com")
        if not label or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789-" for char in label):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid Shopify shop domain.",
            )
        return normalized
