from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils.phones import normalize_phone_number


def _json_dumps(value: Any) -> str:
    return json.dumps(value)


class OrderRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_order(self, *, business_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                INSERT INTO orders (
                    business_id, source_store, external_order_id, customer_name, customer_phone,
                    preferred_language, total_amount, currency, payment_method, delivery_city,
                    delivery_address, order_notes, items, metadata, raw_payload, status, confirmation_status
                )
                VALUES (
                    :business_id, :source_store, :external_order_id, :customer_name, :customer_phone,
                    :preferred_language, :total_amount, :currency, :payment_method, :delivery_city,
                    :delivery_address, :order_notes, CAST(:items AS jsonb), CAST(:metadata AS jsonb),
                    CAST(:raw_payload AS jsonb), :status, :confirmation_status
                )
                ON CONFLICT (business_id, source_store, external_order_id)
                DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    customer_phone = EXCLUDED.customer_phone,
                    preferred_language = EXCLUDED.preferred_language,
                    total_amount = EXCLUDED.total_amount,
                    currency = EXCLUDED.currency,
                    payment_method = EXCLUDED.payment_method,
                    delivery_city = EXCLUDED.delivery_city,
                    delivery_address = EXCLUDED.delivery_address,
                    order_notes = EXCLUDED.order_notes,
                    items = EXCLUDED.items,
                    metadata = EXCLUDED.metadata,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = timezone('utc', now())
                RETURNING id, business_id, source_store, external_order_id, customer_name,
                          customer_phone, preferred_language, total_amount, currency,
                          payment_method, delivery_city, delivery_address, order_notes,
                          items, metadata, raw_payload, status, confirmation_status,
                          created_at, updated_at
                """
            ),
            {
                "business_id": business_id,
                "source_store": payload["source_store"],
                "external_order_id": payload["external_order_id"],
                "customer_name": payload.get("customer_name"),
                "customer_phone": normalize_phone_number(payload["customer_phone"]),
                "preferred_language": payload.get("preferred_language"),
                "total_amount": payload["total_amount"],
                "currency": payload.get("currency") or "MAD",
                "payment_method": payload.get("payment_method"),
                "delivery_city": payload.get("delivery_city"),
                "delivery_address": payload.get("delivery_address"),
                "order_notes": payload.get("order_notes"),
                "items": _json_dumps(payload.get("items") or []),
                "metadata": _json_dumps(payload.get("metadata") or {}),
                "raw_payload": _json_dumps(payload.get("raw_payload") or {}),
                "status": payload.get("status") or "pending_confirmation",
                "confirmation_status": payload.get("confirmation_status") or "pending_send",
            },
        )
        return dict(result.mappings().one())

    async def get_by_id(self, business_id: int, order_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, source_store, external_order_id, customer_name,
                       customer_phone, preferred_language, total_amount, currency,
                       payment_method, delivery_city, delivery_address, order_notes,
                       items, metadata, raw_payload, status, confirmation_status,
                       created_at, updated_at
                FROM orders
                WHERE business_id = :business_id AND id = :order_id
                """
            ),
            {"business_id": business_id, "order_id": order_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} was not found for business {business_id}.",
            )
        return dict(row)

    async def update_order_status(
        self,
        *,
        business_id: int,
        order_id: int,
        status_value: str,
        confirmation_status: str,
        metadata: dict[str, Any] | None = None,
        finalized_order: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        finalized_order = finalized_order or {}
        result = await self.session.execute(
            text(
                """
                UPDATE orders
                SET status = :status_value,
                    confirmation_status = :confirmation_status,
                    customer_phone = COALESCE(:customer_phone, customer_phone),
                    preferred_language = COALESCE(:preferred_language, preferred_language),
                    total_amount = COALESCE(:total_amount, total_amount),
                    currency = COALESCE(:currency, currency),
                    payment_method = COALESCE(:payment_method, payment_method),
                    delivery_city = COALESCE(:delivery_city, delivery_city),
                    delivery_address = COALESCE(:delivery_address, delivery_address),
                    order_notes = COALESCE(:order_notes, order_notes),
                    items = COALESCE(CAST(:items AS jsonb), items),
                    metadata = CAST(:metadata AS jsonb),
                    updated_at = timezone('utc', now())
                WHERE business_id = :business_id
                  AND id = :order_id
                RETURNING id, business_id, source_store, external_order_id, customer_name,
                          customer_phone, preferred_language, total_amount, currency,
                          payment_method, delivery_city, delivery_address, order_notes,
                          items, metadata, raw_payload, status, confirmation_status,
                          created_at, updated_at
                """
            ),
            {
                "business_id": business_id,
                "order_id": order_id,
                "status_value": status_value,
                "confirmation_status": confirmation_status,
                "customer_phone": normalize_phone_number(
                    finalized_order.get("customer_phone", "")
                )
                if finalized_order.get("customer_phone")
                else None,
                "preferred_language": finalized_order.get("preferred_language"),
                "total_amount": finalized_order.get("total_amount"),
                "currency": finalized_order.get("currency"),
                "payment_method": finalized_order.get("payment_method"),
                "delivery_city": finalized_order.get("delivery_city"),
                "delivery_address": finalized_order.get("delivery_address"),
                "order_notes": finalized_order.get("order_notes"),
                "items": _json_dumps(finalized_order["items"])
                if "items" in finalized_order
                else None,
                "metadata": _json_dumps(metadata or {}),
            },
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} was not found for business {business_id}.",
            )
        return dict(row)


class OrderConfirmationRepository:
    _SESSION_COLUMNS = """
        id, business_id, order_id, phone, customer_name, preferred_language, status,
        needs_human, last_detected_intent, started_at, last_customer_message_at,
        confirmed_at, declined_at, expires_at, last_outbound_message_sid,
        structured_snapshot, created_at, updated_at
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def find_latest_by_order(self, business_id: int, order_id: int) -> dict[str, Any] | None:
        result = await self.session.execute(
            text(
                f"""
                SELECT {self._SESSION_COLUMNS}
                FROM order_confirmation_sessions
                WHERE business_id = :business_id
                  AND order_id = :order_id
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """
            ),
            {"business_id": business_id, "order_id": order_id},
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def create_session(
        self,
        *,
        business_id: int,
        order_id: int,
        phone: str,
        customer_name: str | None,
        preferred_language: str | None,
        status_value: str,
        needs_human: bool,
        last_detected_intent: str | None,
        structured_snapshot: dict[str, Any],
        last_outbound_message_sid: str | None = None,
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                f"""
                INSERT INTO order_confirmation_sessions (
                    business_id, order_id, phone, customer_name, preferred_language, status,
                    needs_human, last_detected_intent, started_at, last_outbound_message_sid,
                    structured_snapshot
                )
                VALUES (
                    :business_id, :order_id, :phone, :customer_name, :preferred_language, :status_value,
                    :needs_human, :last_detected_intent, timezone('utc', now()),
                    :last_outbound_message_sid, CAST(:structured_snapshot AS jsonb)
                )
                RETURNING {self._SESSION_COLUMNS}
                """
            ),
            {
                "business_id": business_id,
                "order_id": order_id,
                "phone": normalize_phone_number(phone),
                "customer_name": customer_name,
                "preferred_language": preferred_language,
                "status_value": status_value,
                "needs_human": needs_human,
                "last_detected_intent": last_detected_intent,
                "last_outbound_message_sid": last_outbound_message_sid,
                "structured_snapshot": _json_dumps(structured_snapshot),
            },
        )
        return dict(result.mappings().one())

    async def get_session(self, business_id: int, session_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                f"""
                SELECT {self._SESSION_COLUMNS}
                FROM order_confirmation_sessions
                WHERE business_id = :business_id
                  AND id = :session_id
                """
            ),
            {"business_id": business_id, "session_id": session_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order confirmation session {session_id} was not found.",
            )
        return dict(row)

    async def list_sessions(
        self,
        business_id: int,
        *,
        status_value: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"business_id": business_id, "limit": limit}
        clause = ""
        if status_value:
            clause = "AND status = :status_value"
            params["status_value"] = status_value
        result = await self.session.execute(
            text(
                f"""
                SELECT {self._SESSION_COLUMNS}
                FROM order_confirmation_sessions
                WHERE business_id = :business_id
                  {clause}
                ORDER BY updated_at DESC, id DESC
                LIMIT :limit
                """
            ),
            params,
        )
        return [dict(row) for row in result.mappings().all()]

    async def find_active_session(self, business_id: int, phone: str) -> dict[str, Any] | None:
        result = await self.session.execute(
            text(
                f"""
                SELECT {self._SESSION_COLUMNS}
                FROM order_confirmation_sessions
                WHERE business_id = :business_id
                  AND phone = :phone
                  AND status IN ('pending_send', 'awaiting_customer', 'edit_requested', 'human_requested')
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """
            ),
            {
                "business_id": business_id,
                "phone": normalize_phone_number(phone),
            },
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def update_session(self, session_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        if not payload:
            raise ValueError("update_session requires at least one field.")

        json_fields = {"structured_snapshot"}
        set_clauses: list[str] = []
        params: dict[str, Any] = {"session_id": session_id}
        for key, value in payload.items():
            if key in json_fields:
                set_clauses.append(f"{key} = CAST(:{key} AS jsonb)")
                params[key] = _json_dumps(value or {})
            else:
                set_clauses.append(f"{key} = :{key}")
                params[key] = value
        set_clauses.append("updated_at = timezone('utc', now())")

        result = await self.session.execute(
            text(
                f"""
                UPDATE order_confirmation_sessions
                SET {', '.join(set_clauses)}
                WHERE id = :session_id
                RETURNING {self._SESSION_COLUMNS}
                """
            ),
            params,
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order confirmation session {session_id} was not found.",
            )
        return dict(row)

    async def add_event(
        self,
        *,
        business_id: int,
        session_id: int,
        order_id: int,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                INSERT INTO order_confirmation_events (
                    business_id, session_id, order_id, event_type, payload
                )
                VALUES (
                    :business_id, :session_id, :order_id, :event_type, CAST(:payload AS jsonb)
                )
                RETURNING id, business_id, session_id, order_id, event_type, payload, created_at
                """
            ),
            {
                "business_id": business_id,
                "session_id": session_id,
                "order_id": order_id,
                "event_type": event_type,
                "payload": _json_dumps(payload),
            },
        )
        return dict(result.mappings().one())

    async def list_events(self, session_id: int, limit: int = 50) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, session_id, order_id, event_type, payload, created_at
                FROM order_confirmation_events
                WHERE session_id = :session_id
                ORDER BY created_at ASC, id ASC
                LIMIT :limit
                """
            ),
            {"session_id": session_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]
