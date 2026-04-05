from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status

from app.utils.phones import normalize_phone_number


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _copy_doc(document: dict[str, Any] | None) -> dict[str, Any] | None:
    if document is None:
        return None
    copied = dict(document)
    copied.pop("_id", None)
    return copied


def _order_document_id(*, business_id: int, source_store: str, external_order_id: str) -> str:
    return f"order:{business_id}:{source_store}:{external_order_id}"


def _session_document_id(*, business_id: int, order_id: int) -> str:
    return f"order-confirmation-session:{business_id}:{order_id}"


async def _next_sequence(db: Any, sequence_name: str) -> int:
    from pymongo import ReturnDocument

    row = await db.counters.find_one_and_update(
        {"_id": sequence_name},
        {"$inc": {"value": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return int(row["value"])


class MongoOrderRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def upsert_order(self, *, business_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        from pymongo import ReturnDocument

        now = _utc_now()
        normalized_phone = normalize_phone_number(payload["customer_phone"])
        document_id = _order_document_id(
            business_id=business_id,
            source_store=payload["source_store"],
            external_order_id=payload["external_order_id"],
        )
        existing = await self.db.orders.find_one(
            {
                "business_id": business_id,
                "source_store": payload["source_store"],
                "external_order_id": payload["external_order_id"],
            }
        )
        if existing is not None:
            updated = {
                **existing,
                "customer_name": payload.get("customer_name"),
                "customer_phone": normalized_phone,
                "preferred_language": payload.get("preferred_language"),
                "total_amount": payload["total_amount"],
                "currency": payload.get("currency") or "MAD",
                "payment_method": payload.get("payment_method"),
                "delivery_city": payload.get("delivery_city"),
                "delivery_address": payload.get("delivery_address"),
                "order_notes": payload.get("order_notes"),
                "items": payload.get("items") or [],
                "metadata": payload.get("metadata") or {},
                "raw_payload": payload.get("raw_payload") or {},
                "updated_at": now,
            }
            await self.db.orders.replace_one({"_id": existing["_id"]}, updated)
            return _copy_doc(updated) or {}

        order_id = await _next_sequence(self.db, "orders")
        row = await self.db.orders.find_one_and_update(
            {"_id": document_id},
            {
                "$setOnInsert": {
                    "_id": document_id,
                    "id": order_id,
                    "business_id": business_id,
                    "source_store": payload["source_store"],
                    "external_order_id": payload["external_order_id"],
                    "created_at": now,
                },
                "$set": {
                    "customer_name": payload.get("customer_name"),
                    "customer_phone": normalized_phone,
                    "preferred_language": payload.get("preferred_language"),
                    "total_amount": payload["total_amount"],
                    "currency": payload.get("currency") or "MAD",
                    "payment_method": payload.get("payment_method"),
                    "delivery_city": payload.get("delivery_city"),
                    "delivery_address": payload.get("delivery_address"),
                    "order_notes": payload.get("order_notes"),
                    "items": payload.get("items") or [],
                    "metadata": payload.get("metadata") or {},
                    "raw_payload": payload.get("raw_payload") or {},
                    "status": payload.get("status") or "pending_confirmation",
                    "confirmation_status": payload.get("confirmation_status") or "pending_send",
                    "updated_at": now,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return _copy_doc(row) or {}

    async def get_by_id(self, business_id: int, order_id: int) -> dict[str, Any]:
        row = _copy_doc(await self.db.orders.find_one({"business_id": business_id, "id": order_id}))
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} was not found for business {business_id}.",
            )
        return row

    async def get_by_external_reference(
        self, *, business_id: int, source_store: str, external_order_id: str
    ) -> dict[str, Any] | None:
        return _copy_doc(
            await self.db.orders.find_one(
                {
                    "business_id": business_id,
                    "source_store": source_store,
                    "external_order_id": external_order_id,
                }
            )
        )

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
        existing = await self.db.orders.find_one({"business_id": business_id, "id": order_id})
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} was not found for business {business_id}.",
            )
        finalized_order = finalized_order or {}
        updated = {
            **existing,
            "status": status_value,
            "confirmation_status": confirmation_status,
            "metadata": metadata or {},
            "updated_at": _utc_now(),
        }
        if "customer_phone" in finalized_order and finalized_order.get("customer_phone"):
            updated["customer_phone"] = normalize_phone_number(finalized_order["customer_phone"])
        for field in (
            "preferred_language",
            "total_amount",
            "currency",
            "payment_method",
            "delivery_city",
            "delivery_address",
            "order_notes",
            "items",
        ):
            if field in finalized_order and finalized_order[field] is not None:
                updated[field] = finalized_order[field]
        await self.db.orders.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}


class MongoOrderConfirmationRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def find_latest_by_order(self, business_id: int, order_id: int) -> dict[str, Any] | None:
        rows = await self.db.order_confirmation_sessions.find(
            {"business_id": business_id, "order_id": order_id}
        ).to_list(length=None)
        if not rows:
            return None
        rows.sort(key=lambda row: (row.get("updated_at"), row.get("id", 0)), reverse=True)
        return _copy_doc(rows[0])

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
        from pymongo import ReturnDocument

        now = _utc_now()
        existing = await self.db.order_confirmation_sessions.find_one(
            {"business_id": business_id, "order_id": order_id}
        )
        if existing is not None:
            return _copy_doc(existing) or {}

        session_id = await _next_sequence(self.db, "order_confirmation_sessions")
        row = await self.db.order_confirmation_sessions.find_one_and_update(
            {"_id": _session_document_id(business_id=business_id, order_id=order_id)},
            {
                "$setOnInsert": {
                    "_id": _session_document_id(business_id=business_id, order_id=order_id),
                    "id": session_id,
                    "business_id": business_id,
                    "order_id": order_id,
                    "phone": normalize_phone_number(phone),
                    "customer_name": customer_name,
                    "preferred_language": preferred_language,
                    "status": status_value,
                    "needs_human": needs_human,
                    "last_detected_intent": last_detected_intent,
                    "started_at": now,
                    "last_customer_message_at": None,
                    "confirmed_at": None,
                    "declined_at": None,
                    "expires_at": None,
                    "last_outbound_message_sid": last_outbound_message_sid,
                    "structured_snapshot": structured_snapshot,
                    "created_at": now,
                    "updated_at": now,
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return _copy_doc(row) or {}

    async def get_session(self, business_id: int, session_id: int) -> dict[str, Any]:
        row = _copy_doc(
            await self.db.order_confirmation_sessions.find_one(
                {"business_id": business_id, "id": session_id}
            )
        )
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order confirmation session {session_id} was not found.",
            )
        return row

    async def list_sessions(
        self,
        business_id: int,
        *,
        status_value: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"business_id": business_id}
        if status_value:
            query["status"] = status_value
        rows = await self.db.order_confirmation_sessions.find(query).to_list(length=None)
        rows.sort(key=lambda row: (row.get("updated_at"), row.get("id", 0)), reverse=True)
        return [(_copy_doc(row) or {}) for row in rows[:limit]]

    async def find_active_session(self, business_id: int, phone: str) -> dict[str, Any] | None:
        rows = await self.db.order_confirmation_sessions.find(
            {
                "business_id": business_id,
                "phone": normalize_phone_number(phone),
                "status": {"$in": ["pending_send", "awaiting_customer", "edit_requested", "human_requested"]},
            }
        ).to_list(length=None)
        if not rows:
            return None
        rows.sort(key=lambda row: (row.get("updated_at"), row.get("id", 0)), reverse=True)
        return _copy_doc(rows[0])

    async def update_session(self, session_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        existing = await self.db.order_confirmation_sessions.find_one({"id": session_id})
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order confirmation session {session_id} was not found.",
            )
        updated = {
            **existing,
            **payload,
            "updated_at": _utc_now(),
        }
        await self.db.order_confirmation_sessions.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def add_event(
        self,
        *,
        business_id: int,
        session_id: int,
        order_id: int,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        event_id = await _next_sequence(self.db, "order_confirmation_events")
        row = {
            "_id": event_id,
            "id": event_id,
            "business_id": business_id,
            "session_id": session_id,
            "order_id": order_id,
            "event_type": event_type,
            "payload": payload,
            "created_at": _utc_now(),
        }
        await self.db.order_confirmation_events.insert_one(row)
        return _copy_doc(row) or {}

    async def list_events(self, session_id: int, limit: int = 50) -> list[dict[str, Any]]:
        rows = await self.db.order_confirmation_events.find({"session_id": session_id}).to_list(length=None)
        rows.sort(key=lambda row: (row.get("created_at"), row.get("id", 0)))
        return [(_copy_doc(row) or {}) for row in rows[:limit]]
