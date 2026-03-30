from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
import math
from typing import Any

from fastapi import HTTPException, status

from app.schemas.business import BusinessUpsertRequest
from app.schemas.faq import FAQUpsertRequest
from app.schemas.product import BulkProductUpsertRequest, ProductBulkItem, ProductUpsertRequest
from app.utils.phones import normalize_phone_number


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _copy_doc(document: dict[str, Any] | None) -> dict[str, Any] | None:
    if document is None:
        return None
    copied = dict(document)
    copied.pop("_id", None)
    return copied


def _contains_text(haystack: str | None, needle: str | None) -> bool:
    if not needle:
        return True
    return needle.strip().lower() in (haystack or "").lower()


def _cosine_similarity(left: list[float] | None, right: list[float] | None) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)


def _sorted_desc(rows: Iterable[dict[str, Any]], *, timestamp_field: str = "updated_at") -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (row.get(timestamp_field) or datetime.min.replace(tzinfo=UTC), row.get("id", 0)),
        reverse=True,
    )


async def _next_sequence(db: Any, sequence_name: str) -> int:
    from pymongo import ReturnDocument

    row = await db.counters.find_one_and_update(
        {"_id": sequence_name},
        {"$inc": {"value": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return int(row["value"])


class MongoBusinessRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def get_by_id(self, business_id: int) -> dict[str, Any]:
        row = _copy_doc(await self.db.business.find_one({"id": business_id}))
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Business {business_id} was not found.",
            )
        return row

    async def upsert(self, payload: BusinessUpsertRequest) -> dict[str, Any]:
        now = _utc_now()
        if payload.id is None:
            business_id = await _next_sequence(self.db, "business")
            row = {
                "_id": business_id,
                "id": business_id,
                "name": payload.name,
                "description": payload.description,
                "city": payload.city,
                "shipping_policy": payload.shipping_policy,
                "delivery_zones": payload.delivery_zones,
                "payment_methods": payload.payment_methods,
                "profile_metadata": payload.profile_metadata,
                "created_at": now,
                "updated_at": now,
            }
            await self.db.business.insert_one(row)
            return _copy_doc(row) or {}

        existing = await self.db.business.find_one({"id": payload.id})
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Business {payload.id} was not found.",
            )
        updated = {
            **existing,
            "name": payload.name,
            "description": payload.description,
            "city": payload.city,
            "shipping_policy": payload.shipping_policy,
            "delivery_zones": payload.delivery_zones,
            "payment_methods": payload.payment_methods,
            "profile_metadata": payload.profile_metadata,
            "updated_at": now,
        }
        await self.db.business.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def update_dashboard_profile(
        self, business_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        existing = await self.db.business.find_one({"id": business_id})
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Business {business_id} was not found.",
            )
        updated = {
            **existing,
            "name": payload["name"],
            "description": payload["description"],
            "city": payload["city"],
            "shipping_policy": payload["shipping_policy"],
            "delivery_zones": payload["delivery_zones"],
            "payment_methods": payload["payment_methods"],
            "profile_metadata": payload["profile_metadata"],
            "updated_at": _utc_now(),
        }
        await self.db.business.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def upsert_profile_knowledge(
        self,
        business_id: int,
        title: str,
        content: str,
        metadata: dict[str, Any],
        embedding: list[float],
    ) -> dict[str, Any]:
        existing = await self.db.business_knowledge.find_one(
            {"business_id": business_id, "source_type": "profile", "source_id": business_id}
        )
        now = _utc_now()
        if existing is None:
            knowledge_id = await _next_sequence(self.db, "business_knowledge")
            row = {
                "_id": knowledge_id,
                "id": knowledge_id,
                "business_id": business_id,
                "source_type": "profile",
                "source_id": business_id,
                "title": title,
                "content": content,
                "metadata": metadata,
                "embedding": embedding,
                "created_at": now,
                "updated_at": now,
            }
            await self.db.business_knowledge.insert_one(row)
            return _copy_doc(row) or {}

        updated = {
            **existing,
            "title": title,
            "content": content,
            "metadata": metadata,
            "embedding": embedding,
            "updated_at": now,
        }
        await self.db.business_knowledge.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def search_knowledge(
        self, business_id: int, embedding: list[float], limit: int
    ) -> list[dict[str, Any]]:
        rows = await self.db.business_knowledge.find(
            {"business_id": business_id, "embedding": {"$ne": None}}
        ).to_list(length=None)
        scored = []
        for row in rows:
            score = _cosine_similarity(row.get("embedding"), embedding)
            scored.append(
                {
                    **(_copy_doc(row) or {}),
                    "score": score,
                }
            )
        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:limit]


class MongoProductRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def get_by_id(self, business_id: int, product_id: int) -> dict[str, Any]:
        row = _copy_doc(await self.db.products.find_one({"id": product_id, "business_id": business_id}))
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} was not found for business {business_id}.",
            )
        return row

    async def get_by_product_id(self, product_id: int) -> dict[str, Any]:
        row = _copy_doc(await self.db.products.find_one({"id": product_id}))
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} was not found.",
            )
        return row

    async def list_by_business(self, business_id: int) -> list[dict[str, Any]]:
        rows = await self.db.products.find({"business_id": business_id}).to_list(length=None)
        return [_copy_doc(row) or {} for row in _sorted_desc(rows)]

    async def list_dashboard(
        self, business_id: int, search: str | None = None, category: str | None = None
    ) -> tuple[list[dict[str, Any]], int, list[str]]:
        rows = await self.list_by_business(business_id)
        filtered = [
            row
            for row in rows
            if _contains_text(row.get("name"), search)
            or _contains_text(row.get("description"), search)
        ] if search else rows
        if category:
            filtered = [row for row in filtered if (row.get("category") or "").strip() == category.strip()]
        categories = sorted(
            {
                str(row.get("category")).strip()
                for row in rows
                if str(row.get("category") or "").strip()
            }
        )
        return filtered, len(filtered), categories

    async def count_by_business(self, business_id: int) -> int:
        return await self.db.products.count_documents({"business_id": business_id})

    async def count_active_by_business(self, business_id: int) -> int:
        rows = await self.db.products.find({"business_id": business_id}).to_list(length=None)
        return sum(1 for row in rows if (row.get("availability") or "in_stock") != "out_of_stock")

    async def recent_by_business(self, business_id: int, limit: int) -> list[dict[str, Any]]:
        rows = await self.list_by_business(business_id)
        return rows[:limit]

    async def upsert(self, payload: ProductUpsertRequest) -> dict[str, Any]:
        item = ProductBulkItem.model_validate(payload.model_dump(exclude={"business_id"}))
        return await self._upsert_item(payload.business_id, item)

    async def bulk_upsert(self, payload: BulkProductUpsertRequest) -> list[dict[str, Any]]:
        return [await self._upsert_item(payload.business_id, product) for product in payload.products]

    async def create_dashboard_product(self, payload: dict[str, Any]) -> dict[str, Any]:
        product_id = await _next_sequence(self.db, "products")
        now = _utc_now()
        row = {
            "_id": product_id,
            "id": product_id,
            "business_id": payload["business_id"],
            "external_id": payload.get("external_id"),
            "name": payload["name"],
            "description": payload.get("description"),
            "price": payload.get("price"),
            "currency": payload.get("currency"),
            "category": payload.get("category"),
            "availability": payload.get("availability"),
            "variants": payload.get("variants", []),
            "tags": payload.get("tags", []),
            "metadata": payload.get("metadata", {}),
            "embedding": None,
            "created_at": now,
            "updated_at": now,
        }
        await self.db.products.insert_one(row)
        return _copy_doc(row) or {}

    async def update_dashboard_product(
        self, product_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        existing = await self.db.products.find_one({"id": product_id})
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} was not found.",
            )
        updated = {
            **existing,
            "external_id": payload.get("external_id"),
            "name": payload["name"],
            "description": payload.get("description"),
            "price": payload.get("price"),
            "currency": payload.get("currency"),
            "category": payload.get("category"),
            "availability": payload.get("availability"),
            "variants": payload.get("variants", []),
            "tags": payload.get("tags", []),
            "metadata": payload.get("metadata", {}),
            "updated_at": _utc_now(),
        }
        await self.db.products.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def delete(self, product_id: int) -> dict[str, Any]:
        existing = await self.db.products.find_one({"id": product_id})
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} was not found.",
            )
        await self.db.products.delete_one({"_id": existing["_id"]})
        return {"id": int(existing["id"]), "business_id": int(existing["business_id"])}

    async def _upsert_item(self, business_id: int, product: ProductBulkItem) -> dict[str, Any]:
        now = _utc_now()
        existing = None
        if product.id is not None:
            existing = await self.db.products.find_one({"id": product.id, "business_id": business_id})
            if existing is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Product {product.id} was not found for business {business_id}.",
                )
        elif product.external_id:
            existing = await self.db.products.find_one(
                {"business_id": business_id, "external_id": product.external_id}
            )

        if existing is None:
            product_id = await _next_sequence(self.db, "products")
            row = {
                "_id": product_id,
                "id": product_id,
                "business_id": business_id,
                "external_id": product.external_id,
                "name": product.name,
                "description": product.description,
                "price": product.price,
                "currency": product.currency,
                "category": product.category,
                "availability": product.availability,
                "variants": product.variants,
                "tags": product.tags,
                "metadata": product.metadata,
                "embedding": None,
                "created_at": now,
                "updated_at": now,
            }
            await self.db.products.insert_one(row)
            return _copy_doc(row) or {}

        updated = {
            **existing,
            "external_id": product.external_id,
            "name": product.name,
            "description": product.description,
            "price": product.price,
            "currency": product.currency,
            "category": product.category,
            "availability": product.availability,
            "variants": product.variants,
            "tags": product.tags,
            "metadata": product.metadata,
            "updated_at": now,
        }
        await self.db.products.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def update_embedding(self, product_id: int, embedding: list[float]) -> None:
        await self.db.products.update_one(
            {"id": product_id},
            {"$set": {"embedding": embedding, "updated_at": _utc_now()}},
        )

    async def search(self, business_id: int, embedding: list[float], limit: int) -> list[dict[str, Any]]:
        rows = await self.db.products.find({"business_id": business_id, "embedding": {"$ne": None}}).to_list(length=None)
        scored = []
        for row in rows:
            score = _cosine_similarity(row.get("embedding"), embedding)
            scored.append(
                {
                    **(_copy_doc(row) or {}),
                    "score": score,
                }
            )
        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:limit]


class MongoFAQRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def get_by_id(self, business_id: int, faq_id: int) -> dict[str, Any]:
        row = _copy_doc(await self.db.faqs.find_one({"id": faq_id, "business_id": business_id}))
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"FAQ {faq_id} was not found for business {business_id}.",
            )
        return row

    async def list_by_business(self, business_id: int) -> list[dict[str, Any]]:
        rows = await self.db.faqs.find({"business_id": business_id}).to_list(length=None)
        return [_copy_doc(row) or {} for row in _sorted_desc(rows)]

    async def upsert(self, payload: FAQUpsertRequest) -> dict[str, Any]:
        now = _utc_now()
        existing = None
        if payload.id is not None:
            existing = await self.db.faqs.find_one({"id": payload.id, "business_id": payload.business_id})
            if existing is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"FAQ {payload.id} was not found for business {payload.business_id}.",
                )
        elif payload.external_id:
            existing = await self.db.faqs.find_one(
                {"business_id": payload.business_id, "external_id": payload.external_id}
            )

        if existing is None:
            faq_id = await _next_sequence(self.db, "faqs")
            row = {
                "_id": faq_id,
                "id": faq_id,
                "business_id": payload.business_id,
                "external_id": payload.external_id,
                "question": payload.question,
                "answer": payload.answer,
                "metadata": payload.metadata,
                "embedding": None,
                "created_at": now,
                "updated_at": now,
            }
            await self.db.faqs.insert_one(row)
            return _copy_doc(row) or {}

        updated = {
            **existing,
            "external_id": payload.external_id,
            "question": payload.question,
            "answer": payload.answer,
            "metadata": payload.metadata,
            "updated_at": now,
        }
        await self.db.faqs.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def replace_for_business(
        self, business_id: int, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        keep_ids: list[int] = []
        now = _utc_now()
        for item in items:
            raw_id = item.get("id")
            faq_id = int(raw_id) if raw_id and str(raw_id).isdigit() else None
            if faq_id is not None:
                existing = await self.db.faqs.find_one({"id": faq_id, "business_id": business_id})
                if existing is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"FAQ {faq_id} was not found for business {business_id}.",
                    )
                updated = {
                    **existing,
                    "question": item["question"],
                    "answer": item["answer"],
                    "updated_at": now,
                }
                await self.db.faqs.replace_one({"_id": existing["_id"]}, updated)
                keep_ids.append(faq_id)
                continue

            new_id = await _next_sequence(self.db, "faqs")
            row = {
                "_id": new_id,
                "id": new_id,
                "business_id": business_id,
                "external_id": None,
                "question": item["question"],
                "answer": item["answer"],
                "metadata": {},
                "embedding": None,
                "created_at": now,
                "updated_at": now,
            }
            await self.db.faqs.insert_one(row)
            keep_ids.append(new_id)

        if keep_ids:
            await self.db.faqs.delete_many(
                {"business_id": business_id, "id": {"$nin": keep_ids}}
            )
        else:
            await self.db.faqs.delete_many({"business_id": business_id})

        return await self.list_by_business(business_id)

    async def update_embedding(self, faq_id: int, embedding: list[float]) -> None:
        await self.db.faqs.update_one(
            {"id": faq_id},
            {"$set": {"embedding": embedding, "updated_at": _utc_now()}},
        )

    async def search(self, business_id: int, embedding: list[float], limit: int) -> list[dict[str, Any]]:
        rows = await self.db.faqs.find({"business_id": business_id, "embedding": {"$ne": None}}).to_list(length=None)
        scored = []
        for row in rows:
            score = _cosine_similarity(row.get("embedding"), embedding)
            scored.append(
                {
                    **(_copy_doc(row) or {}),
                    "score": score,
                }
            )
        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:limit]


class MongoChatRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def get_message(self, message_id: int) -> dict[str, Any]:
        row = _copy_doc(await self.db.chat_messages.find_one({"id": message_id}))
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat message {message_id} was not found.",
            )
        return row

    async def list_messages(
        self,
        business_id: int,
        *,
        phone: str | None = None,
        intent: str | None = None,
        direction: str | None = None,
        needs_human: bool | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = await self.db.chat_messages.find({"business_id": business_id}).to_list(length=None)
        filtered: list[dict[str, Any]] = []
        for row in rows:
            if phone and phone.strip().lower() not in str(row.get("phone") or "").lower():
                continue
            if intent and row.get("intent") != intent:
                continue
            if direction and row.get("direction") != direction:
                continue
            if needs_human is not None and bool(row.get("needs_human")) != needs_human:
                continue
            filtered.append(_copy_doc(row) or {})
        filtered = _sorted_desc(filtered, timestamp_field="created_at")
        if limit is not None:
            filtered = filtered[:limit]
        return filtered

    async def get_thread(self, business_id: int, phone: str) -> list[dict[str, Any]]:
        normalized_phone = normalize_phone_number(phone)
        rows = await self.db.chat_messages.find(
            {"business_id": business_id, "phone": normalized_phone}
        ).to_list(length=None)
        rows = sorted(
            (_copy_doc(row) or {} for row in rows),
            key=lambda row: (row.get("created_at") or datetime.min.replace(tzinfo=UTC), row.get("id", 0)),
        )
        return rows

    async def count_messages(self, business_id: int) -> int:
        return await self.db.chat_messages.count_documents({"business_id": business_id})

    async def count_conversations(self, business_id: int) -> int:
        phones = await self.db.chat_messages.distinct("phone", {"business_id": business_id})
        return len([phone for phone in phones if phone])

    async def upsert_message(
        self,
        *,
        business_id: int,
        phone: str,
        customer_name: str | None,
        text: str,
        direction: str,
        intent: str | None,
        needs_human: bool,
        is_read: bool,
        provider: str | None,
        provider_message_sid: str | None,
        provider_status: str | None,
        error_code: str | None,
        raw_payload: dict[str, Any],
    ) -> dict[str, Any]:
        now = _utc_now()
        normalized_phone = normalize_phone_number(phone)
        existing = None
        if provider_message_sid:
            existing = await self.db.chat_messages.find_one({"provider_message_sid": provider_message_sid})

        if existing is None:
            message_id = await _next_sequence(self.db, "chat_messages")
            row = {
                "_id": message_id,
                "id": message_id,
                "business_id": business_id,
                "phone": normalized_phone,
                "customer_name": customer_name,
                "text": text,
                "direction": direction,
                "intent": intent,
                "needs_human": needs_human,
                "is_read": is_read,
                "provider": provider,
                "provider_message_sid": provider_message_sid,
                "provider_status": provider_status,
                "error_code": error_code,
                "raw_payload": raw_payload,
                "created_at": now,
                "updated_at": now,
            }
            await self.db.chat_messages.insert_one(row)
            return _copy_doc(row) or {}

        updated = {
            **existing,
            "customer_name": customer_name or existing.get("customer_name"),
            "text": text,
            "provider_status": provider_status or existing.get("provider_status"),
            "error_code": error_code,
            "raw_payload": raw_payload,
            "updated_at": now,
        }
        await self.db.chat_messages.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def update_provider_status(
        self,
        *,
        provider_message_sid: str,
        provider_status: str | None,
        error_code: str | None,
        raw_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        existing = await self.db.chat_messages.find_one({"provider_message_sid": provider_message_sid})
        if existing is None:
            return None
        updated = {
            **existing,
            "provider_status": provider_status,
            "error_code": error_code,
            "raw_payload": raw_payload,
            "updated_at": _utc_now(),
        }
        await self.db.chat_messages.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated)

    async def update_message_analysis(
        self,
        message_id: int,
        *,
        intent: str | None,
        needs_human: bool,
    ) -> dict[str, Any]:
        existing = await self.db.chat_messages.find_one({"id": message_id})
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat message {message_id} was not found.",
            )
        updated = {
            **existing,
            "intent": intent,
            "needs_human": needs_human,
            "updated_at": _utc_now(),
        }
        await self.db.chat_messages.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}


class MongoAIRunRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def create_run(
        self,
        *,
        business_id: int,
        phone: str | None,
        inbound_chat_message_id: int | None,
        outbound_chat_message_id: int | None,
        provider: str,
        model: str,
        status_value: str,
        customer_message: str,
        language: str | None,
        intent: str | None,
        needs_human: bool,
        confidence: float,
        reply_text: str | None,
        fallback_reason: str | None,
        retrieval_summary: dict[str, Any],
        prompt_version: str,
        request_payload: dict[str, Any],
        response_payload: dict[str, Any],
    ) -> dict[str, Any]:
        run_id = await _next_sequence(self.db, "ai_message_runs")
        now = _utc_now()
        row = {
            "_id": run_id,
            "id": run_id,
            "business_id": business_id,
            "phone": normalize_phone_number(phone) if phone else None,
            "inbound_chat_message_id": inbound_chat_message_id,
            "outbound_chat_message_id": outbound_chat_message_id,
            "provider": provider,
            "model": model,
            "status": status_value,
            "customer_message": customer_message,
            "language": language,
            "intent": intent,
            "needs_human": needs_human,
            "confidence": confidence,
            "reply_text": reply_text,
            "fallback_reason": fallback_reason,
            "retrieval_summary": retrieval_summary,
            "prompt_version": prompt_version,
            "request_payload": request_payload,
            "response_payload": response_payload,
            "created_at": now,
            "updated_at": now,
        }
        await self.db.ai_message_runs.insert_one(row)
        return _copy_doc(row) or {}

    async def update_run(
        self,
        run_id: int,
        *,
        status_value: str,
        outbound_chat_message_id: int | None = None,
        fallback_reason: str | None = None,
        response_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        existing = await self.db.ai_message_runs.find_one({"id": run_id})
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"AI run {run_id} was not found.",
            )
        updated = {
            **existing,
            "status": status_value,
            "updated_at": _utc_now(),
            "outbound_chat_message_id": outbound_chat_message_id
            if outbound_chat_message_id is not None
            else existing.get("outbound_chat_message_id"),
            "fallback_reason": fallback_reason
            if fallback_reason is not None
            else existing.get("fallback_reason"),
            "response_payload": response_payload
            if response_payload is not None
            else existing.get("response_payload") or {},
        }
        await self.db.ai_message_runs.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def list_runs(self, business_id: int, limit: int = 50) -> list[dict[str, Any]]:
        rows = await self.db.ai_message_runs.find({"business_id": business_id}).to_list(length=None)
        return [_copy_doc(row) or {} for row in _sorted_desc(rows, timestamp_field="created_at")[:limit]]

    async def get_run(self, business_id: int, run_id: int) -> dict[str, Any]:
        row = _copy_doc(await self.db.ai_message_runs.find_one({"business_id": business_id, "id": run_id}))
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"AI run {run_id} was not found for business {business_id}.",
            )
        return row


class MongoIntegrationRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def list_connections(self, business_id: int) -> list[dict[str, Any]]:
        rows = await self.db.integration_connections.find({"business_id": business_id}).to_list(length=None)
        return [_copy_doc(row) or {} for row in _sorted_desc(rows)]

    async def get_connection(
        self, business_id: int, integration_type: str
    ) -> dict[str, Any] | None:
        return _copy_doc(
            await self.db.integration_connections.find_one(
                {"business_id": business_id, "integration_type": integration_type}
            )
        )

    async def upsert_connection(
        self,
        *,
        business_id: int,
        integration_type: str,
        status_value: str,
        health: str,
        config: dict[str, Any],
        metrics: dict[str, Any],
        last_activity_at: Any = None,
        last_synced_at: Any = None,
    ) -> dict[str, Any]:
        existing = await self.db.integration_connections.find_one(
            {"business_id": business_id, "integration_type": integration_type}
        )
        now = _utc_now()
        if existing is None:
            connection_id = await _next_sequence(self.db, "integration_connections")
            row = {
                "_id": f"{business_id}:{integration_type}",
                "id": connection_id,
                "business_id": business_id,
                "integration_type": integration_type,
                "status": status_value,
                "health": health,
                "config": config,
                "metrics": metrics,
                "last_activity_at": last_activity_at,
                "last_synced_at": last_synced_at,
                "created_at": now,
                "updated_at": now,
            }
            await self.db.integration_connections.insert_one(row)
            return _copy_doc(row) or {}

        updated = {
            **existing,
            "status": status_value,
            "health": health,
            "config": config,
            "metrics": metrics,
            "last_activity_at": last_activity_at,
            "last_synced_at": last_synced_at,
            "updated_at": now,
        }
        await self.db.integration_connections.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def find_whatsapp_connection(
        self, *, sender_phone: str, subaccount_sid: str | None = None
    ) -> dict[str, Any] | None:
        normalized_sender = normalize_phone_number(sender_phone)
        rows = await self.db.integration_connections.find({"integration_type": "whatsapp"}).to_list(length=None)
        matches = []
        for row in rows:
            config = dict(row.get("config") or {})
            if (
                normalize_phone_number(config.get("whatsapp_number")) == normalized_sender
                or normalize_phone_number(config.get("phone_number")) == normalized_sender
                or (subaccount_sid and config.get("subaccount_sid") == subaccount_sid)
            ):
                matches.append(row)
        if not matches:
            return None
        matches = _sorted_desc(matches)
        return _copy_doc(matches[0])

    async def increment_whatsapp_metrics(
        self,
        business_id: int,
        *,
        received_delta: int = 0,
        sent_delta: int = 0,
        failed_delta: int = 0,
        touch_last_activity: bool = False,
    ) -> dict[str, Any] | None:
        connection = await self.get_connection(business_id, "whatsapp")
        if connection is None:
            return None
        metrics = {
            "received_messages_last_30_days": 0,
            "sent_messages_last_30_days": 0,
            "failed_messages_last_30_days": 0,
        }
        metrics.update(dict(connection.get("metrics") or {}))
        metrics["received_messages_last_30_days"] = int(metrics.get("received_messages_last_30_days") or 0) + received_delta
        metrics["sent_messages_last_30_days"] = int(metrics.get("sent_messages_last_30_days") or 0) + sent_delta
        metrics["failed_messages_last_30_days"] = int(metrics.get("failed_messages_last_30_days") or 0) + failed_delta

        return await self.upsert_connection(
            business_id=business_id,
            integration_type="whatsapp",
            status_value=connection["status"],
            health=connection["health"],
            config=dict(connection.get("config") or {}),
            metrics=metrics,
            last_activity_at=_utc_now() if touch_last_activity else connection.get("last_activity_at"),
            last_synced_at=connection.get("last_synced_at"),
        )


class MongoSyncStatusRepository:
    def __init__(self, session: Any) -> None:
        self.session = session
        self.db = session.db

    async def get_status(self, business_id: int) -> dict[str, Any] | None:
        return _copy_doc(await self.db.embedding_sync_status.find_one({"business_id": business_id}))

    async def mark_running(self, business_id: int, embedding_model: str) -> dict[str, Any]:
        return await self.upsert_status(
            business_id=business_id,
            status_value="running",
            last_synced_at=None,
            last_result="Embedding sync is running.",
            synced_products=0,
            synced_business_knowledge=0,
            synced_faqs=0,
            embedding_model=embedding_model,
        )

    async def mark_error(
        self, business_id: int, message: str, embedding_model: str
    ) -> dict[str, Any]:
        counts = await self.get_embedding_counts(business_id)
        return await self.upsert_status(
            business_id=business_id,
            status_value="error",
            last_synced_at=counts.get("last_embedded_at"),
            last_result=message,
            synced_products=int(counts.get("synced_products") or 0),
            synced_business_knowledge=int(counts.get("synced_business_knowledge") or 0),
            synced_faqs=int(counts.get("synced_faqs") or 0),
            embedding_model=embedding_model,
        )

    async def upsert_status(
        self,
        *,
        business_id: int,
        status_value: str,
        last_synced_at: Any,
        last_result: str | None,
        synced_products: int,
        synced_business_knowledge: int,
        synced_faqs: int,
        embedding_model: str,
    ) -> dict[str, Any]:
        existing = await self.db.embedding_sync_status.find_one({"business_id": business_id})
        now = _utc_now()
        if existing is None:
            row = {
                "_id": business_id,
                "business_id": business_id,
                "status": status_value,
                "last_synced_at": last_synced_at,
                "last_result": last_result,
                "synced_products": synced_products,
                "synced_business_knowledge": synced_business_knowledge,
                "synced_faqs": synced_faqs,
                "embedding_model": embedding_model,
                "created_at": now,
                "updated_at": now,
            }
            await self.db.embedding_sync_status.insert_one(row)
            return _copy_doc(row) or {}

        updated = {
            **existing,
            "status": status_value,
            "last_synced_at": last_synced_at,
            "last_result": last_result,
            "synced_products": synced_products,
            "synced_business_knowledge": synced_business_knowledge,
            "synced_faqs": synced_faqs,
            "embedding_model": embedding_model,
            "updated_at": now,
        }
        await self.db.embedding_sync_status.replace_one({"_id": existing["_id"]}, updated)
        return _copy_doc(updated) or {}

    async def get_embedding_counts(self, business_id: int) -> dict[str, Any]:
        product_rows = await self.db.products.find({"business_id": business_id, "embedding": {"$ne": None}}).to_list(length=None)
        faq_rows = await self.db.faqs.find({"business_id": business_id, "embedding": {"$ne": None}}).to_list(length=None)
        knowledge_rows = await self.db.business_knowledge.find({"business_id": business_id, "embedding": {"$ne": None}}).to_list(length=None)
        updated_values = [
            row.get("updated_at")
            for row in [*product_rows, *faq_rows, *knowledge_rows]
            if row.get("updated_at") is not None
        ]
        return {
            "synced_products": len(product_rows),
            "synced_business_knowledge": len(knowledge_rows),
            "synced_faqs": len(faq_rows),
            "last_embedded_at": max(updated_values) if updated_values else None,
        }
