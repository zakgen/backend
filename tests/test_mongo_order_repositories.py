from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pymongo import ReturnDocument

from app.services.mongo_order_repositories import (
    MongoOrderConfirmationRepository,
    MongoOrderRepository,
)


class FakeCollection:
    def __init__(self) -> None:
        self.documents: list[dict] = []

    async def find_one(self, query: dict):
        for document in self.documents:
            if all(document.get(key) == value for key, value in query.items()):
                return dict(document)
        return None

    async def insert_one(self, document: dict):
        self.documents.append(dict(document))
        return type("InsertOneResult", (), {"inserted_id": document.get("_id")})()

    async def replace_one(self, query: dict, document: dict):
        for index, existing in enumerate(self.documents):
            if all(existing.get(key) == value for key, value in query.items()):
                self.documents[index] = dict(document)
                return type("ReplaceOneResult", (), {"matched_count": 1})()
        return type("ReplaceOneResult", (), {"matched_count": 0})()

    async def find_one_and_update(
        self,
        query: dict,
        update: dict,
        *,
        upsert: bool = False,
        return_document=None,
    ):
        for index, existing in enumerate(self.documents):
            if all(existing.get(key) == value for key, value in query.items()):
                updated = dict(existing)
                if "$inc" in update:
                    for key, value in update["$inc"].items():
                        updated[key] = int(updated.get(key, 0)) + int(value)
                if "$set" in update:
                    updated.update(update["$set"])
                if "$setOnInsert" in update:
                    pass
                self.documents[index] = updated
                return dict(updated if return_document == ReturnDocument.AFTER else existing)

        if not upsert:
            return None

        inserted = dict(query)
        if "$setOnInsert" in update:
            inserted.update(update["$setOnInsert"])
        if "$set" in update:
            inserted.update(update["$set"])
        if "$inc" in update:
            for key, value in update["$inc"].items():
                inserted[key] = int(inserted.get(key, 0)) + int(value)
        self.documents.append(inserted)
        return dict(inserted)


class FakeDB:
    def __init__(self) -> None:
        self.counters = FakeCollection()
        self.orders = FakeCollection()
        self.order_confirmation_sessions = FakeCollection()


class FakeSession:
    def __init__(self) -> None:
        self.db = FakeDB()


@pytest.mark.asyncio
async def test_mongo_order_upsert_reuses_same_external_reference() -> None:
    repository = MongoOrderRepository(FakeSession())
    payload = {
        "source_store": "shopify",
        "external_order_id": "6624177684716",
        "customer_name": "Shopify customer",
        "customer_phone": "+212773823618",
        "preferred_language": "english",
        "total_amount": 1025.0,
        "currency": "USD",
        "payment_method": "manual",
        "delivery_city": None,
        "delivery_address": None,
        "order_notes": None,
        "items": [{"product_name": "Board", "quantity": 1}],
        "metadata": {"shopify_order_gid": "gid://shopify/Order/6624177684716"},
        "raw_payload": {"id": "6624177684716"},
        "status": "pending_confirmation",
        "confirmation_status": "pending_send",
    }

    first = await repository.upsert_order(business_id=1, payload=payload)
    second = await repository.upsert_order(
        business_id=1,
        payload={**payload, "total_amount": 1100.0},
    )

    assert first["id"] == second["id"]
    assert second["total_amount"] == 1100.0
    assert len(repository.db.orders.documents) == 1


@pytest.mark.asyncio
async def test_mongo_order_confirmation_create_session_is_idempotent_per_order() -> None:
    repository = MongoOrderConfirmationRepository(FakeSession())
    snapshot = {
        "external_order_id": "6624177684716",
        "customer_name": "Shopify customer",
        "customer_phone": "+212773823618",
        "items": [{"product_name": "Board", "quantity": 1}],
    }

    first = await repository.create_session(
        business_id=1,
        order_id=11,
        phone="+212773823618",
        customer_name="Shopify customer",
        preferred_language="english",
        status_value="pending_send",
        needs_human=False,
        last_detected_intent="order_confirmation_pending",
        structured_snapshot=snapshot,
    )
    second = await repository.create_session(
        business_id=1,
        order_id=11,
        phone="+212773823618",
        customer_name="Shopify customer",
        preferred_language="english",
        status_value="pending_send",
        needs_human=False,
        last_detected_intent="order_confirmation_pending",
        structured_snapshot=snapshot,
    )

    assert first["id"] == second["id"]
    assert len(repository.db.order_confirmation_sessions.documents) == 1
