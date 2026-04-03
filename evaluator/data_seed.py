from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from app.schemas.business import BusinessUpsertRequest
from app.schemas.faq import FAQUpsertRequest
from app.schemas.product import BulkProductUpsertRequest, ProductBulkItem
from app.services.database import get_session_factory
from app.services.embedding_service import EmbeddingService
from app.services.repository_factory import RepositoryFactory
from app.services.sync_service import SyncService
from evaluator.config import EvalSettings


@dataclass(slots=True)
class SeededBusiness:
    business_id: int
    name: str
    product_count: int
    faq_count: int
    delivery_zone_count: int


class EvalDataSeeder:
    def __init__(self, settings: EvalSettings) -> None:
        self.settings = settings

    async def seed(self, business_profile: dict[str, Any]) -> SeededBusiness:
        session_factory = get_session_factory()
        business_id: int | None = None
        product_items = self._build_product_items(business_profile)
        faq_items: list[FAQUpsertRequest] = []

        try:
            async with session_factory() as session:
                factory = RepositoryFactory(session)
                business_repository = factory.business()
                product_repository = factory.products()
                faq_repository = factory.faqs()

                business_row = await business_repository.upsert(
                    self._build_business_upsert_request(business_profile)
                )
                business_id = int(business_row["id"])

                await business_repository.update_dashboard_profile(
                    business_id,
                    self._build_dashboard_business_payload(business_profile),
                )

                await product_repository.bulk_upsert(
                    BulkProductUpsertRequest(
                        business_id=business_id,
                        products=product_items,
                    )
                )

                faq_items = self._build_faq_requests(business_id, business_profile)
                for faq in faq_items:
                    await faq_repository.upsert(faq)

                sync_service = SyncService(session=session, embedding_service=EmbeddingService())
                await sync_service.sync_business_embeddings(business_id)
                await session.commit()
        except Exception:
            if business_id is not None:
                await self.cleanup(business_id)
            raise

        return SeededBusiness(
            business_id=business_id or 0,
            name=str(business_profile["business"]["name"]),
            product_count=len(product_items),
            faq_count=len(faq_items),
            delivery_zone_count=len(business_profile["delivery"]["zones"]),
        )

    async def cleanup(self, business_id: int) -> None:
        session_factory = get_session_factory()
        async with session_factory() as session:
            if self.settings.database_backend == "mongo":
                await session.db.business.delete_one({"id": business_id})
                await session.db.products.delete_many({"business_id": business_id})
                await session.db.faqs.delete_many({"business_id": business_id})
                await session.db.business_knowledge.delete_many({"business_id": business_id})
                await session.db.chat_messages.delete_many({"business_id": business_id})
                await session.db.integration_connections.delete_many({"business_id": business_id})
                await session.db.embedding_sync_status.delete_many({"business_id": business_id})
                await session.db.ai_message_runs.delete_many({"business_id": business_id})
                await session.commit()
                return

            await session.execute(
                text("DELETE FROM business WHERE id = :business_id"),
                {"business_id": business_id},
            )
            await session.commit()

    def _build_business_upsert_request(
        self, business_profile: dict[str, Any]
    ) -> BusinessUpsertRequest:
        business = business_profile["business"]
        delivery_zones = [zone["city"] for zone in business_profile["delivery"]["zones"]]
        payment_methods = business.get("payment_methods") or []
        return BusinessUpsertRequest(
            name=business["name"],
            description=business["description"],
            city=business["location"]["city"],
            shipping_policy=self._build_shipping_policy(business_profile),
            delivery_zones=delivery_zones,
            payment_methods=payment_methods,
            profile_metadata={
                "evaluator_seed": True,
                "seed_source": "run_eval.py",
            },
        )

    def _build_dashboard_business_payload(
        self, business_profile: dict[str, Any]
    ) -> dict[str, Any]:
        business = business_profile["business"]
        delivery = business_profile["delivery"]
        return_policy = business_profile["return_policy"]
        delivery_zones = [zone["city"] for zone in delivery["zones"]]
        return {
            "name": business["name"],
            "description": business["description"],
            "city": business["location"]["city"],
            "shipping_policy": self._build_shipping_policy(business_profile),
            "delivery_zones": delivery_zones,
            "payment_methods": business.get("payment_methods") or [],
            "profile_metadata": {
                "summary": business["description"],
                "niche": business.get("category", ""),
                "supported_languages": business.get("supported_languages", []),
                "tone_of_voice": "professional",
                "opening_hours": self._build_opening_hours(business["working_hours"]),
                "store_address": (
                    f"{business['location']['address']}, "
                    f"{business['location']['city']}, "
                    f"{business['location']['country']}"
                ),
                "support_phone": business["contact"]["phone"],
                "whatsapp_number": business["contact"]["whatsapp"],
                "support_email": business["contact"]["email"],
                "delivery_time": self._build_delivery_time_summary(delivery["zones"]),
                "delivery_tracking_method": delivery["tracking_method"],
                "delivery_zone_details": delivery["zones"],
                "return_policy": self._build_return_policy_summary(return_policy),
                "return_window_days": return_policy["window_days"],
                "return_conditions": return_policy["conditions"],
                "order_rules": [],
                "escalation_contact": (
                    f"WhatsApp: {business['contact']['whatsapp']}, "
                    f"Phone: {business['contact']['phone']}, "
                    f"Email: {business['contact']['email']}"
                ),
                "upsell_rules": [],
                "tracking_available": delivery["tracking_available"],
                "evaluator_seed": True,
                "seed_source": "run_eval.py",
            },
        }

    def _build_product_items(self, business_profile: dict[str, Any]) -> list[ProductBulkItem]:
        items: list[ProductBulkItem] = []
        for product in business_profile["products"]:
            items.append(
                ProductBulkItem(
                    external_id=product["sku"],
                    name=product["name"],
                    description=product["description"],
                    price=product["price_mad"],
                    currency="MAD",
                    category=product["category"],
                    availability=product["availability"],
                    metadata={"sku": product["sku"]},
                )
            )
        return items

    def _build_faq_requests(
        self, business_id: int, business_profile: dict[str, Any]
    ) -> list[FAQUpsertRequest]:
        delivery = business_profile["delivery"]
        return_policy = business_profile["return_policy"]
        business = business_profile["business"]

        faqs: list[FAQUpsertRequest] = []
        for zone in delivery["zones"]:
            faqs.append(
                FAQUpsertRequest(
                    business_id=business_id,
                    external_id=f"delivery-{zone['city'].lower()}",
                    question=f"Do you deliver to {zone['city']}?",
                    answer=(
                        f"Yes, we deliver to {zone['city']} for {zone['fee_mad']} MAD "
                        f"with an estimated time of {zone['estimated_time']}."
                    ),
                    metadata={"topic": "delivery", "city": zone["city"]},
                )
            )

        faqs.extend(
            [
                FAQUpsertRequest(
                    business_id=business_id,
                    external_id="delivery-tracking",
                    question="How does delivery tracking work?",
                    answer=delivery["tracking_method"],
                    metadata={"topic": "delivery"},
                ),
                FAQUpsertRequest(
                    business_id=business_id,
                    external_id="return-policy",
                    question="What is your return policy?",
                    answer=self._build_return_policy_summary(return_policy),
                    metadata={"topic": "returns"},
                ),
                FAQUpsertRequest(
                    business_id=business_id,
                    external_id="store-contact",
                    question="What is your store address and support contact?",
                    answer=(
                        f"Our store is at {business['location']['address']}, "
                        f"{business['location']['city']}. "
                        f"Phone: {business['contact']['phone']}. "
                        f"WhatsApp: {business['contact']['whatsapp']}."
                    ),
                    metadata={"topic": "profile"},
                ),
            ]
        )
        return faqs

    @staticmethod
    def _build_shipping_policy(business_profile: dict[str, Any]) -> str:
        delivery = business_profile["delivery"]
        zones_summary = "; ".join(
            f"{zone['city']}: {zone['fee_mad']} MAD, {zone['estimated_time']}"
            for zone in delivery["zones"]
        )
        return (
            "Delivery is available in supported Moroccan cities. "
            f"Zones and estimates: {zones_summary}. "
            f"Tracking: {delivery['tracking_method']}"
        )

    @staticmethod
    def _build_delivery_time_summary(zones: list[dict[str, Any]]) -> str:
        return "; ".join(
            f"{zone['city']}: {zone['estimated_time']}" for zone in zones
        )

    @staticmethod
    def _build_opening_hours(working_hours: dict[str, str]) -> list[str]:
        return [
            f"Monday to Friday: {working_hours['monday_to_friday']}",
            f"Saturday: {working_hours['saturday']}",
            f"Sunday: {working_hours['sunday']}",
        ]

    @staticmethod
    def _build_return_policy_summary(return_policy: dict[str, Any]) -> str:
        conditions = " ".join(str(item) for item in return_policy["conditions"])
        return (
            f"Returns are accepted within {return_policy['window_days']} days. "
            f"{conditions}"
        )
