from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.services.embedding_service import EmbeddingService
from app.services.repositories import (
    BusinessRepository,
    FAQRepository,
    ProductRepository,
    SyncStatusRepository,
)
from app.services.text_builder import (
    build_business_profile_text,
    build_faq_embedding_text,
    build_product_embedding_text,
)


class SyncService:
    def __init__(self, session: AsyncSession, embedding_service: EmbeddingService) -> None:
        self.session = session
        self.embedding_service = embedding_service
        self.settings = get_settings()
        self.business_repository = BusinessRepository(session)
        self.product_repository = ProductRepository(session)
        self.faq_repository = FAQRepository(session)
        self.sync_status_repository = SyncStatusRepository(session)

    async def sync_business_embeddings(self, business_id: int) -> dict[str, int | str]:
        business_synced = await self.sync_business_profile(business_id)
        products_synced = await self.sync_products(business_id)
        faqs_synced = await self.sync_faqs(business_id)

        status_payload = await self.update_status_snapshot(
            business_id,
            last_result="Embedding sync completed successfully.",
        )

        return {
            "business_id": business_id,
            "synced_products": status_payload["synced_products"] or products_synced,
            "synced_business_knowledge": status_payload["synced_business_knowledge"]
            or business_synced,
            "synced_faqs": status_payload["synced_faqs"] or faqs_synced,
            "embedding_model": self.settings.embedding_model,
        }

    async def sync_business_profile(self, business_id: int) -> int:
        business = await self.business_repository.get_by_id(business_id)
        text = build_business_profile_text(business)
        if not text:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Business {business_id} has no profile content to embed.",
            )

        embedding = await self.embedding_service.embed_text(text)
        metadata = {
            "city": business.get("city"),
            "delivery_zones": business.get("delivery_zones") or [],
            "payment_methods": business.get("payment_methods") or [],
        }
        await self.business_repository.upsert_profile_knowledge(
            business_id=business_id,
            title=business["name"],
            content=text,
            metadata=metadata,
            embedding=embedding,
        )
        return 1

    async def sync_products(
        self, business_id: int, product_ids: list[int] | None = None
    ) -> int:
        products = await self.product_repository.list_by_business(business_id)
        if product_ids is not None:
            product_ids_set = set(product_ids)
            products = [product for product in products if product["id"] in product_ids_set]

        if not products:
            return 0

        texts = [build_product_embedding_text(product) for product in products]
        embeddings = await self.embedding_service.embed_texts(texts)

        for product, embedding in zip(products, embeddings, strict=True):
            await self.product_repository.update_embedding(product["id"], embedding)
        return len(products)

    async def sync_faqs(self, business_id: int, faq_ids: list[int] | None = None) -> int:
        faqs = await self.faq_repository.list_by_business(business_id)
        if faq_ids is not None:
            faq_ids_set = set(faq_ids)
            faqs = [faq for faq in faqs if faq["id"] in faq_ids_set]

        if not faqs:
            return 0

        texts = [build_faq_embedding_text(faq) for faq in faqs]
        embeddings = await self.embedding_service.embed_texts(texts)

        for faq, embedding in zip(faqs, embeddings, strict=True):
            await self.faq_repository.update_embedding(faq["id"], embedding)
        return len(faqs)

    async def update_status_snapshot(
        self,
        business_id: int,
        *,
        last_result: str | None = None,
        status_value: str | None = None,
    ) -> dict[str, int | str | None]:
        counts = await self.sync_status_repository.get_embedding_counts(business_id)
        has_products = await self.product_repository.count_by_business(business_id) > 0
        resolved_status = status_value or self._resolve_status(
            counts=counts,
            has_products=has_products,
        )
        return await self.sync_status_repository.upsert_status(
            business_id=business_id,
            status_value=resolved_status,
            last_synced_at=counts.get("last_embedded_at"),
            last_result=last_result,
            synced_products=int(counts.get("synced_products") or 0),
            synced_business_knowledge=int(counts.get("synced_business_knowledge") or 0),
            synced_faqs=int(counts.get("synced_faqs") or 0),
            embedding_model=self.settings.embedding_model,
        )

    async def mark_running(self, business_id: int) -> dict[str, int | str | None]:
        return await self.sync_status_repository.mark_running(
            business_id, self.settings.embedding_model
        )

    async def mark_error(self, business_id: int, message: str) -> dict[str, int | str | None]:
        return await self.sync_status_repository.mark_error(
            business_id, message, self.settings.embedding_model
        )

    def _resolve_status(self, *, counts: dict[str, object], has_products: bool) -> str:
        if int(counts.get("synced_business_knowledge") or 0) == 0:
            return "recommended"
        if has_products and int(counts.get("synced_products") or 0) == 0:
            return "recommended"
        return "up_to_date"
