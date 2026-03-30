from __future__ import annotations

from typing import Any

from app.config import get_settings
from app.schemas.search import BusinessContext, SearchMatch, SearchRequest, SearchResponse
from app.services.embedding_service import EmbeddingService
from app.services.search_formatting import (
    format_business_match,
    format_faq_match,
    format_product_match,
)
from app.services.repositories import BusinessRepository, FAQRepository, ProductRepository


class SearchService:
    def __init__(self, session: Any, embedding_service: EmbeddingService) -> None:
        self.session = session
        self.embedding_service = embedding_service
        self.settings = get_settings()
        self.business_repository = BusinessRepository(session)
        self.product_repository = ProductRepository(session)
        self.faq_repository = FAQRepository(session)

    async def search(self, payload: SearchRequest) -> SearchResponse:
        business = await self.business_repository.get_by_id(payload.business_id)
        query_embedding = await self.embedding_service.embed_text(payload.query)

        products = await self.product_repository.search(
            payload.business_id, query_embedding, payload.top_k
        )
        faqs = await self.faq_repository.search(
            payload.business_id, query_embedding, payload.top_k
        )
        knowledge = await self.business_repository.search_knowledge(
            payload.business_id, query_embedding, payload.top_k
        )

        matches = [
            format_product_match(row)
            for row in products
            if float(row["score"]) >= self.settings.search_min_score
        ]
        matches.extend(
            format_faq_match(row)
            for row in faqs
            if float(row["score"]) >= self.settings.search_min_score
        )
        matches.extend(
            format_business_match(row)
            for row in knowledge
            if float(row["score"]) >= self.settings.search_min_score
        )
        matches.sort(key=lambda item: item.score, reverse=True)

        business_context = BusinessContext.model_validate(
            {
                "id": business["id"],
                "name": business["name"],
                "description": business.get("description"),
                "city": business.get("city"),
                "shipping_policy": business.get("shipping_policy"),
                "delivery_zones": business.get("delivery_zones") or [],
                "payment_methods": business.get("payment_methods") or [],
                "profile_metadata": business.get("profile_metadata") or {},
            }
        )

        return SearchResponse(
            business_id=payload.business_id,
            query=payload.query,
            matches=matches[: payload.top_k],
            business_context=business_context,
        )
