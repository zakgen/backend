from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    business_id: int = Field(gt=0)
    query: str = Field(min_length=2, max_length=1000)
    top_k: int = Field(default=5, ge=1, le=20)


class SearchMatch(BaseModel):
    type: Literal["product", "business_knowledge", "faq"]
    id: int
    name: str
    description: str | None = None
    price: float | None = None
    currency: str | None = None
    score: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class BusinessContext(BaseModel):
    id: int
    name: str
    description: str | None = None
    city: str | None = None
    shipping_policy: str | None = None
    delivery_zones: list[str] = Field(default_factory=list)
    payment_methods: list[str] = Field(default_factory=list)
    profile_metadata: dict[str, Any] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    business_id: int
    query: str
    matches: list[SearchMatch] = Field(default_factory=list)
    business_context: BusinessContext


class EmbeddingSyncResponse(BaseModel):
    business_id: int
    synced_products: int
    synced_business_knowledge: int
    synced_faqs: int
    embedding_model: str


class SyncStatusResponse(BaseModel):
    business_id: int
    status: Literal["up_to_date", "recommended", "running", "error"]
    last_synced_at: str | None = None
    last_result: str | None = None
    synced_products: int
    synced_business_knowledge: int
    synced_faqs: int
    embedding_model: str
    ai_ready: bool
    stale_reasons: list[str] = Field(default_factory=list)
