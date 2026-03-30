from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class BusinessUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    city: str | None = Field(default=None, max_length=120)
    shipping_policy: str | None = None
    delivery_zones: list[str] = Field(default_factory=list)
    payment_methods: list[str] = Field(default_factory=list)
    profile_metadata: dict[str, Any] = Field(default_factory=dict)


class BusinessResponse(BaseModel):
    id: int
    name: str
    description: str | None = None
    city: str | None = None
    shipping_policy: str | None = None
    delivery_zones: list[str] = Field(default_factory=list)
    payment_methods: list[str] = Field(default_factory=list)
    profile_metadata: dict[str, Any] = Field(default_factory=dict)
    updated_at: str | None = None


ToneOfVoice = Literal["formal", "friendly", "professional"]
PaymentMethod = Literal["cash_on_delivery", "card_payment", "bank_transfer"]


class BusinessFAQItem(BaseModel):
    id: str
    question: str
    answer: str


class BusinessFAQUpdateItem(BaseModel):
    id: str | None = None
    question: str = Field(min_length=1)
    answer: str = Field(min_length=1)


class BusinessProfile(BaseModel):
    id: int
    name: str
    summary: str
    niche: str
    city: str
    supported_languages: list[str] = Field(default_factory=list)
    tone_of_voice: ToneOfVoice
    opening_hours: list[str] = Field(default_factory=list)
    delivery_zones: list[str] = Field(default_factory=list)
    delivery_time: str
    shipping_policy: str
    return_policy: str
    payment_methods: list[PaymentMethod] = Field(default_factory=list)
    faq: list[BusinessFAQItem] = Field(default_factory=list)
    order_rules: list[str] = Field(default_factory=list)
    escalation_contact: str
    upsell_rules: list[str] = Field(default_factory=list)
    updated_at: str


class BusinessProfileUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=255)
    summary: str | None = None
    niche: str | None = None
    city: str | None = None
    supported_languages: list[str] | None = None
    tone_of_voice: ToneOfVoice | None = None
    opening_hours: list[str] | None = None
    delivery_zones: list[str] | None = None
    delivery_time: str | None = None
    shipping_policy: str | None = None
    return_policy: str | None = None
    payment_methods: list[PaymentMethod] | None = None
    faq: list[BusinessFAQUpdateItem] | None = None
    order_rules: list[str] | None = None
    escalation_contact: str | None = None
    upsell_rules: list[str] | None = None


class SetupChecklistItem(BaseModel):
    id: Literal["business", "products", "whatsapp"]
    label: str
    completed: bool
    detail: str
    action_href: str | None = None
    action_label: str | None = None


class SetupChecklist(BaseModel):
    completed_count: int
    total: int
    items: list[SetupChecklistItem]


class OverviewStats(BaseModel):
    total_conversations: int
    messages_handled: int
    active_products: int
    ai_knowledge_status: str


class AIInsight(BaseModel):
    title: str
    description: str


class OverviewData(BaseModel):
    stats: OverviewStats
    recent_chats: list["ConversationSummary"] = Field(default_factory=list)
    recent_products: list["Product"] = Field(default_factory=list)
    ai_insight: AIInsight
    sync_notice: str | None = None
    checklist: SetupChecklist


from app.schemas.conversation import ConversationSummary
from app.schemas.product import Product

OverviewData.model_rebuild()
