from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


StorePlatform = Literal["generic", "shopify", "woocommerce", "youcan", "zid"]
OrderConfirmationStatus = Literal[
    "pending_send",
    "awaiting_customer",
    "confirmed",
    "declined",
    "edit_requested",
    "human_requested",
    "expired",
]
OrderConfirmationAction = Literal[
    "confirm",
    "decline",
    "request_edit",
    "request_human",
    "resend",
    "reopen",
]


class StoreOrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_name: str = Field(min_length=1, max_length=255)
    quantity: int = Field(default=1, ge=1, le=999)
    variant: str | None = Field(default=None, max_length=255)
    unit_price: float | None = Field(default=None, ge=0)
    sku: str | None = Field(default=None, max_length=120)


class StoreOrderIngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_store: StorePlatform = "generic"
    external_order_id: str = Field(min_length=1, max_length=255)
    customer_name: str | None = Field(default=None, max_length=255)
    customer_phone: str = Field(min_length=6, max_length=40)
    preferred_language: Literal["english", "french", "darija"] | None = None
    total_amount: float = Field(ge=0)
    currency: str = Field(default="MAD", min_length=1, max_length=12)
    payment_method: str | None = Field(default=None, max_length=120)
    delivery_city: str | None = Field(default=None, max_length=120)
    delivery_address: str | None = None
    order_notes: str | None = None
    items: list[StoreOrderItem] = Field(default_factory=list, min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    send_confirmation: bool = True


class OrderRecord(BaseModel):
    id: str
    business_id: int
    source_store: StorePlatform | str
    external_order_id: str
    customer_name: str | None = None
    customer_phone: str
    preferred_language: str | None = None
    total_amount: float
    currency: str
    payment_method: str | None = None
    delivery_city: str | None = None
    delivery_address: str | None = None
    order_notes: str | None = None
    status: str
    confirmation_status: OrderConfirmationStatus
    items: list[StoreOrderItem] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None


class OrderConfirmationEvent(BaseModel):
    id: str
    session_id: str
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None


class OrderConfirmationSessionSummary(BaseModel):
    id: str
    order_id: str
    business_id: int
    phone: str
    customer_name: str | None = None
    preferred_language: str | None = None
    status: OrderConfirmationStatus
    needs_human: bool = False
    last_detected_intent: str | None = None
    started_at: str | None = None
    last_customer_message_at: str | None = None
    confirmed_at: str | None = None
    declined_at: str | None = None
    updated_at: str | None = None


class OrderConfirmationSessionDetail(OrderConfirmationSessionSummary):
    structured_snapshot: dict[str, Any] = Field(default_factory=dict)
    order: OrderRecord
    events: list[OrderConfirmationEvent] = Field(default_factory=list)


class OrderConfirmationIngestResponse(BaseModel):
    order: OrderRecord
    session: OrderConfirmationSessionDetail
    confirmation_message_sent: bool


class OrderConfirmationSessionListResponse(BaseModel):
    sessions: list[OrderConfirmationSessionSummary] = Field(default_factory=list)
    total: int


class OrderConfirmationActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: OrderConfirmationAction
    note: str | None = Field(default=None, max_length=1000)

