from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


ConversationDirection = Literal["inbound", "outbound"]
ConversationIntent = Literal[
    "livraison",
    "prix",
    "disponibilite",
    "retour",
    "paiement",
    "infos_produit",
    "autre",
]


class ConversationMessage(BaseModel):
    id: str
    phone: str
    text: str
    direction: ConversationDirection
    timestamp: str
    intent: ConversationIntent | None = None
    needs_human: bool | None = None


class ConversationSummary(BaseModel):
    phone: str
    customer_name: str | None = None
    last_message: str
    last_timestamp: str
    unread_count: int
    intents: list[str]
    needs_human: bool
    inbound_count: int
    outbound_count: int


class ConversationThread(BaseModel):
    phone: str
    customer_name: str | None = None
    first_contact_at: str | None = None
    messages: list[ConversationMessage]


class ConversationReplyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=1600)
    intent: ConversationIntent | None = None
    needs_human: bool | None = None
