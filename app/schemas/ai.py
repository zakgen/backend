from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.conversation import ConversationIntent, ConversationMessage


AIRunStatus = Literal["generated", "sent", "escalated", "failed"]
AIReplyDecision = Literal["send", "needs_human", "failed"]
AISourceType = Literal["product", "faq", "business_knowledge", "business_fact"]


class AIReplyHistoryMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=1600)
    direction: Literal["inbound", "outbound"]


class AIReplyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = Field(min_length=1, max_length=1600)
    phone: str | None = None
    recent_messages: list[AIReplyHistoryMessage] | None = None


class AISourceReference(BaseModel):
    type: AISourceType
    id: int | str
    name: str
    score: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class AIModelReply(BaseModel):
    reply_text: str | None = None
    intent: ConversationIntent | None = None
    language: str | None = None
    used_sources: list[AISourceReference] = Field(default_factory=list)
    grounded: bool = False
    needs_human: bool = False
    confidence: float = Field(default=0.0, ge=0, le=1)
    reason_code: str | None = None
    follow_up_question: str | None = None


class AIReplyResponse(BaseModel):
    run_id: str
    business_id: int
    phone: str | None = None
    customer_message: str
    reply_text: str | None = None
    intent: ConversationIntent | None = None
    language: str | None = None
    grounded: bool
    needs_human: bool
    confidence: float
    reason_code: str | None = None
    follow_up_question: str | None = None
    decision: AIReplyDecision
    used_sources: list[AISourceReference] = Field(default_factory=list)
    retrieved_sources: list[AISourceReference] = Field(default_factory=list)
    sent: bool = False
    outbound_message: ConversationMessage | None = None
    created_at: str
    updated_at: str


class AIRunSummary(BaseModel):
    id: str
    business_id: int
    phone: str | None = None
    status: AIRunStatus
    customer_message: str
    reply_text: str | None = None
    language: str | None = None
    intent: ConversationIntent | None = None
    needs_human: bool
    confidence: float
    fallback_reason: str | None = None
    created_at: str
    updated_at: str


class AIRunDetail(AIRunSummary):
    provider: str
    model: str
    prompt_version: str
    inbound_chat_message_id: str | None = None
    outbound_chat_message_id: str | None = None
    retrieval_summary: dict[str, Any] = Field(default_factory=dict)
    request_payload: dict[str, Any] = Field(default_factory=dict)
    response_payload: dict[str, Any] = Field(default_factory=dict)
