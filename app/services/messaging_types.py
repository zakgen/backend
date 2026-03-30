from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ConnectionState:
    business_id: int
    integration_type: str
    status: str
    health: str
    config: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SendMessageCommand:
    business_id: int
    phone: str
    text: str
    config: dict[str, Any]
    subaccount_sid: str


@dataclass(slots=True)
class SentMessageResult:
    provider: str
    provider_message_sid: str
    provider_status: str | None
    raw_payload: dict[str, Any]
    from_phone: str
    to_phone: str
    error_code: str | None = None


@dataclass(slots=True)
class InboundMessageEvent:
    provider: str
    provider_message_sid: str
    from_phone: str
    to_phone: str
    text: str
    customer_name: str | None
    raw_payload: dict[str, Any]


@dataclass(slots=True)
class DeliveryStatusEvent:
    provider: str
    provider_message_sid: str
    provider_status: str | None
    error_code: str | None
    raw_payload: dict[str, Any]
