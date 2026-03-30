from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

from app.services.messaging_types import (
    ConnectionState,
    DeliveryStatusEvent,
    InboundMessageEvent,
    SendMessageCommand,
    SentMessageResult,
)


class AbstractMessagingProvider(ABC):
    provider_name: str

    @abstractmethod
    async def begin_connection(
        self,
        business_id: int,
        connect_payload: dict[str, Any],
        existing_connection: ConnectionState | None = None,
    ) -> ConnectionState:
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self, connection_state: ConnectionState) -> ConnectionState:
        raise NotImplementedError

    @abstractmethod
    async def send_text(self, command: SendMessageCommand) -> SentMessageResult:
        raise NotImplementedError

    @abstractmethod
    def validate_webhook(
        self, headers: Mapping[str, str], url: str, params: Mapping[str, Any]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def parse_inbound_webhook(self, params: Mapping[str, Any]) -> InboundMessageEvent:
        raise NotImplementedError

    @abstractmethod
    def parse_status_webhook(self, params: Mapping[str, Any]) -> DeliveryStatusEvent:
        raise NotImplementedError
