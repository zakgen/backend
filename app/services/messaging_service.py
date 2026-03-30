from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.conversation import ConversationReplyRequest
from app.schemas.integration import WhatsAppConnectRequest
from app.services.dashboard_service import build_whatsapp_integration, chat_row_to_message, to_iso
from app.services.messaging_provider import AbstractMessagingProvider
from app.services.messaging_types import ConnectionState, SendMessageCommand
from app.services.repositories import BusinessRepository, ChatRepository, IntegrationRepository


def _connection_state_from_row(
    business_id: int, row: dict[str, Any] | None
) -> ConnectionState | None:
    if row is None:
        return None
    return ConnectionState(
        business_id=business_id,
        integration_type=row["integration_type"],
        status=row["status"],
        health=row["health"],
        config=dict(row.get("config") or {}),
        metrics=dict(row.get("metrics") or {}),
    )


class MessagingService:
    def __init__(
        self,
        *,
        session: AsyncSession,
        provider: AbstractMessagingProvider,
    ) -> None:
        self.session = session
        self.provider = provider
        self.business_repository = BusinessRepository(session)
        self.chat_repository = ChatRepository(session)
        self.integration_repository = IntegrationRepository(session)

    async def begin_whatsapp_connection(
        self, business_id: int, payload: WhatsAppConnectRequest
    ) -> dict[str, Any]:
        business = await self.business_repository.get_by_id(business_id)
        existing = _connection_state_from_row(
            business_id,
            await self.integration_repository.get_connection(business_id, "whatsapp"),
        )
        state = await self.provider.begin_connection(
            business_id,
            payload.model_dump(),
            existing_connection=existing,
        )
        row = await self.integration_repository.upsert_connection(
            business_id=business_id,
            integration_type="whatsapp",
            status_value=state.status,
            health=state.health,
            config=state.config,
            metrics=state.metrics,
        )
        return build_whatsapp_integration(business["name"], row).model_dump()

    async def disconnect_whatsapp(self, business_id: int) -> dict[str, Any]:
        business = await self.business_repository.get_by_id(business_id)
        existing_row = await self.integration_repository.get_connection(business_id, "whatsapp")
        existing_state = _connection_state_from_row(business_id, existing_row)
        if existing_state is None:
            existing_state = ConnectionState(
                business_id=business_id,
                integration_type="whatsapp",
                status="disconnected",
                health="attention",
                config={},
                metrics={},
            )
        state = await self.provider.disconnect(existing_state)
        row = await self.integration_repository.upsert_connection(
            business_id=business_id,
            integration_type="whatsapp",
            status_value=state.status,
            health=state.health,
            config=state.config,
            metrics=state.metrics,
            last_activity_at=(existing_row or {}).get("last_activity_at"),
            last_synced_at=(existing_row or {}).get("last_synced_at"),
        )
        return build_whatsapp_integration(business["name"], row).model_dump()

    async def get_ready_whatsapp_connection(self, business_id: int) -> dict[str, Any]:
        connection = await self.integration_repository.get_connection(business_id, "whatsapp")
        if connection is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="WhatsApp integration is not configured.",
            )
        config = dict(connection.get("config") or {})
        if (
            connection.get("status") != "connected"
            or config.get("provider") != self.provider.provider_name
            or config.get("onboarding_status") != "connected"
            or not config.get("subaccount_sid")
            or not config.get("sender_sid")
            or not (config.get("whatsapp_number") or config.get("phone_number"))
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="WhatsApp integration is not finalized for this business.",
            )
        return connection

    async def test_whatsapp(self, business_id: int) -> dict[str, Any]:
        business = await self.business_repository.get_by_id(business_id)
        connection = await self.get_ready_whatsapp_connection(business_id)
        updated = await self.integration_repository.upsert_connection(
            business_id=business_id,
            integration_type="whatsapp",
            status_value="connected",
            health="healthy",
            config=dict(connection.get("config") or {}),
            metrics=dict(connection.get("metrics") or {}),
            last_activity_at=connection.get("last_activity_at"),
            last_synced_at=connection.get("last_synced_at"),
        )
        return build_whatsapp_integration(business["name"], updated).model_dump()

    async def send_reply(
        self, business_id: int, phone: str, payload: ConversationReplyRequest
    ) -> dict[str, Any]:
        connection = await self.get_ready_whatsapp_connection(business_id)
        config = dict(connection.get("config") or {})
        result = await self.provider.send_text(
            SendMessageCommand(
                business_id=business_id,
                phone=phone,
                text=payload.text,
                config=config,
                subaccount_sid=str(config["subaccount_sid"]),
            )
        )
        row = await self.chat_repository.upsert_message(
            business_id=business_id,
            phone=result.to_phone,
            customer_name=None,
            text=payload.text,
            direction="outbound",
            intent=payload.intent,
            needs_human=payload.needs_human or False,
            is_read=True,
            provider=result.provider,
            provider_message_sid=result.provider_message_sid,
            provider_status=result.provider_status,
            error_code=result.error_code,
            raw_payload=result.raw_payload,
        )
        await self.integration_repository.increment_whatsapp_metrics(
            business_id,
            sent_delta=1,
            failed_delta=1 if result.error_code else 0,
            touch_last_activity=True,
        )
        return chat_row_to_message(row).model_dump()

    async def handle_inbound_webhook(
        self, *, url: str, headers: Mapping[str, str], params: Mapping[str, Any]
    ) -> dict[str, Any]:
        self.provider.validate_webhook(headers, url, params)
        event = self.provider.parse_inbound_webhook(params)
        connection = await self.integration_repository.find_whatsapp_connection(
            sender_phone=event.to_phone,
            subaccount_sid=str(params.get("AccountSid") or ""),
        )
        if connection is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No business is linked to this Twilio WhatsApp sender.",
            )
        config = dict(connection.get("config") or {})
        config["last_webhook_validation_at"] = to_iso(datetime.now(UTC))
        await self.integration_repository.upsert_connection(
            business_id=int(connection["business_id"]),
            integration_type="whatsapp",
            status_value="connected"
            if config.get("onboarding_status") == "connected"
            else connection["status"],
            health="healthy"
            if config.get("onboarding_status") == "connected"
            else connection["health"],
            config=config,
            metrics=dict(connection.get("metrics") or {}),
            last_activity_at=datetime.now(UTC),
            last_synced_at=connection.get("last_synced_at"),
        )
        row = await self.chat_repository.upsert_message(
            business_id=int(connection["business_id"]),
            phone=event.from_phone,
            customer_name=event.customer_name,
            text=event.text,
            direction="inbound",
            intent=None,
            needs_human=False,
            is_read=False,
            provider=event.provider,
            provider_message_sid=event.provider_message_sid,
            provider_status="received",
            error_code=None,
            raw_payload=event.raw_payload,
        )
        await self.integration_repository.increment_whatsapp_metrics(
            int(connection["business_id"]),
            received_delta=1,
            touch_last_activity=True,
        )
        return row

    async def handle_status_webhook(
        self, *, url: str, headers: Mapping[str, str], params: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        self.provider.validate_webhook(headers, url, params)
        event = self.provider.parse_status_webhook(params)
        row = await self.chat_repository.update_provider_status(
            provider_message_sid=event.provider_message_sid,
            provider_status=event.provider_status,
            error_code=event.error_code,
            raw_payload=event.raw_payload,
        )
        if row is None:
            return None
        if event.error_code:
            await self.integration_repository.increment_whatsapp_metrics(
                int(row["business_id"]),
                failed_delta=1,
                touch_last_activity=True,
            )
        return row
