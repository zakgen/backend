from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
import logging
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.conversation import ConversationReplyRequest
from app.schemas.integration import WhatsAppConnectRequest
from app.services.ai_helpers import normalize_language_label
from app.services.ai_reply_service import AIReplyService
from app.services.dashboard_service import build_whatsapp_integration, chat_row_to_message, to_iso
from app.services.messaging_provider import AbstractMessagingProvider
from app.services.messaging_types import ConnectionState, SendMessageCommand
from app.services.order_confirmation_service import OrderConfirmationService
from app.services.repository_factory import RepositoryFactory


logger = logging.getLogger(__name__)
TERMINAL_ORDER_SESSION_STATUSES = {"declined", "confirmed"}


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
        factory = RepositoryFactory(session)
        self.business_repository = factory.business()
        self.chat_repository = factory.chats()
        self.integration_repository = factory.integrations()
        self.order_repository = factory.orders()
        self.order_confirmation_repository = factory.order_confirmations()

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
        if not await self._is_free_text_allowed(business_id, phone):
            logger.info(
                "Free-text reply skipped outside 24h window business_id=%s phone=%s",
                business_id,
                phone,
            )
            row = await self.chat_repository.upsert_message(
                business_id=business_id,
                phone=phone,
                customer_name=None,
                text=payload.text,
                direction="outbound",
                intent=payload.intent,
                needs_human=payload.needs_human or False,
                is_read=True,
                provider=self.provider.provider_name,
                provider_message_sid=None,
                provider_status="skipped_window",
                error_code="outside_24h_window",
                raw_payload={"skipped": True, "reason": "outside_24h_window"},
            )
            return chat_row_to_message(row).model_dump()
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

    async def _is_free_text_allowed(self, business_id: int, phone: str) -> bool:
        rows = await self.chat_repository.list_messages(
            business_id,
            phone=phone,
            direction="inbound",
            limit=1,
        )
        if not rows:
            return False
        last_inbound = self._coerce_datetime(rows[0].get("created_at"))
        if last_inbound is None:
            return False
        return datetime.now(UTC) - last_inbound <= timedelta(hours=24)

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        if isinstance(value, str):
            try:
                normalized = value.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(normalized)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
            except ValueError:
                return None
        return None

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
        try:
            confirmation_service = OrderConfirmationService(
                session=self.session,
                messaging_provider=self.provider,
            )
            handled = await confirmation_service.handle_inbound_message(
                connection=connection,
                inbound_row=row,
            )
            if handled:
                return row
            latest_session = await self.order_confirmation_repository.find_latest_by_phone(
                int(connection["business_id"]),
                str(row.get("phone") or ""),
            )
            if latest_session is not None and latest_session.get("status") in TERMINAL_ORDER_SESSION_STATUSES:
                handled_terminal_follow_up = await self._maybe_handle_finalized_order_follow_up(
                    connection=connection,
                    inbound_row=row,
                    latest_session=latest_session,
                )
                if handled_terminal_follow_up:
                    logger.info(
                        "Finalized order follow-up handled in read-only mode business_id=%s phone=%s session_id=%s status=%s",
                        connection["business_id"],
                        row.get("phone"),
                        latest_session.get("id"),
                        latest_session.get("status"),
                    )
                    return row
            ai_service = AIReplyService(
                session=self.session,
                messaging_provider=self.provider,
            )
            await ai_service.process_inbound_message(
                connection=connection,
                inbound_row=row,
            )
        except Exception as exc:
            logger.exception(
                "AI auto-reply processing failed for business %s inbound message %s",
                connection["business_id"],
                row["id"],
                exc_info=exc,
            )
            await self.chat_repository.update_message_analysis(
                int(row["id"]),
                intent=None,
                needs_human=True,
            )
        return row

    async def _maybe_handle_finalized_order_follow_up(
        self,
        *,
        connection: dict[str, Any],
        inbound_row: dict[str, Any],
        latest_session: dict[str, Any],
    ) -> bool:
        business_id = int(connection["business_id"])
        customer_text = str(inbound_row.get("text") or "")
        resolved_order = await self.order_repository.get_by_id(
            business_id,
            int(latest_session["order_id"]),
        )
        resolved_session = latest_session

        for candidate in self._extract_order_reference_candidates(customer_text):
            candidate_order = await self.order_repository.find_by_external_id(
                business_id=business_id,
                external_order_id=candidate,
            )
            if candidate_order is None:
                continue
            candidate_session = await self.order_confirmation_repository.find_latest_by_order(
                business_id,
                int(candidate_order["id"]),
            )
            if candidate_session is None or candidate_session.get("status") not in TERMINAL_ORDER_SESSION_STATUSES:
                continue
            resolved_order = candidate_order
            resolved_session = candidate_session
            break

        if not self._is_finalized_order_follow_up(
            message=customer_text,
            latest_order=resolved_order,
        ):
            return False

        business_row = await self.business_repository.get_by_id(business_id)
        language = normalize_language_label(
            resolved_session.get("preferred_language") or resolved_order.get("preferred_language"),
            "darija",
        )
        reply_text = self._build_finalized_order_reply(
            language=language,
            order_row=resolved_order,
            session_row=resolved_session,
            customer_text=customer_text,
            business_row=business_row,
        )
        await self._send_connection_reply(
            connection=connection,
            phone=str(inbound_row.get("phone") or ""),
            text=reply_text,
        )
        return True

    async def _send_connection_reply(
        self,
        *,
        connection: dict[str, Any],
        phone: str,
        text: str,
    ) -> dict[str, Any]:
        config = dict(connection.get("config") or {})
        result = await self.provider.send_text(
            SendMessageCommand(
                business_id=int(connection["business_id"]),
                phone=phone,
                text=text,
                config=config,
                subaccount_sid=str(config["subaccount_sid"]),
            )
        )
        row = await self.chat_repository.upsert_message(
            business_id=int(connection["business_id"]),
            phone=result.to_phone,
            customer_name=None,
            text=text,
            direction="outbound",
            intent="autre",
            needs_human=True,
            is_read=True,
            provider=result.provider,
            provider_message_sid=result.provider_message_sid,
            provider_status=result.provider_status,
            error_code=result.error_code,
            raw_payload=result.raw_payload,
        )
        await self.integration_repository.increment_whatsapp_metrics(
            int(connection["business_id"]),
            sent_delta=1,
            failed_delta=1 if result.error_code else 0,
            touch_last_activity=True,
        )
        return row

    def _extract_order_reference_candidates(self, message: str) -> list[str]:
        candidates: list[str] = []
        token = []
        for char in message:
            if char.isalnum() or char in {"-", "_", "#"}:
                token.append(char)
                continue
            if token:
                candidates.append("".join(token))
                token = []
        if token:
            candidates.append("".join(token))
        normalized: list[str] = []
        for candidate in candidates:
            stripped = candidate.strip().lstrip("#").rstrip(".,!?;:")
            if len(stripped) < 4 or not any(ch.isdigit() for ch in stripped):
                continue
            if stripped not in normalized:
                normalized.append(stripped)
        return normalized

    def _is_finalized_order_follow_up(self, *, message: str, latest_order: dict[str, Any]) -> bool:
        normalized = message.strip().lower()
        if not normalized:
            return False
        if self._looks_like_finalized_order_mutation(message):
            return True
        if any(
            token in normalized
            for token in (
                "order",
                "commande",
                "طلب",
                "status",
                "where is",
                "delivery",
                "tracking",
                "suivi",
                "حالة",
                "تتبع",
            )
        ):
            return True
        external_order_id = str(latest_order.get("external_order_id") or "").strip().lower()
        return bool(external_order_id and external_order_id in normalized)

    def _build_finalized_order_reply(
        self,
        *,
        language: str,
        order_row: dict[str, Any],
        session_row: dict[str, Any],
        customer_text: str,
        business_row: dict[str, Any],
    ) -> str:
        external_order_id = str(order_row.get("external_order_id") or order_row.get("id"))
        session_status = str(session_row.get("status") or "")
        final_label = {
            "confirmed": {
                "english": "confirmed",
                "french": "confirmée",
                "darija": "متأكد",
            },
            "declined": {
                "english": "cancelled",
                "french": "annulée",
                "darija": "ملغي",
            },
        }["declined" if session_status == "declined" else "confirmed"][language]
        mutation_requested = self._looks_like_finalized_order_mutation(customer_text)
        support_line = self._build_support_contact_line(language, business_row)
        if language == "french":
            locked_line = (
                "Cette commande ne peut plus être modifiée dans ce chat."
                if mutation_requested
                else "Cette commande est déjà finalisée."
            )
            return (
                f"📦 La commande *#{external_order_id}* est déjà {final_label}.\n\n"
                f"{locked_line}\n\n"
                f"{support_line}"
            )
        if language == "darija":
            locked_line = (
                "هاد الطلب ما بقىش ممكن تبدلو من هاد الشات."
                if mutation_requested
                else "هاد الطلب راه تسالى وتأكد بالفعل."
                if session_status == "confirmed"
                else "هاد الطلب راه تلغى بالفعل."
            )
            return (
                f"📦 الطلب *#{external_order_id}* راه {final_label} بالفعل.\n\n"
                f"{locked_line}\n\n"
                f"{support_line}"
            )
        locked_line = (
            "This order can no longer be changed in this chat."
            if mutation_requested
            else "This order is already finalized."
        )
        return (
            f"📦 Order *#{external_order_id}* is already {final_label}.\n\n"
            f"{locked_line}\n\n"
            f"{support_line}"
        )

    def _looks_like_finalized_order_mutation(self, message: str) -> bool:
        normalized = message.strip().lower()
        return any(
            token in normalized
            for token in (
                "change",
                "edit",
                "modify",
                "cancel",
                "confirm",
                "address",
                "adresse",
                "city",
                "ville",
                "quantity",
                "variant",
                "color",
                "بدل",
                "تعديل",
                "إلغاء",
                "تأكيد",
                "العنوان",
                "المدينة",
                "الكمية",
                "اللون",
            )
        )

    def _build_support_contact_line(self, language: str, business_row: dict[str, Any]) -> str:
        whatsapp = str(business_row.get("whatsapp_number") or "").strip()
        phone = str(business_row.get("support_phone") or "").strip()
        email = str(business_row.get("support_email") or "").strip()
        parts = []
        if whatsapp:
            parts.append(f"WhatsApp: {whatsapp}")
        if phone:
            parts.append(f"Phone: {phone}")
        if email:
            parts.append(f"Email: {email}")
        details = "; ".join(parts)
        if language == "french":
            return f"Merci de contacter le support. {details}".strip()
        if language == "darija":
            return f"من فضلك تواصل مع الدعم. {details}".strip()
        return f"Please contact support. {details}".strip()

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
