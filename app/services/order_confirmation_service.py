from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.order_confirmation import (
    OrderConfirmationAction,
    OrderConfirmationActionRequest,
    OrderSessionInterpretation,
    StoreOrderIngestRequest,
)
from app.services.ai_helpers import normalize_language_label
from app.services.ai_reply_service import build_llm_provider
from app.services.dashboard_service import to_iso
from app.services.llm_provider import AbstractLLMProvider
from app.services.messaging_provider import AbstractMessagingProvider
from app.services.messaging_types import SendMessageCommand
from app.services.repository_factory import RepositoryFactory
from app.utils.phones import normalize_phone_number


ACTIVE_SESSION_STATUSES = {
    "pending_send",
    "awaiting_customer",
    "edit_requested",
    "human_requested",
}


class OrderConfirmationService:
    def __init__(
        self,
        *,
        session: AsyncSession,
        messaging_provider: AbstractMessagingProvider,
        llm_provider: AbstractLLMProvider | None = None,
    ) -> None:
        self.session = session
        self.messaging_provider = messaging_provider
        self.llm_provider = llm_provider or build_llm_provider()
        factory = RepositoryFactory(session)
        self.business_repository = factory.business()
        self.integration_repository = factory.integrations()
        self.chat_repository = factory.chats()
        self.order_repository = factory.orders()
        self.order_confirmation_repository = factory.order_confirmations()

    async def ingest_store_order(
        self, business_id: int, payload: StoreOrderIngestRequest
    ) -> dict[str, Any]:
        business_row = await self.business_repository.get_by_id(business_id)
        order_row = await self.order_repository.upsert_order(
            business_id=business_id,
            payload={
                **payload.model_dump(),
                "status": "pending_confirmation",
                "confirmation_status": "pending_send",
            },
        )
        snapshot = self._build_snapshot(business_row, order_row)
        session_row = await self.order_confirmation_repository.find_latest_by_order(
            business_id, int(order_row["id"])
        )
        if session_row is None or session_row["status"] not in ACTIVE_SESSION_STATUSES:
            session_row = await self.order_confirmation_repository.create_session(
                business_id=business_id,
                order_id=int(order_row["id"]),
                phone=str(order_row["customer_phone"]),
                customer_name=order_row.get("customer_name"),
                preferred_language=order_row.get("preferred_language"),
                status_value="pending_send",
                needs_human=False,
                last_detected_intent="order_confirmation_pending",
                structured_snapshot=snapshot,
            )
            await self.order_confirmation_repository.add_event(
                business_id=business_id,
                session_id=int(session_row["id"]),
                order_id=int(order_row["id"]),
                event_type="order_ingested",
                payload={
                    "source_store": order_row["source_store"],
                    "external_order_id": order_row["external_order_id"],
                },
            )
        else:
            session_row = await self.order_confirmation_repository.update_session(
                int(session_row["id"]),
                {
                    "preferred_language": order_row.get("preferred_language"),
                    "structured_snapshot": snapshot,
                    "last_detected_intent": "order_confirmation_refreshed",
                },
            )
            await self.order_confirmation_repository.add_event(
                business_id=business_id,
                session_id=int(session_row["id"]),
                order_id=int(order_row["id"]),
                event_type="order_refreshed",
                payload={"external_order_id": order_row["external_order_id"]},
            )

        confirmation_message_sent = False
        if payload.send_confirmation:
            connection = await self._get_ready_whatsapp_connection(business_id)
            confirmation_message = self._build_initial_confirmation_message(
                business_name=str(business_row.get("name") or ""),
                order_row=order_row,
                language=normalize_language_label(order_row.get("preferred_language"), "french"),
            )
            outbound_row = await self._send_text(
                business_id=business_id,
                phone=str(order_row["customer_phone"]),
                text=confirmation_message,
                connection=connection,
            )
            session_row = await self.order_confirmation_repository.update_session(
                int(session_row["id"]),
                {
                    "status": "awaiting_customer",
                    "last_outbound_message_sid": outbound_row.get("provider_message_sid"),
                    "structured_snapshot": snapshot,
                },
            )
            order_row = await self.order_repository.update_order_status(
                business_id=business_id,
                order_id=int(order_row["id"]),
                status_value="pending_confirmation",
                confirmation_status="awaiting_customer",
                metadata=dict(order_row.get("metadata") or {}),
            )
            confirmation_message_sent = True
            await self.order_confirmation_repository.add_event(
                business_id=business_id,
                session_id=int(session_row["id"]),
                order_id=int(order_row["id"]),
                event_type="confirmation_sent",
                payload={"text": confirmation_message},
            )

        return {
            "order": order_row,
            "session": session_row,
            "confirmation_message_sent": confirmation_message_sent,
        }

    async def handle_inbound_message(
        self,
        *,
        connection: dict[str, Any],
        inbound_row: dict[str, Any],
    ) -> bool:
        business_id = int(connection["business_id"])
        phone = str(inbound_row.get("phone") or "")
        session_row = await self.order_confirmation_repository.find_active_session(
            business_id, phone
        )
        if session_row is None:
            return False

        message_text = str(inbound_row.get("text") or "")
        snapshot = dict(session_row.get("structured_snapshot") or {})
        session_language = normalize_language_label(
            session_row.get("preferred_language")
            or snapshot.get("preferred_language"),
            "french",
        )
        order_row = await self.order_repository.get_by_id(
            business_id, int(session_row["order_id"])
        )
        action = self._detect_customer_action(message_text)
        should_detect_language = action is None and self._should_detect_language_for_message(
            message_text
        )
        language = session_language
        should_persist_language = False
        if should_detect_language:
            try:
                detected_language, _ = await self.llm_provider.detect_language(
                    message=message_text
                )
            except Exception:
                detected_language = session_language
            language = normalize_language_label(
                detected_language,
                fallback=session_language,
            )
            should_persist_language = language != session_language
        interpretation: OrderSessionInterpretation | None = None
        if action is None:
            interpretation = await self._interpret_session_message(
                customer_message=message_text,
                session_row=session_row,
                order_row=order_row,
                snapshot=snapshot,
            )
            interpreted_language = normalize_language_label(
                interpretation.language,
                fallback=language,
            )
            if should_detect_language:
                language = interpreted_language
                should_persist_language = language != session_language
        if should_persist_language:
            snapshot["preferred_language"] = language
        session_update: dict[str, Any] = {
            "last_customer_message_at": datetime.now(UTC),
            "last_detected_intent": action
            or (interpretation.primary_action if interpretation is not None else "free_text"),
        }
        if should_persist_language:
            session_update["preferred_language"] = language
            session_update["structured_snapshot"] = snapshot
        order_status = order_row.get("status") or "pending_confirmation"
        confirmation_status = order_row.get("confirmation_status") or session_row["status"]
        finalized_order: dict[str, Any] | None = None
        outbound_text: str
        event_type: str
        needs_human = False
        event_payload: dict[str, Any] = {"message": message_text, "action": action}

        if action == "confirm":
            snapshot, finalized_order = self._prepare_confirmed_order_snapshot(
                session_row=session_row,
                snapshot=snapshot,
                order_row=order_row,
                language=language,
            )
            session_update.update(
                {
                    "status": "confirmed",
                    "confirmed_at": datetime.now(UTC),
                    "needs_human": False,
                    "structured_snapshot": snapshot,
                }
            )
            order_status = "confirmed"
            confirmation_status = "confirmed"
            event_type = "customer_confirmed"
            outbound_text = self._build_confirmed_reply(language, order_row)
            event_payload["finalized_order"] = finalized_order
        elif action == "decline":
            session_update.update({"status": "declined", "declined_at": datetime.now(UTC), "needs_human": True})
            order_status = "cancelled_by_customer"
            confirmation_status = "declined"
            event_type = "customer_declined"
            outbound_text = self._build_declined_reply(language)
            needs_human = True
        elif action == "request_edit":
            session_update.update({"status": "edit_requested", "needs_human": False})
            order_status = "pending_confirmation"
            confirmation_status = "awaiting_customer"
            event_type = "customer_requested_edit"
            outbound_text = self._build_edit_reply(language)
            event_payload["automation_outcome"] = "collecting_edit_details"
        elif action == "request_human":
            session_update.update({"status": "human_requested", "needs_human": True})
            order_status = "needs_review"
            confirmation_status = "human_requested"
            event_type = "customer_requested_human"
            outbound_text = self._build_human_reply(language)
            needs_human = True
            event_payload["automation_outcome"] = "human_review"
        elif interpretation is not None:
            (
                session_update,
                order_status,
                confirmation_status,
                event_type,
                outbound_text,
                needs_human,
                snapshot,
                finalized_order,
            ) = self._apply_ai_interpretation(
                interpretation=interpretation,
                session_row=session_row,
                order_row=order_row,
                snapshot=snapshot,
                language=language,
                default_session_update=session_update,
                default_order_status=order_status,
                default_confirmation_status=confirmation_status,
            )
            event_payload.update(
                {
                    "normalized_edits": snapshot.get("latest_detected_edits")
                    or [{"field": edit.field, "value": edit.value} for edit in interpretation.edits],
                    "automation_outcome": "awaiting_final_confirmation"
                    if snapshot.get("awaiting_final_confirmation_after_edits") and not needs_human
                    else "human_review"
                    if needs_human
                    else "automated_answer",
                    "applied_to_snapshot": bool(snapshot.get("latest_detected_edits")),
                    "awaiting_final_confirmation_after_edits": bool(
                        snapshot.get("awaiting_final_confirmation_after_edits")
                    ),
                }
            )
        elif session_row["status"] == "edit_requested":
            pending_edits = list(snapshot.get("pending_edits") or [])
            pending_edits.append(
                {
                    "message": message_text,
                    "received_at": to_iso(datetime.now(UTC)),
                }
            )
            snapshot["pending_edits"] = pending_edits
            session_update.update(
                {
                    "status": "human_requested",
                    "needs_human": True,
                    "structured_snapshot": snapshot,
                }
            )
            order_status = "needs_review"
            confirmation_status = "edit_requested"
            event_type = "customer_shared_edit_details"
            outbound_text = self._build_edit_details_reply(language)
            needs_human = True
            event_payload["automation_outcome"] = "human_review"
        else:
            session_update.update({"status": "human_requested", "needs_human": True})
            order_status = "needs_review"
            confirmation_status = "human_requested"
            event_type = "customer_unrecognized_reply"
            outbound_text = self._build_fallback_reply(language)
            needs_human = True
            event_payload["automation_outcome"] = "human_review"

        session_row = await self.order_confirmation_repository.update_session(
            int(session_row["id"]),
            session_update,
        )
        order_row = await self.order_repository.update_order_status(
            business_id=business_id,
            order_id=int(order_row["id"]),
            status_value=order_status,
            confirmation_status=confirmation_status,
            metadata=self._build_order_metadata(
                order_row=order_row,
                snapshot=snapshot,
                confirmation_status=confirmation_status,
            ),
            finalized_order=finalized_order,
        )
        await self.order_confirmation_repository.add_event(
            business_id=business_id,
            session_id=int(session_row["id"]),
            order_id=int(order_row["id"]),
            event_type=event_type,
            payload=event_payload
            | {"ai_interpretation": interpretation.model_dump() if interpretation is not None else None},
        )
        await self.chat_repository.update_message_analysis(
            int(inbound_row["id"]),
            intent="autre",
            needs_human=needs_human,
        )
        await self._send_text(
            business_id=business_id,
            phone=phone,
            text=outbound_text,
            connection=connection,
        )
        return True

    async def _interpret_session_message(
        self,
        *,
        customer_message: str,
        session_row: dict[str, Any],
        order_row: dict[str, Any],
        snapshot: dict[str, Any],
    ) -> OrderSessionInterpretation:
        try:
            interpretation, _ = await self.llm_provider.interpret_order_session(
                customer_message=customer_message,
                preferred_language=str(session_row.get("preferred_language") or ""),
                session_status=str(session_row.get("status") or "awaiting_customer"),
                order_snapshot={
                    **snapshot,
                    "order_status": order_row.get("status"),
                    "confirmation_status": order_row.get("confirmation_status"),
                },
            )
            return interpretation
        except Exception:
            return OrderSessionInterpretation(
                language=normalize_language_label(
                    session_row.get("preferred_language"), "french"
                ),
                primary_action="unknown",
                confidence=0.0,
                needs_human=True,
                reply_summary="Fallback interpretation due to LLM failure.",
            )

    def _apply_ai_interpretation(
        self,
        *,
        interpretation: OrderSessionInterpretation,
        session_row: dict[str, Any],
        order_row: dict[str, Any],
        snapshot: dict[str, Any],
        language: str,
        default_session_update: dict[str, Any],
        default_order_status: str,
        default_confirmation_status: str,
    ) -> tuple[dict[str, Any], str, str, str, str, bool, dict[str, Any], dict[str, Any] | None]:
        session_update = dict(default_session_update)
        order_status = default_order_status
        confirmation_status = default_confirmation_status
        finalized_order: dict[str, Any] | None = None
        needs_human = interpretation.needs_human or interpretation.confidence < 0.55
        applied_edits, ambiguous_edits, snapshot = self._apply_interpreted_edits_to_snapshot(
            snapshot=snapshot,
            order_row=order_row,
            interpretation=interpretation,
        )
        if ambiguous_edits:
            needs_human = True

        primary_action = interpretation.primary_action
        secondary_actions = set(interpretation.secondary_actions)

        if primary_action == "confirm" and not needs_human and not interpretation.edits:
            snapshot, finalized_order = self._prepare_confirmed_order_snapshot(
                session_row=session_row,
                snapshot=snapshot,
                order_row=order_row,
                language=language,
            )
            session_update.update(
                {
                    "status": "confirmed",
                    "confirmed_at": datetime.now(UTC),
                    "needs_human": False,
                    "structured_snapshot": snapshot,
                }
            )
            return (
                session_update,
                "confirmed",
                "confirmed",
                "customer_confirmed_ai",
                self._build_confirmed_reply(language, order_row),
                False,
                snapshot,
                finalized_order,
            )

        if primary_action == "decline":
            session_update.update(
                {
                    "status": "declined",
                    "declined_at": datetime.now(UTC),
                    "needs_human": True,
                    "structured_snapshot": snapshot,
                }
            )
            return (
                session_update,
                "cancelled_by_customer",
                "declined",
                "customer_declined_ai",
                self._build_declined_reply(language),
                True,
                snapshot,
                None,
            )

        if primary_action == "edit_request" or interpretation.edits or "edit_request" in secondary_actions:
            if needs_human:
                session_update.update(
                    {
                        "status": "human_requested",
                        "needs_human": True,
                        "structured_snapshot": snapshot,
                    }
                )
                return (
                    session_update,
                    "needs_review",
                    "human_requested",
                    "customer_requested_edit_ai",
                    self._build_human_reply(language),
                    True,
                    snapshot,
                    None,
                )

            snapshot["latest_detected_edits"] = applied_edits
            snapshot["awaiting_final_confirmation_after_edits"] = True
            session_update.update(
                {
                    "status": "awaiting_customer",
                    "needs_human": False,
                    "last_detected_intent": "awaiting_final_confirmation_after_edits",
                    "structured_snapshot": snapshot,
                }
            )
            return (
                session_update,
                "pending_confirmation",
                "awaiting_customer",
                "customer_requested_edit_ai",
                self._build_edit_interpretation_reply(language, interpretation, snapshot),
                False,
                snapshot,
                None,
            )

        if primary_action == "delivery_question":
            session_update.update({"status": "awaiting_customer", "needs_human": False})
            return (
                session_update,
                order_status,
                confirmation_status,
                "customer_asked_delivery_question",
                self._build_delivery_question_reply(language, order_row, snapshot),
                False,
                snapshot,
                None,
            )

        if primary_action == "payment_question":
            session_update.update({"status": "awaiting_customer", "needs_human": False})
            return (
                session_update,
                order_status,
                confirmation_status,
                "customer_asked_payment_question",
                self._build_payment_question_reply(language, order_row),
                False,
                snapshot,
                None,
            )

        if primary_action == "return_policy_question":
            session_update.update({"status": "awaiting_customer", "needs_human": False})
            return (
                session_update,
                order_status,
                confirmation_status,
                "customer_asked_return_policy_question",
                self._build_return_policy_question_reply(language),
                False,
                snapshot,
                None,
            )

        if primary_action == "support_request" or needs_human:
            session_update.update(
                {
                    "status": "human_requested",
                    "needs_human": True,
                    "structured_snapshot": snapshot,
                }
            )
            return (
                session_update,
                "needs_review",
                "human_requested",
                "customer_requested_human_ai",
                self._build_human_reply(language),
                True,
                snapshot,
                None,
            )

        session_update.update(
            {
                "status": "human_requested",
                "needs_human": True,
                "structured_snapshot": snapshot,
            }
        )
        return (
            session_update,
            "needs_review",
            "human_requested",
            "customer_unrecognized_reply_ai",
            self._build_fallback_reply(language),
            True,
            snapshot,
            None,
        )

    async def list_sessions(
        self, business_id: int, *, status_value: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        return await self.order_confirmation_repository.list_sessions(
            business_id, status_value=status_value, limit=limit
        )

    async def get_session_detail(self, business_id: int, session_id: int) -> dict[str, Any]:
        session_row = await self.order_confirmation_repository.get_session(business_id, session_id)
        order_row = await self.order_repository.get_by_id(
            business_id, int(session_row["order_id"])
        )
        events = await self.order_confirmation_repository.list_events(session_id)
        return {
            **session_row,
            "order": order_row,
            "events": events,
        }

    async def apply_action(
        self,
        business_id: int,
        session_id: int,
        payload: OrderConfirmationActionRequest,
    ) -> dict[str, Any]:
        session_row = await self.order_confirmation_repository.get_session(business_id, session_id)
        order_row = await self.order_repository.get_by_id(
            business_id, int(session_row["order_id"])
        )
        language = normalize_language_label(session_row.get("preferred_language"), "french")
        action = payload.action
        update_payload: dict[str, Any]
        order_status: str
        confirmation_status: str
        finalized_order: dict[str, Any] | None = None
        metadata_snapshot = dict(session_row.get("structured_snapshot") or {})
        event_type: str

        if action == "confirm":
            metadata_snapshot, finalized_order = self._prepare_confirmed_order_snapshot(
                session_row=session_row,
                snapshot=metadata_snapshot,
                order_row=order_row,
                language=language,
            )
            update_payload = {
                "status": "confirmed",
                "needs_human": False,
                "confirmed_at": datetime.now(UTC),
                "structured_snapshot": metadata_snapshot,
            }
            order_status = "confirmed"
            confirmation_status = "confirmed"
            event_type = "admin_confirmed"
        elif action == "decline":
            update_payload = {"status": "declined", "needs_human": True, "declined_at": datetime.now(UTC)}
            order_status = "cancelled_by_customer"
            confirmation_status = "declined"
            event_type = "admin_declined"
        elif action == "request_edit":
            update_payload = {"status": "edit_requested", "needs_human": True}
            order_status = "needs_review"
            confirmation_status = "edit_requested"
            event_type = "admin_requested_edit"
        elif action == "request_human":
            update_payload = {"status": "human_requested", "needs_human": True}
            order_status = "needs_review"
            confirmation_status = "human_requested"
            event_type = "admin_requested_human"
        elif action == "reopen":
            update_payload = {"status": "awaiting_customer", "needs_human": False}
            order_status = "pending_confirmation"
            confirmation_status = "awaiting_customer"
            event_type = "admin_reopened"
        elif action == "resend":
            connection = await self._get_ready_whatsapp_connection(business_id)
            business_row = await self.business_repository.get_by_id(business_id)
            confirmation_message = self._build_initial_confirmation_message(
                business_name=str(business_row.get("name") or ""),
                order_row=order_row,
                language=language,
            )
            outbound_row = await self._send_text(
                business_id=business_id,
                phone=str(session_row["phone"]),
                text=confirmation_message,
                connection=connection,
            )
            update_payload = {
                "status": "awaiting_customer",
                "needs_human": False,
                "last_outbound_message_sid": outbound_row.get("provider_message_sid"),
            }
            order_status = "pending_confirmation"
            confirmation_status = "awaiting_customer"
            event_type = "admin_resent_confirmation"
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unsupported confirmation action: {action}",
            )

        session_row = await self.order_confirmation_repository.update_session(
            session_id, update_payload
        )
        order_row = await self.order_repository.update_order_status(
            business_id=business_id,
            order_id=int(order_row["id"]),
            status_value=order_status,
            confirmation_status=confirmation_status,
            metadata=self._build_order_metadata(
                order_row=order_row,
                snapshot=metadata_snapshot,
                confirmation_status=confirmation_status,
            ),
            finalized_order=finalized_order,
        )
        await self.order_confirmation_repository.add_event(
            business_id=business_id,
            session_id=session_id,
            order_id=int(order_row["id"]),
            event_type=event_type,
            payload={"note": payload.note, "finalized_order": finalized_order},
        )
        return {
            **session_row,
            "order": order_row,
            "events": await self.order_confirmation_repository.list_events(session_id),
        }

    async def _get_ready_whatsapp_connection(self, business_id: int) -> dict[str, Any]:
        connection = await self.integration_repository.get_connection(business_id, "whatsapp")
        if connection is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="WhatsApp integration is not configured for this business.",
            )
        config = dict(connection.get("config") or {})
        if (
            connection.get("status") != "connected"
            or config.get("provider") != self.messaging_provider.provider_name
            or config.get("onboarding_status") != "connected"
            or not config.get("subaccount_sid")
            or not config.get("sender_sid")
            or not (config.get("whatsapp_number") or config.get("phone_number"))
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="WhatsApp integration is not finalized for order confirmations.",
            )
        return connection

    async def _send_text(
        self,
        *,
        business_id: int,
        phone: str,
        text: str,
        connection: dict[str, Any],
    ) -> dict[str, Any]:
        config = dict(connection.get("config") or {})
        result = await self.messaging_provider.send_text(
            SendMessageCommand(
                business_id=business_id,
                phone=phone,
                text=text,
                config=config,
                subaccount_sid=str(config["subaccount_sid"]),
            )
        )
        row = await self.chat_repository.upsert_message(
            business_id=business_id,
            phone=result.to_phone,
            customer_name=None,
            text=text,
            direction="outbound",
            intent="autre",
            needs_human=False,
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
        return row

    def _build_snapshot(self, business_row: dict[str, Any], order_row: dict[str, Any]) -> dict[str, Any]:
        return {
            "business_name": business_row.get("name"),
            "external_order_id": order_row.get("external_order_id"),
            "customer_name": order_row.get("customer_name"),
            "customer_phone": order_row.get("customer_phone"),
            "preferred_language": order_row.get("preferred_language"),
            "delivery_city": order_row.get("delivery_city"),
            "delivery_address": order_row.get("delivery_address"),
            "total_amount": float(order_row.get("total_amount") or 0),
            "currency": order_row.get("currency") or "MAD",
            "items": list(order_row.get("items") or []),
            "payment_method": order_row.get("payment_method"),
            "order_notes": order_row.get("order_notes"),
        }

    def _prepare_confirmed_order_snapshot(
        self,
        *,
        session_row: dict[str, Any],
        snapshot: dict[str, Any],
        order_row: dict[str, Any],
        language: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        finalized_snapshot = dict(snapshot)
        finalized_snapshot["preferred_language"] = language
        finalized_snapshot["confirmation_status"] = "confirmed"
        finalized_snapshot["awaiting_final_confirmation_after_edits"] = False
        finalized_snapshot["finalized_at"] = to_iso(datetime.now(UTC))
        latest_detected_edits = list(finalized_snapshot.get("latest_detected_edits") or [])
        if latest_detected_edits:
            finalized_snapshot["confirmed_edits"] = latest_detected_edits
        finalized_order = self._build_finalized_order_payload(
            session_row=session_row,
            order_row=order_row,
            snapshot=finalized_snapshot,
            language=language,
        )
        finalized_snapshot["finalized_order"] = finalized_order
        return finalized_snapshot, finalized_order

    def _build_finalized_order_payload(
        self,
        *,
        session_row: dict[str, Any],
        order_row: dict[str, Any],
        snapshot: dict[str, Any],
        language: str,
    ) -> dict[str, Any]:
        computed_total = self._calculate_snapshot_total(snapshot)
        finalized_order = {
            "customer_phone": snapshot.get("customer_phone") or order_row.get("customer_phone"),
            "preferred_language": language,
            "total_amount": computed_total
            if computed_total is not None
            else snapshot.get("total_amount", order_row.get("total_amount")),
            "currency": snapshot.get("currency") or order_row.get("currency") or "MAD",
            "payment_method": snapshot.get("payment_method") or order_row.get("payment_method"),
            "delivery_city": snapshot.get("delivery_city") or order_row.get("delivery_city"),
            "delivery_address": snapshot.get("delivery_address")
            or order_row.get("delivery_address"),
            "order_notes": snapshot.get("order_notes") or order_row.get("order_notes"),
            "items": list(snapshot.get("items") or order_row.get("items") or []),
        }
        return finalized_order

    def _build_order_metadata(
        self,
        *,
        order_row: dict[str, Any],
        snapshot: dict[str, Any],
        confirmation_status: str,
    ) -> dict[str, Any]:
        metadata = dict(order_row.get("metadata") or {})
        if confirmation_status == "confirmed":
            metadata["order_confirmation"] = {
                "final_snapshot_applied": True,
                "finalized_at": snapshot.get("finalized_at") or to_iso(datetime.now(UTC)),
                "confirmed_edits": list(
                    snapshot.get("confirmed_edits")
                    or snapshot.get("latest_detected_edits")
                    or []
                ),
            }
        return metadata

    def _build_initial_confirmation_message(
        self,
        *,
        business_name: str,
        order_row: dict[str, Any],
        language: str,
    ) -> str:
        items_summary = ", ".join(self._item_summary(item) for item in (order_row.get("items") or [])[:3])
        amount = f"{order_row.get('total_amount')} {order_row.get('currency') or 'MAD'}"
        address_bits = [
            str(order_row.get("delivery_city") or "").strip(),
            str(order_row.get("delivery_address") or "").strip(),
        ]
        address = ", ".join(bit for bit in address_bits if bit)
        order_ref = str(order_row.get("external_order_id") or order_row.get("id"))
        customer_name = str(order_row.get("customer_name") or "").strip()
        name_prefix = f" {customer_name}" if customer_name else ""
        items_line = items_summary or "-"
        address_line = address or {
            "english": "as shared on your order",
            "french": "selon les informations de votre commande",
            "darija": "b7al ma t9ayd f talab",
        }[language]
        action_menu = self._build_action_menu(language)
        if language == "english":
            return (
                f"Hello{name_prefix} 👋\n\n"
                f"Thanks for your order from *{business_name}*.\n\n"
                f"🧾 Order: #{order_ref}\n"
                f"📦 Items: {items_line}\n"
                f"💰 Total: {amount}\n"
                f"📍 Delivery: {address_line}\n\n"
                "Please reply with one option:\n"
                f"{action_menu}"
            )
        if language == "darija":
            return (
                f"Salam{name_prefix} 👋\n\n"
                f"Shukran 3la talab dyalk m3a *{business_name}*.\n\n"
                f"🧾 Commande: #{order_ref}\n"
                f"📦 Talab: {items_line}\n"
                f"💰 Total: {amount}\n"
                f"📍 Delivery: {address_line}\n\n"
                "Jawb b wa7ed l option:\n"
                f"{action_menu}"
            )
        return (
            f"Bonjour{name_prefix} 👋\n\n"
            f"Merci pour votre commande chez *{business_name}*.\n\n"
            f"🧾 Commande : #{order_ref}\n"
            f"📦 Articles : {items_line}\n"
            f"💰 Total : {amount}\n"
            f"📍 Livraison : {address_line}\n\n"
            "Répondez avec une option :\n"
            f"{action_menu}"
        )

    def _build_action_menu(self, language: str) -> str:
        if language == "english":
            return (
                "1️⃣ Confirm order\n"
                "2️⃣ Edit details\n"
                "3️⃣ Cancel order\n"
                "4️⃣ Talk to support"
            )
        if language == "darija":
            return (
                "1️⃣ Confirmi commande\n"
                "2️⃣ Bdel chi 7aja\n"
                "3️⃣ Lghi commande\n"
                "4️⃣ Hder m3a support"
            )
        return (
            "1️⃣ Confirmer la commande\n"
            "2️⃣ Modifier les détails\n"
            "3️⃣ Annuler la commande\n"
            "4️⃣ Parler au support"
        )

    def _item_summary(self, item: dict[str, Any]) -> str:
        name = str(item.get("product_name") or "").strip()
        quantity = int(item.get("quantity") or 1)
        variant = str(item.get("variant") or "").strip()
        if variant:
            return f"{name} ({variant}) x{quantity}"
        return f"{name} x{quantity}"

    def _detect_customer_action(self, message: str) -> str | None:
        normalized = message.strip().lower()
        if normalized in {"1", "ok", "okay", "yes", "oui", "confirm", "confirmed", "confirmer", "valider", "wakha", "نعم"}:
            return "confirm"
        if normalized in {"2", "edit", "modifier", "modify", "change", "بدل"}:
            return "request_edit"
        if normalized in {"3", "cancel", "annuler", "annule", "non", "no", "رفض", "لا", "nlghi"}:
            return "decline"
        if normalized in {"4", "agent", "support", "human", "personne", "call me", "n3ayet", "اتصل"}:
            return "request_human"
        return None

    def _should_detect_language_for_message(self, message: str) -> bool:
        normalized = message.strip().lower()
        if not normalized:
            return False
        if normalized in {"1", "2", "3", "4"}:
            return False
        weak_acknowledgements = {
            "ok",
            "okay",
            "yes",
            "oui",
            "no",
            "non",
            "merci",
            "thanks",
            "thank you",
            "wakha",
            "safi",
            "تمام",
            "نعم",
            "لا",
        }
        if normalized in weak_acknowledgements:
            return False
        alpha_chars = sum(1 for char in normalized if char.isalpha())
        word_count = len([word for word in normalized.split() if word])
        if alpha_chars < 5:
            return False
        if word_count <= 1 and alpha_chars <= 6:
            return False
        return True

    def _build_confirmed_reply(self, language: str, order_row: dict[str, Any]) -> str:
        order_ref = str(order_row.get("external_order_id") or order_row.get("id"))
        if language == "english":
            return (
                f"✅ Thank you.\n\n"
                f"Your order *#{order_ref}* is confirmed and will be prepared for the next step."
            )
        if language == "darija":
            return (
                f"✅ Shukran.\n\n"
                f"Commande *#{order_ref}* tconfirmat, w ghadi nwjduha l marhala jaya."
            )
        return (
            f"✅ Merci.\n\n"
            f"Votre commande *#{order_ref}* est confirmée et sera préparée pour la suite."
        )

    def _build_declined_reply(self, language: str) -> str:
        if language == "english":
            return (
                "📝 Understood.\n\n"
                "We have marked this order as declined. Our support team can help if you need anything else."
            )
        if language == "darija":
            return (
                "📝 Wad7.\n\n"
                "Sjlna had commande comme annulée. Ila bghiti chi 7aja khra, support y9der y3awnek."
            )
        return (
            "📝 C'est noté.\n\n"
            "Nous avons marqué cette commande comme annulée. Le support peut vous aider si besoin."
        )

    def _build_edit_reply(self, language: str) -> str:
        if language == "english":
            return (
                "✏️ Sure.\n\n"
                "Please reply with what you want to change, such as address, phone number, quantity, or variant. Our team will review it."
            )
        if language == "darija":
            return (
                "✏️ Wakha.\n\n"
                "Jawbna b dakchi li bghiti tbdel, b7al l'adresse, numéro, quantité, wela variant, w l'équipe dyalna ghadi tراجعو."
            )
        return (
            "✏️ Très bien.\n\n"
            "Répondez avec les éléments à modifier, comme l'adresse, le numéro, la quantité ou la variante, et notre équipe va vérifier."
        )

    def _build_edit_details_reply(self, language: str) -> str:
        if language == "english":
            return "Thanks, we received your requested changes. Our team will review them and get back to you on WhatsApp."
        if language == "darija":
            return "Shukran, tsjlat talab dyal التعديل. L'équipe dyalna ghadi tراجعو w ترجع ليك ف WhatsApp."
        return "Merci, votre demande de modification a bien été reçue. Notre équipe va la vérifier et revenir vers vous sur WhatsApp."

    def _build_edit_interpretation_reply(
        self,
        language: str,
        interpretation: OrderSessionInterpretation,
        snapshot: dict[str, Any],
    ) -> str:
        changes = ", ".join(f"{edit.field}: {edit.value}" for edit in interpretation.edits[:3])
        revised_summary = self._build_snapshot_confirmation_summary(language, snapshot)
        total_note = self._build_total_update_note(language, snapshot, interpretation)
        if language == "english":
            if changes:
                return (
                    "✏️ Understood.\n\n"
                    f"I noted these requested changes: {changes}.\n"
                    f"{revised_summary}\n\n"
                    f"{total_note}\n\n"
                    "Reply 1 to confirm the updated order or send another change."
                )
            return (
                "✏️ Understood.\n\n"
                f"{revised_summary}\n\n"
                "Reply 1 to confirm the updated order or send another change."
            )
        if language == "darija":
            if changes:
                return (
                    "✏️ Wad7.\n\n"
                    f"Tsjlo had talabat dyal التعديل: {changes}.\n"
                    f"{revised_summary}\n\n"
                    f"{total_note}\n\n"
                    "Jawb b 1 bach tconfirmi l commande b ta3dilat jdod, wela sift taghyir akhor."
                )
            return (
                "✏️ Wad7.\n\n"
                f"{revised_summary}\n\n"
                "Jawb b 1 bach tconfirmi l commande b ta3dilat jdod, wela sift taghyir akhor."
            )
        if changes:
            return (
                "✏️ C'est noté.\n\n"
                f"J'ai enregistré ces changements demandés : {changes}.\n"
                f"{revised_summary}\n\n"
                f"{total_note}\n\n"
                "Répondez 1 pour confirmer la commande mise à jour ou envoyez un autre changement."
            )
        return (
            "✏️ C'est noté.\n\n"
            f"{revised_summary}\n\n"
            "Répondez 1 pour confirmer la commande mise à jour ou envoyez un autre changement."
        )

    def _apply_interpreted_edits_to_snapshot(
        self,
        *,
        snapshot: dict[str, Any],
        order_row: dict[str, Any],
        interpretation: OrderSessionInterpretation,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        updated_snapshot = dict(snapshot)
        items = [dict(item) for item in (updated_snapshot.get("items") or order_row.get("items") or [])]
        updated_snapshot["items"] = items
        pending_edits = list(updated_snapshot.get("pending_edits") or [])
        applied: list[dict[str, Any]] = []
        ambiguous: list[dict[str, Any]] = []
        multi_item = len(items) > 1

        for edit in interpretation.edits:
            normalized_edit = {
                "field": edit.field,
                "value": edit.value,
                "received_at": to_iso(datetime.now(UTC)),
            }
            pending_edits.append(normalized_edit)
            applied_successfully = False

            if edit.field == "delivery_city":
                updated_snapshot["delivery_city"] = edit.value
                applied_successfully = True
            elif edit.field == "delivery_address":
                updated_snapshot["delivery_address"] = edit.value
                applied_successfully = True
            elif edit.field == "customer_phone":
                updated_snapshot["customer_phone"] = edit.value
                applied_successfully = True
            elif items and not multi_item and edit.field in {"variant", "quantity", "product_name"}:
                target_item = items[0]
                if edit.field == "quantity":
                    try:
                        target_item["quantity"] = max(1, int(str(edit.value).strip()))
                        applied_successfully = True
                    except ValueError:
                        applied_successfully = False
                elif edit.field == "variant":
                    target_item["variant"] = edit.value
                    applied_successfully = True
                elif edit.field == "product_name":
                    target_item["product_name"] = edit.value
                    applied_successfully = True
            elif edit.field in {"variant", "quantity", "product_name"} and multi_item:
                applied_successfully = False

            if applied_successfully:
                applied.append(normalized_edit)
            else:
                ambiguous.append(normalized_edit)

        updated_snapshot["pending_edits"] = pending_edits
        return applied, ambiguous, updated_snapshot

    def _build_snapshot_confirmation_summary(self, language: str, snapshot: dict[str, Any]) -> str:
        items = snapshot.get("items") or []
        items_summary = ", ".join(self._item_summary(item) for item in items[:3]) or "-"
        amount = self._calculate_snapshot_total(snapshot)
        if amount is None:
            amount_line = f"{snapshot.get('total_amount', 0)} {snapshot.get('currency') or 'MAD'}"
        else:
            amount_line = f"{amount} {snapshot.get('currency') or 'MAD'}"
        address_bits = [
            str(snapshot.get("delivery_city") or "").strip(),
            str(snapshot.get("delivery_address") or "").strip(),
        ]
        address = ", ".join(bit for bit in address_bits if bit) or "-"
        if language == "english":
            return (
                "Updated order summary:\n"
                f"📦 Items: {items_summary}\n"
                f"💰 Total: {amount_line}\n"
                f"📍 Delivery: {address}"
            )
        if language == "darija":
            return (
                "Hadchi howa l update dyal commande:\n"
                f"📦 Talab: {items_summary}\n"
                f"💰 Total: {amount_line}\n"
                f"📍 Delivery: {address}"
            )
        return (
            "Résumé mis à jour de la commande :\n"
            f"📦 Articles : {items_summary}\n"
            f"💰 Total : {amount_line}\n"
            f"📍 Livraison : {address}"
        )

    def _calculate_snapshot_total(self, snapshot: dict[str, Any]) -> float | None:
        items = snapshot.get("items") or []
        computed_total = 0.0
        has_price = False
        for item in items:
            unit_price = item.get("unit_price")
            if unit_price is None:
                continue
            try:
                computed_total += float(unit_price) * int(item.get("quantity") or 1)
                has_price = True
            except (TypeError, ValueError):
                return None
        if has_price:
            return round(computed_total, 2)
        return None

    def _build_total_update_note(
        self,
        language: str,
        snapshot: dict[str, Any],
        interpretation: OrderSessionInterpretation,
    ) -> str:
        quantity_changed = any(edit.field == "quantity" for edit in interpretation.edits)
        recalculated = self._calculate_snapshot_total(snapshot)
        if quantity_changed and recalculated is None:
            if language == "english":
                return "We noted the updated quantity. The final total will be confirmed with the order."
            if language == "darija":
                return "Sjlna l quantité jdida. Total النهائي ghadi يتأكد m3a l commande."
            return "La quantité mise à jour a été enregistrée. Le total final sera confirmé avec la commande."
        return {
            "english": "If everything looks right, confirm the updated order below.",
            "darija": "Ila kolchi mzyan, confirmi l commande b ta3dilat jdod.",
            "french": "Si tout est correct, confirmez la commande mise à jour ci-dessous.",
        }[language]

    def _build_delivery_question_reply(
        self, language: str, order_row: dict[str, Any], snapshot: dict[str, Any]
    ) -> str:
        city = str(order_row.get("delivery_city") or snapshot.get("delivery_city") or "").strip()
        address = str(order_row.get("delivery_address") or snapshot.get("delivery_address") or "").strip()
        if language == "english":
            return (
                "📍 Delivery details\n\n"
                f"Current address: {city}, {address}\n\n"
                "If this is correct, reply 1 to confirm.\n"
                "If you want to change it, send the new details."
            ).strip()
        if language == "darija":
            return (
                "📍 Delivery details\n\n"
                f"Les infos li 3andna daba: {city}, {address}\n\n"
                "Ila صحاح jawb b 1 bach tconfirmi.\n"
                "Ila bghiti tbdelhom, sift l details jdod."
            ).strip()
        return (
            "📍 Détails de livraison\n\n"
            f"Adresse actuelle : {city}, {address}\n\n"
            "Si c'est correct, répondez 1 pour confirmer.\n"
            "Sinon, envoyez les nouvelles informations."
        ).strip()

    def _build_payment_question_reply(self, language: str, order_row: dict[str, Any]) -> str:
        payment_method = str(order_row.get("payment_method") or "cash_on_delivery").replace("_", " ")
        if language == "english":
            return (
                "💳 Payment details\n\n"
                f"The payment method on this order is {payment_method}.\n"
                "If you want a change, reply with the new request and our team will review it."
            )
        if language == "darija":
            return (
                "💳 Paiement\n\n"
                f"Tariqat l paiement f had commande hiya {payment_method}.\n"
                "Ila bghiti tbdelha, sift talab dyalk w l'équipe ghadi tراجعو."
            )
        return (
            "💳 Paiement\n\n"
            f"Le mode de paiement actuel pour cette commande est {payment_method}.\n"
            "Si vous voulez le modifier, envoyez votre demande et notre équipe va la vérifier."
        )

    def _build_return_policy_question_reply(self, language: str) -> str:
        if language == "english":
            return (
                "↩️ Return policy\n\n"
                "For return-policy questions related to this order, our support team will guide you on WhatsApp."
            )
        if language == "darija":
            return (
                "↩️ Return policy\n\n"
                "Bnisba l politique dyal l return f had commande, support dyalna ghadi yشرحها ليك f WhatsApp."
            )
        return (
            "↩️ Politique de retour\n\n"
            "Pour les questions de retour liées à cette commande, notre équipe support va vous guider sur WhatsApp."
        )

    def _build_human_reply(self, language: str) -> str:
        if language == "english":
            return (
                "🤝 Understood.\n\n"
                "We are handing this order to a human agent who will continue with you on WhatsApp."
            )
        if language == "darija":
            return (
                "🤝 Wad7.\n\n"
                "Ghadi n7awlo had commande l agent bach ykمل m3ak f WhatsApp."
            )
        return (
            "🤝 Bien reçu.\n\n"
            "Nous passons cette commande à un agent humain qui poursuivra avec vous sur WhatsApp."
        )

    def _build_fallback_reply(self, language: str) -> str:
        if language == "english":
            return (
                "🤝 I did not fully understand your reply.\n\n"
                "A support agent will continue with you on WhatsApp."
            )
        if language == "darija":
            return (
                "🤝 Ma fhemtch mzyan jawab dyalk.\n\n"
                "Support ghadi ykمل m3ak f WhatsApp."
            )
        return (
            "🤝 Je n'ai pas bien compris votre réponse.\n\n"
            "Un agent support va poursuivre avec vous sur WhatsApp."
        )
