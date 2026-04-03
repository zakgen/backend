from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.order_confirmation import (
    OrderConfirmationAction,
    OrderConfirmationActionRequest,
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
        try:
            language_hint, _ = await self.llm_provider.detect_language(message=message_text)
        except Exception:
            language_hint = str(session_row.get("preferred_language") or "")
        language = normalize_language_label(
            language_hint or session_row.get("preferred_language"),
            fallback=normalize_language_label(session_row.get("preferred_language"), "french"),
        )
        action = self._detect_customer_action(message_text)
        order_row = await self.order_repository.get_by_id(
            business_id, int(session_row["order_id"])
        )
        snapshot = dict(session_row.get("structured_snapshot") or {})
        session_update: dict[str, Any] = {
            "preferred_language": language,
            "last_customer_message_at": datetime.now(UTC),
            "last_detected_intent": action or "free_text",
        }
        order_status = order_row.get("status") or "pending_confirmation"
        confirmation_status = order_row.get("confirmation_status") or session_row["status"]
        outbound_text: str
        event_type: str
        needs_human = False

        if action == "confirm":
            session_update.update({"status": "confirmed", "confirmed_at": datetime.now(UTC), "needs_human": False})
            order_status = "confirmed"
            confirmation_status = "confirmed"
            event_type = "customer_confirmed"
            outbound_text = self._build_confirmed_reply(language, order_row)
        elif action == "decline":
            session_update.update({"status": "declined", "declined_at": datetime.now(UTC), "needs_human": True})
            order_status = "cancelled_by_customer"
            confirmation_status = "declined"
            event_type = "customer_declined"
            outbound_text = self._build_declined_reply(language)
            needs_human = True
        elif action == "request_edit":
            session_update.update({"status": "edit_requested", "needs_human": True})
            order_status = "needs_review"
            confirmation_status = "edit_requested"
            event_type = "customer_requested_edit"
            outbound_text = self._build_edit_reply(language)
            needs_human = True
        elif action == "request_human":
            session_update.update({"status": "human_requested", "needs_human": True})
            order_status = "needs_review"
            confirmation_status = "human_requested"
            event_type = "customer_requested_human"
            outbound_text = self._build_human_reply(language)
            needs_human = True
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
        else:
            session_update.update({"status": "human_requested", "needs_human": True})
            order_status = "needs_review"
            confirmation_status = "human_requested"
            event_type = "customer_unrecognized_reply"
            outbound_text = self._build_fallback_reply(language)
            needs_human = True

        session_row = await self.order_confirmation_repository.update_session(
            int(session_row["id"]),
            session_update,
        )
        order_row = await self.order_repository.update_order_status(
            business_id=business_id,
            order_id=int(order_row["id"]),
            status_value=order_status,
            confirmation_status=confirmation_status,
            metadata=dict(order_row.get("metadata") or {}),
        )
        await self.order_confirmation_repository.add_event(
            business_id=business_id,
            session_id=int(session_row["id"]),
            order_id=int(order_row["id"]),
            event_type=event_type,
            payload={"message": message_text, "action": action},
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
        event_type: str

        if action == "confirm":
            update_payload = {"status": "confirmed", "needs_human": False, "confirmed_at": datetime.now(UTC)}
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
            metadata=dict(order_row.get("metadata") or {}),
        )
        await self.order_confirmation_repository.add_event(
            business_id=business_id,
            session_id=session_id,
            order_id=int(order_row["id"]),
            event_type=event_type,
            payload={"note": payload.note},
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
        if language == "english":
            return (
                f"Hello{name_prefix}, thanks for your order from {business_name}. "
                f"Order #{order_ref}: {items_summary}. Total: {amount}. "
                f"Delivery: {address or 'as shared on your order'}. "
                "Reply 1 to confirm, 2 to edit your details, 3 to cancel, or 4 to talk to support."
            )
        if language == "darija":
            return (
                f"Salam{name_prefix}, shukran 3la talab dyalk m3a {business_name}. "
                f"Commande #{order_ref}: {items_summary}. Total: {amount}. "
                f"Delivery: {address or 'b7al ma t9ayd f talab'}. "
                "Jawb b 1 bach tconfirmi, 2 ila bghiti tbdel chi 7aja, 3 ila bghiti tlghi, w 4 ila bghiti support."
            )
        return (
            f"Bonjour{name_prefix}, merci pour votre commande chez {business_name}. "
            f"Commande #{order_ref}: {items_summary}. Total: {amount}. "
            f"Livraison: {address or 'selon les informations de votre commande'}. "
            "Répondez 1 pour confirmer, 2 pour modifier, 3 pour annuler ou 4 pour parler au support."
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

        if any(token in normalized for token in ("confirm", "oui", "yes", "valide", "wakha", "موافق")):
            return "confirm"
        if any(token in normalized for token in ("cancel", "annul", "decline", "nlghi", "نلغي", "رفض")):
            return "decline"
        if any(token in normalized for token in ("edit", "change", "modifier", "بدل", "adresse", "address", "quantity", "taille", "color")):
            return "request_edit"
        if any(token in normalized for token in ("agent", "support", "human", "call", "phone", "whatsapp", "n3ayet", "اتصل")):
            return "request_human"
        return None

    def _build_confirmed_reply(self, language: str, order_row: dict[str, Any]) -> str:
        order_ref = str(order_row.get("external_order_id") or order_row.get("id"))
        if language == "english":
            return f"Thank you. Your order #{order_ref} is confirmed and will be prepared for the next step."
        if language == "darija":
            return f"Shukran. Commande #{order_ref} tconfirmat, w ghadi nwjduha l marhala jaya."
        return f"Merci. Votre commande #{order_ref} est confirmée et sera préparée pour la suite."

    def _build_declined_reply(self, language: str) -> str:
        if language == "english":
            return "Understood. We have marked this order as declined. Our support team can help if you need anything else."
        if language == "darija":
            return "Wad7. Sjlna had commande comme annulée. Ila bghiti chi 7aja khra, support يقدر يعاونك."
        return "C'est noté. Nous avons marqué cette commande comme annulée. Le support peut vous aider si besoin."

    def _build_edit_reply(self, language: str) -> str:
        if language == "english":
            return "Please reply with the details you want to change, such as address, phone number, quantity, or variant. Our team will review it."
        if language == "darija":
            return "Jawbna b dakchi li bghiti tbdel, b7al l'adresse, numéro, quantité, ولا variant, w l'équipe dyalna ghadi tراجعو."
        return "Répondez avec les éléments à modifier, comme l'adresse, le numéro, la quantité ou la variante, et notre équipe va vérifier."

    def _build_edit_details_reply(self, language: str) -> str:
        if language == "english":
            return "Thanks, we received your requested changes. Our team will review them and get back to you on WhatsApp."
        if language == "darija":
            return "Shukran, tsjlat talab dyal التعديل. L'équipe dyalna ghadi tراجعو w ترجع ليك ف WhatsApp."
        return "Merci, votre demande de modification a bien été reçue. Notre équipe va la vérifier et revenir vers vous sur WhatsApp."

    def _build_human_reply(self, language: str) -> str:
        if language == "english":
            return "Understood. We are handing this order to a human agent who will continue with you on WhatsApp."
        if language == "darija":
            return "Wad7. Ghadi نحولو had commande l agent bach يكمل m3ak f WhatsApp."
        return "Bien reçu. Nous passons cette commande à un agent humain qui poursuivra avec vous sur WhatsApp."

    def _build_fallback_reply(self, language: str) -> str:
        if language == "english":
            return "I did not fully understand your reply for this order confirmation. A support agent will continue with you on WhatsApp."
        if language == "darija":
            return "Ma fhemtch mzyan jawab dyalk bnisba l confirmation dyal had commande. Support ghadi يكمل m3ak f WhatsApp."
        return "Je n'ai pas bien compris votre réponse pour cette confirmation de commande. Un agent support va poursuivre avec vous sur WhatsApp."
