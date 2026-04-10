from __future__ import annotations

from collections.abc import Mapping
import json
import logging
from typing import Any
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.schemas.ai import (
    AIModelReply,
    AIReplyHistoryMessage,
    AIReplyRequest,
    AIReplyResponse,
    AIRunDetail,
    AIRunSummary,
    AISourceReference,
)
from app.schemas.conversation import ConversationMessage
from app.services.ai_helpers import (
    infer_intent_hint,
    is_order_management_request,
    normalize_language_label,
    source_preference,
)
from app.services.ai_prompt_builder import PROMPT_VERSION, build_ai_reply_prompts
from app.services.dashboard_service import business_row_to_profile, chat_row_to_message, to_iso
from app.services.embedding_service import EmbeddingService
from app.services.llm_provider import AbstractLLMProvider
from app.services.openai_llm_provider import OpenAILLMProvider
from app.services.reply_validation_service import ReplyValidationService
from app.services.repository_factory import RepositoryFactory
from app.services.search_formatting import (
    format_business_match,
    format_faq_match,
    format_product_match,
)
from app.services.messaging_provider import AbstractMessagingProvider
from app.services.messaging_types import SendMessageCommand


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("app.ai_reply_audit")
BUSINESS_FACT_IDS = {
    "city": "city",
    "store_address": "store_address",
    "support_phone": "support_phone",
    "whatsapp_number": "whatsapp_number",
    "support_email": "support_email",
    "delivery_zones": "delivery_zones",
    "delivery_zone_details": "delivery_zone_details",
    "delivery_time": "delivery_time",
    "delivery_tracking_method": "delivery_tracking_method",
    "shipping_policy": "shipping_policy",
    "return_policy": "return_policy",
    "return_window_days": "return_window_days",
    "return_conditions": "return_conditions",
    "payment_methods": "payment_methods",
    "opening_hours": "opening_hours",
    "supported_languages": "supported_languages",
    "order_rules": "order_rules",
    "upsell_rules": "upsell_rules",
    "escalation_contact": "escalation_contact",
    "summary": "summary",
    "niche": "niche",
}


def build_llm_provider(settings: Settings | None = None) -> AbstractLLMProvider:
    resolved = settings or get_settings()
    if resolved.llm_provider == "openai":
        return OpenAILLMProvider(resolved)
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=f"Unsupported LLM provider: {resolved.llm_provider}",
    )


class AIReplyService:
    def __init__(
        self,
        *,
        session: AsyncSession,
        llm_provider: AbstractLLMProvider | None = None,
        embedding_service: EmbeddingService | None = None,
        messaging_provider: AbstractMessagingProvider | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.llm_provider = llm_provider or build_llm_provider(self.settings)
        self.embedding_service = embedding_service or EmbeddingService(self.settings)
        self.messaging_provider = messaging_provider
        self.validation_service = ReplyValidationService(self.settings)
        factory = RepositoryFactory(session, self.settings)
        self.business_repository = factory.business()
        self.product_repository = factory.products()
        self.faq_repository = factory.faqs()
        self.chat_repository = factory.chats()
        self.integration_repository = factory.integrations()
        self.ai_run_repository = factory.ai_runs()

    async def generate_preview(
        self, business_id: int, payload: AIReplyRequest
    ) -> AIReplyResponse:
        result = await self._execute_reply_flow(
            business_id=business_id,
            customer_message=payload.message,
            phone=payload.phone,
            recent_messages_override=payload.recent_messages,
            inbound_chat_message_id=None,
            connection=None,
            auto_send=False,
        )
        return self._result_to_response(result)

    async def list_runs(self, business_id: int, limit: int = 50) -> list[AIRunSummary]:
        await self.business_repository.get_by_id(business_id)
        return [self._row_to_run_summary(row) for row in await self.ai_run_repository.list_runs(business_id, limit)]

    async def get_run(self, business_id: int, run_id: int) -> AIRunDetail:
        await self.business_repository.get_by_id(business_id)
        return self._row_to_run_detail(await self.ai_run_repository.get_run(business_id, run_id))

    async def process_inbound_message(
        self,
        *,
        connection: dict[str, Any],
        inbound_row: dict[str, Any],
    ) -> AIReplyResponse | None:
        config = dict(connection.get("config") or {})
        if (
            connection.get("status") != "connected"
            or config.get("onboarding_status") != "connected"
            or not self._is_auto_reply_enabled(config)
        ):
            logger.info(
                "AI auto-reply skipped for business %s message %s: status=%s onboarding_status=%s auto_reply_enabled=%s mode=%s",
                connection.get("business_id"),
                inbound_row.get("id"),
                connection.get("status"),
                config.get("onboarding_status"),
                config.get("ai_auto_reply_enabled", self.settings.ai_auto_reply_enabled_default),
                config.get("ai_reply_mode"),
            )
            return None

        logger.info(
            "AI auto-reply started for business %s message %s phone=%s",
            connection.get("business_id"),
            inbound_row.get("id"),
            inbound_row.get("phone"),
        )
        result = await self._execute_reply_flow(
            business_id=int(connection["business_id"]),
            customer_message=str(inbound_row.get("text") or ""),
            phone=str(inbound_row.get("phone") or ""),
            recent_messages_override=None,
            inbound_chat_message_id=int(inbound_row["id"]),
            connection=connection,
            auto_send=True,
        )
        return self._result_to_response(result)

    def _is_auto_reply_enabled(self, config: dict[str, Any]) -> bool:
        if config.get("ai_reply_mode") in {"paused", "human_only"}:
            return False
        enabled = config.get("ai_auto_reply_enabled")
        if enabled is None:
            return self.settings.ai_auto_reply_enabled_default
        return bool(enabled)

    async def _execute_reply_flow(
        self,
        *,
        business_id: int,
        customer_message: str,
        phone: str | None,
        recent_messages_override: list[AIReplyHistoryMessage] | None,
        inbound_chat_message_id: int | None,
        connection: dict[str, Any] | None,
        auto_send: bool,
    ) -> dict[str, Any]:
        intent_hint = infer_intent_hint(customer_message)
        language_hint, language_detection_payload = await self.llm_provider.detect_language(
            message=customer_message
        )
        retrieval_summary: dict[str, Any] = {
            "intent_hint": intent_hint,
            "language_hint": language_hint,
            "language_detection": language_detection_payload,
            "selected_sources": [],
        }
        request_payload: dict[str, Any] = {}
        response_payload: dict[str, Any] = {}
        business_profile = None
        selected_context_items: list[dict[str, Any]] = []
        recent_messages: list[dict[str, str]] = []

        try:
            business_profile = await self._load_business_profile(business_id)
            recent_messages = await self._load_recent_messages(
                business_id=business_id,
                phone=phone,
                recent_messages_override=recent_messages_override,
            )
            rule_based = self._maybe_rule_based_reply(
                customer_message=customer_message,
                business_profile=business_profile,
                language_hint=language_hint,
                intent_hint=intent_hint,
            )
            if rule_based is not None:
                reply, selected_context_items, request_payload, response_payload = rule_based
            else:
                selected_context_items = await self._select_context(
                    business_id=business_id,
                    customer_message=customer_message,
                    intent_hint=intent_hint,
                    business_profile=business_profile,
                )
                system_prompt, user_prompt = build_ai_reply_prompts(
                    business_profile=business_profile,
                    customer_message=customer_message,
                    recent_messages=recent_messages,
                    selected_sources=selected_context_items,
                    language_hint=language_hint,
                    intent_hint=intent_hint,
                )
                request_payload = {
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                    "prompt_version": PROMPT_VERSION,
                }
                logger.info(
                    "AI retrieval assembled for business %s phone=%s intent_hint=%s language_hint=%s selected_sources=%d history_messages=%d",
                    business_id,
                    phone,
                    intent_hint,
                    language_hint,
                    len(selected_context_items),
                    len(recent_messages),
                )
                reply, response_payload = await self.llm_provider.generate_structured_reply(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                )
            retrieval_summary["selected_sources"] = [
                self._source_to_summary(item) for item in selected_context_items
            ]
            retrieval_summary["history_messages"] = recent_messages
        except Exception as exc:
            logger.exception(
                "AI reply generation failed for business %s phone=%s",
                business_id,
                phone,
                exc_info=exc,
            )
            if inbound_chat_message_id is not None:
                await self.chat_repository.update_message_analysis(
                    inbound_chat_message_id,
                    intent=intent_hint,
                    needs_human=True,
                )
            run_row = await self.ai_run_repository.create_run(
                business_id=business_id,
                phone=phone,
                inbound_chat_message_id=inbound_chat_message_id,
                outbound_chat_message_id=None,
                provider=self.llm_provider.provider_name,
                model=self.llm_provider.model_name,
                status_value="failed",
                customer_message=customer_message,
                language=language_hint,
                intent=intent_hint,
                needs_human=True,
                confidence=0.0,
                reply_text=None,
                fallback_reason=str(exc),
                retrieval_summary=retrieval_summary,
                prompt_version=PROMPT_VERSION,
                request_payload=request_payload,
                response_payload={"error": str(exc), **response_payload},
            )
            result = {
                "run": run_row,
                "reply": AIModelReply(
                    reply_text=None,
                    intent=intent_hint,
                    language=language_hint,
                    grounded=False,
                    needs_human=True,
                    confidence=0.0,
                    reason_code="generation_failed",
                ),
                "decision": "failed",
                "retrieved_sources": retrieval_summary["selected_sources"],
                "outbound_message": None,
                "sent": False,
            }
            self._write_audit_log(
                run_row=run_row,
                customer_message=customer_message,
                request_payload=request_payload,
                retrieval_summary=retrieval_summary,
                response_payload={"error": str(exc), **response_payload},
                decision="failed",
                fallback_reason=str(exc),
                sent=False,
                outbound_row=None,
            )
            return result

        if reply.intent is None:
            reply.intent = intent_hint
        reply.language = normalize_language_label(reply.language, fallback=language_hint)

        validated_reply, decision, fallback_reason = self.validation_service.validate(
            reply,
            available_sources=selected_context_items,
        )
        logger.info(
            "AI reply decision for business %s phone=%s inbound_message_id=%s decision=%s reason=%s intent=%s confidence=%.3f grounded=%s used_sources=%d",
            business_id,
            phone,
            inbound_chat_message_id,
            decision,
            fallback_reason,
            validated_reply.intent,
            validated_reply.confidence,
            validated_reply.grounded,
            len(validated_reply.used_sources),
        )

        if inbound_chat_message_id is not None:
            await self.chat_repository.update_message_analysis(
                inbound_chat_message_id,
                intent=validated_reply.intent,
                needs_human=decision != "send",
            )

        status_value = "generated" if decision == "send" else "escalated" if decision == "needs_human" else "failed"
        run_row = await self.ai_run_repository.create_run(
            business_id=business_id,
            phone=phone,
            inbound_chat_message_id=inbound_chat_message_id,
            outbound_chat_message_id=None,
            provider=self.llm_provider.provider_name,
            model=self.llm_provider.model_name,
            status_value=status_value,
            customer_message=customer_message,
            language=validated_reply.language,
            intent=validated_reply.intent,
            needs_human=decision != "send",
            confidence=validated_reply.confidence,
            reply_text=validated_reply.reply_text,
            fallback_reason=fallback_reason,
            retrieval_summary=retrieval_summary,
            prompt_version=PROMPT_VERSION,
            request_payload=request_payload,
            response_payload=response_payload,
        )

        outbound_row = None
        sent = False
        if auto_send and decision == "send" and connection is not None and phone and self.messaging_provider is not None:
            try:
                async with self.session.begin_nested():
                    outbound_row = await self._send_generated_reply(
                        business_id=business_id,
                        phone=phone,
                        reply=validated_reply,
                        connection=connection,
                    )
                    if outbound_row is None:
                        logger.info(
                            "AI reply skipped outside 24h window business_id=%s phone=%s inbound_message_id=%s",
                            business_id,
                            phone,
                            inbound_chat_message_id,
                        )
                        run_row = await self.ai_run_repository.update_run(
                            int(run_row["id"]),
                            status_value="skipped",
                        )
                    else:
                        logger.info(
                            "AI reply sent for business %s phone=%s inbound_message_id=%s outbound_message_id=%s",
                            business_id,
                            phone,
                            inbound_chat_message_id,
                            outbound_row["id"],
                        )
                        run_row = await self.ai_run_repository.update_run(
                            int(run_row["id"]),
                            status_value="sent",
                            outbound_chat_message_id=int(outbound_row["id"]),
                        )
                        sent = True
            except Exception as exc:
                logger.exception(
                    "AI reply send failed for business %s phone=%s inbound_message_id=%s",
                    business_id,
                    phone,
                    inbound_chat_message_id,
                    exc_info=exc,
                )
                response_payload = {**response_payload, "send_error": str(exc)}
                run_row = await self.ai_run_repository.update_run(
                    int(run_row["id"]),
                    status_value="failed",
                    fallback_reason=f"send_failed: {exc}",
                    response_payload=response_payload,
                )
                if inbound_chat_message_id is not None:
                    await self.chat_repository.update_message_analysis(
                        inbound_chat_message_id,
                        intent=validated_reply.intent,
                        needs_human=True,
                    )
                decision = "failed"
                validated_reply.needs_human = True
                fallback_reason = f"send_failed: {exc}"

        result = {
            "run": run_row,
            "reply": validated_reply,
            "decision": decision,
            "retrieved_sources": retrieval_summary["selected_sources"],
            "outbound_message": outbound_row,
            "sent": sent,
        }
        self._write_audit_log(
            run_row=run_row,
            customer_message=customer_message,
            request_payload=request_payload,
            retrieval_summary=retrieval_summary,
            response_payload=response_payload,
            decision=decision,
            fallback_reason=fallback_reason,
            sent=sent,
            outbound_row=outbound_row,
        )
        return result

    async def _load_business_profile(self, business_id: int):
        business_row = await self.business_repository.get_by_id(business_id)
        faq_rows = await self.faq_repository.list_by_business(business_id)
        return business_row_to_profile(business_row, faq_rows)

    async def _load_recent_messages(
        self,
        *,
        business_id: int,
        phone: str | None,
        recent_messages_override: list[AIReplyHistoryMessage] | None,
    ) -> list[dict[str, str]]:
        if recent_messages_override is not None:
            messages = recent_messages_override[-self.settings.ai_reply_max_history_messages :]
            return [
                {"direction": item.direction, "text": item.text}
                for item in messages
            ]

        if not phone:
            return []

        thread = await self.chat_repository.get_thread(business_id, phone)
        recent_rows = thread[-self.settings.ai_reply_max_history_messages :]
        return [
            {"direction": str(row.get("direction") or "inbound"), "text": str(row.get("text") or "")}
            for row in recent_rows
            if row.get("text")
        ]

    def _maybe_rule_based_reply(
        self,
        *,
        customer_message: str,
        business_profile,
        language_hint: str,
        intent_hint: str,
    ) -> tuple[AIModelReply, list[dict[str, Any]], dict[str, Any], dict[str, Any]] | None:
        normalized_language = normalize_language_label(language_hint, fallback="english")
        if is_order_management_request(customer_message):
            return self._build_order_handoff_reply(business_profile, normalized_language)

        if (
            intent_hint == "infos_boutique"
            and business_profile.opening_hours
            and self._asks_for_opening_hours(customer_message)
        ):
            return self._build_opening_hours_reply(
                business_profile,
                normalized_language,
                customer_message,
            )

        if (
            intent_hint == "infos_boutique"
            and (business_profile.store_address or business_profile.support_phone or business_profile.whatsapp_number)
            and self._asks_for_contact_or_location(customer_message)
        ):
            return self._build_contact_reply(
                business_profile,
                normalized_language,
                customer_message,
            )

        if intent_hint == "retour" and (business_profile.return_policy or business_profile.return_window_days):
            return self._build_return_policy_reply(business_profile, normalized_language)

        if intent_hint == "livraison":
            zone = self._find_delivery_zone(business_profile, customer_message)
            if zone is not None:
                return self._build_delivery_reply(
                    business_profile,
                    normalized_language,
                    zone,
                    include_tracking=self._asks_for_tracking(customer_message),
                )

        return None

    async def _select_context(
        self,
        *,
        business_id: int,
        customer_message: str,
        intent_hint: str,
        business_profile,
    ) -> list[dict[str, Any]]:
        query_embedding = await self.embedding_service.embed_text(customer_message)
        raw_limit = max(self.settings.ai_reply_max_context_items * 2, self.settings.ai_reply_max_context_items)

        product_rows = await self.product_repository.search(business_id, query_embedding, raw_limit)
        faq_rows = await self.faq_repository.search(business_id, query_embedding, raw_limit)
        knowledge_rows = await self.business_repository.search_knowledge(
            business_id, query_embedding, raw_limit
        )

        grouped = {
            "business_fact": self._business_fact_context(business_profile, intent_hint),
            "product": [self._match_to_context(format_product_match(row), row) for row in product_rows if float(row["score"]) >= self.settings.search_min_score],
            "faq": [self._match_to_context(format_faq_match(row), row) for row in faq_rows if float(row["score"]) >= self.settings.search_min_score],
            "business_knowledge": [self._match_to_context(format_business_match(row), row) for row in knowledge_rows if float(row["score"]) >= self.settings.search_min_score],
        }

        selected: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for source_type in source_preference(intent_hint):
            for item in grouped[source_type]:
                key = (str(item["type"]), str(item["id"]))
                if key in seen:
                    continue
                selected.append(item)
                seen.add(key)
                if len(selected) >= self.settings.ai_reply_max_context_items:
                    return selected

        for item in grouped["business_fact"]:
            key = (str(item["type"]), str(item["id"]))
            if key in seen:
                continue
            selected.append(item)
            seen.add(key)
            if len(selected) >= self.settings.ai_reply_max_context_items:
                return selected

        for source_type in ("product", "faq", "business_knowledge"):
            for item in grouped[source_type]:
                key = (str(item["type"]), str(item["id"]))
                if key in seen:
                    continue
                selected.append(item)
                seen.add(key)
                if len(selected) >= self.settings.ai_reply_max_context_items:
                    return selected

        return selected

    def _business_fact_context(self, business_profile, intent_hint: str) -> list[dict[str, Any]]:
        candidate_keys = {
            "livraison": (
                "delivery_zones",
                "delivery_zone_details",
                "delivery_time",
                "delivery_tracking_method",
                "shipping_policy",
                "payment_methods",
                "city",
            ),
            "paiement": ("payment_methods", "support_phone", "whatsapp_number"),
            "retour": (
                "return_policy",
                "return_window_days",
                "return_conditions",
                "support_phone",
                "whatsapp_number",
            ),
            "infos_produit": ("summary", "niche"),
            "infos_boutique": (
                "opening_hours",
                "store_address",
                "support_phone",
                "whatsapp_number",
                "support_email",
                "city",
                "summary",
            ),
            "autre": ("summary", "niche", "support_phone", "whatsapp_number"),
        }.get(
            intent_hint,
            ("summary", "niche"),
        )

        items: list[dict[str, Any]] = []
        for key in candidate_keys:
            item = self._business_fact_item(business_profile, key)
            if item is not None:
                items.append(item)
        return items

    def _business_fact_item(self, business_profile, key: str) -> dict[str, Any] | None:
        value = getattr(business_profile, key, None)
        if isinstance(value, list):
            content = ", ".join(
                json.dumps(item, ensure_ascii=False) if isinstance(item, dict) else str(item)
                for item in value
                if item
            )
        else:
            content = str(value or "").strip()
        if not content:
            return None
        return {
            "type": "business_fact",
            "id": BUSINESS_FACT_IDS[key],
            "name": key.replace("_", " ").title(),
            "content": content,
            "score": 1.0,
            "metadata": {
                "source_type": "business_profile",
                "fact_key": key,
            },
        }

    def _match_to_context(self, match, row: dict[str, Any]) -> dict[str, Any]:
        content = match.description or ""
        if match.type == "product" and match.price is not None:
            content = (
                f"{content} Price: {match.price} {match.currency or 'MAD'}."
            ).strip()
        return {
            "type": match.type,
            "id": match.id,
            "name": match.name,
            "content": content,
            "score": match.score,
            "metadata": dict(match.metadata),
        }

    def _source_to_summary(self, source: dict[str, Any]) -> dict[str, Any]:
        return AISourceReference(
            type=source["type"],
            id=source["id"],
            name=source["name"],
            score=float(source["score"]),
            metadata=dict(source.get("metadata") or {}),
        ).model_dump()

    def _build_order_handoff_reply(
        self, business_profile, language: str
    ) -> tuple[AIModelReply, list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        sources = [
            item
            for item in (
                self._business_fact_item(business_profile, "whatsapp_number"),
                self._business_fact_item(business_profile, "support_phone"),
                self._business_fact_item(business_profile, "support_email"),
            )
            if item is not None
        ]
        contact_line = self._format_contact_line(business_profile, language)
        reply_text = {
            "english": (
                "Order management is handled by our support team, not directly in this chat. "
                f"Please contact support for order status, changes, cancellations, or complaints. {contact_line}"
            ),
            "french": (
                "La gestion des commandes est prise en charge par notre équipe support, pas directement dans ce chat. "
                f"Pour le suivi, les modifications, les annulations ou les réclamations, merci de contacter le support. {contact_line}"
            ),
            "darija": (
                "Tadبير dyal les commandes kaydirouh support, machi مباشرة من هاد الشات. "
                f"Ila bghiti suivi, modification, annulation, ولا شكاية، تواصل m3a support. {contact_line}"
            ),
        }[language]
        return self._compose_rule_based_reply(
            reply_text=reply_text,
            intent="autre",
            language=language,
            used_sources=sources,
            grounded=True,
            needs_human=True,
            confidence=0.98,
            reason_code="order_handoff",
            strategy="rule_based_order_handoff",
        )

    def _build_opening_hours_reply(
        self,
        business_profile,
        language: str,
        customer_message: str,
    ) -> tuple[AIModelReply, list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        opening_hours = business_profile.opening_hours or []
        normalized = customer_message.lower()
        if any(token in normalized for token in ("monday", "friday", "lundi", "vendredi", "weekday")):
            selected_hours = [
                item for item in opening_hours if item.lower().startswith("monday")
            ] or opening_hours
        elif any(token in normalized for token in ("saturday", "sunday", "samedi", "السبت", "الأحد")):
            selected_hours = [
                item
                for item in opening_hours
                if item.lower().startswith("saturday") or item.lower().startswith("sunday")
            ] or opening_hours
        else:
            selected_hours = opening_hours
        hours_line = ", ".join(selected_hours)
        reply_text = {
            "english": f"We are open {hours_line}.",
            "french": f"Nos horaires sont les suivants: {hours_line}.",
            "darija": f"Les horaires dyalna houma: {hours_line}.",
        }[language]
        sources = [
            item
            for item in (
                self._business_fact_item(business_profile, "opening_hours"),
            )
            if item is not None
        ]
        return self._compose_rule_based_reply(
            reply_text=reply_text,
            intent="infos_boutique",
            language=language,
            used_sources=sources,
            grounded=True,
            needs_human=False,
            confidence=0.99,
            reason_code="rule_based_opening_hours",
            strategy="rule_based_opening_hours",
        )

    def _build_contact_reply(
        self,
        business_profile,
        language: str,
        customer_message: str,
    ) -> tuple[AIModelReply, list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        wants_phone = any(token in customer_message.lower() for token in ("phone", "tel", "telephone", "numéro", "numero", "رقم"))
        wants_whatsapp = "whatsapp" in customer_message.lower()
        wants_email = "email" in customer_message.lower() or "mail" in customer_message.lower()

        channels = self._contact_channels(business_profile)
        requested_channels = []
        if wants_phone and channels["phone"]:
            requested_channels.append(("Phone", channels["phone"]))
        if wants_whatsapp and channels["whatsapp"]:
            requested_channels.append(("WhatsApp", channels["whatsapp"]))
        if wants_email and channels["email"]:
            requested_channels.append(("Email", channels["email"]))
        if not requested_channels:
            requested_channels = [
                (label, value)
                for label, value in (
                    ("Phone", channels["phone"]),
                    ("WhatsApp", channels["whatsapp"]),
                    ("Email", channels["email"]),
                )
                if value
            ]

        address = business_profile.store_address or business_profile.city
        channels_text = "; ".join(f"{label}: {value}" for label, value in requested_channels)
        reply_text = {
            "english": f"Our store address is {address}. {channels_text}",
            "french": f"Notre adresse est {address}. {channels_text}",
            "darija": f"L'adresse dyalna hiya {address}. {channels_text}",
        }[language]
        sources = [
            item
            for item in (
                self._business_fact_item(business_profile, "store_address"),
                self._business_fact_item(business_profile, "support_phone"),
                self._business_fact_item(business_profile, "whatsapp_number"),
                self._business_fact_item(business_profile, "support_email"),
            )
            if item is not None
        ]
        return self._compose_rule_based_reply(
            reply_text=reply_text,
            intent="infos_boutique",
            language=language,
            used_sources=sources,
            grounded=True,
            needs_human=False,
            confidence=0.99,
            reason_code="rule_based_contact",
            strategy="rule_based_contact",
        )

    def _build_return_policy_reply(
        self, business_profile, language: str
    ) -> tuple[AIModelReply, list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        conditions = " ".join(business_profile.return_conditions)
        if language == "english":
            reply_text = (
                f"You can return an item within {business_profile.return_window_days} days. "
                f"{conditions}"
            ).strip()
        elif language == "french":
            reply_text = (
                f"Vous pouvez retourner un produit sous {business_profile.return_window_days} jours. "
                f"{conditions}"
            ).strip()
        else:
            reply_text = (
                f"T9dar ترجع produit f {business_profile.return_window_days} أيام. "
                f"{conditions}"
            ).strip()
        sources = [
            item
            for item in (
                self._business_fact_item(business_profile, "return_window_days"),
                self._business_fact_item(business_profile, "return_conditions"),
                self._business_fact_item(business_profile, "return_policy"),
            )
            if item is not None
        ]
        return self._compose_rule_based_reply(
            reply_text=reply_text,
            intent="retour",
            language=language,
            used_sources=sources,
            grounded=True,
            needs_human=False,
            confidence=0.99,
            reason_code="rule_based_return_policy",
            strategy="rule_based_return_policy",
        )

    def _build_delivery_reply(
        self,
        business_profile,
        language: str,
        zone: dict[str, Any],
        *,
        include_tracking: bool,
    ) -> tuple[AIModelReply, list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        city = zone["city"]
        fee = zone["fee_mad"]
        eta = zone["estimated_time"]
        tracking_line = ""
        if include_tracking and business_profile.delivery_tracking_method:
            tracking_line = {
                "english": f" Tracking is available: {business_profile.delivery_tracking_method}",
                "french": f" Le suivi est disponible: {business_profile.delivery_tracking_method}",
                "darija": f" Tracking kayn: {business_profile.delivery_tracking_method}",
            }[language]
        reply_text = {
            "english": f"Yes, we deliver to {city} for {fee} MAD, with an estimated time of {eta}.{tracking_line}",
            "french": f"Oui, nous livrons à {city} pour {fee} MAD, avec un délai estimé de {eta}.{tracking_line}",
            "darija": f"Iyah, kandirou delivery l {city} b {fee} MAD, w l délai ta9riban {eta}.{tracking_line}",
        }[language]
        sources = [
            item
            for item in (
                self._business_fact_item(business_profile, "delivery_zone_details"),
                self._business_fact_item(business_profile, "delivery_tracking_method"),
            )
            if item is not None
        ]
        return self._compose_rule_based_reply(
            reply_text=reply_text,
            intent="livraison",
            language=language,
            used_sources=sources,
            grounded=True,
            needs_human=False,
            confidence=0.99,
            reason_code="rule_based_delivery",
            strategy="rule_based_delivery",
        )

    def _compose_rule_based_reply(
        self,
        *,
        reply_text: str,
        intent: str,
        language: str,
        used_sources: list[dict[str, Any]],
        grounded: bool,
        needs_human: bool,
        confidence: float,
        reason_code: str,
        strategy: str,
    ) -> tuple[AIModelReply, list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        reply = AIModelReply(
            reply_text=reply_text,
            intent=intent,
            language=language,
            grounded=grounded,
            needs_human=needs_human,
            confidence=confidence,
            reason_code=reason_code,
            used_sources=[
                AISourceReference(
                    type=item["type"],
                    id=item["id"],
                    name=item["name"],
                    score=float(item["score"]),
                    metadata=dict(item.get("metadata") or {}),
                )
                for item in used_sources
            ],
        )
        return (
            reply,
            used_sources,
            {"strategy": strategy, "prompt_version": PROMPT_VERSION},
            {"strategy": strategy, "structured_reply": reply.model_dump(mode="json")},
        )

    def _asks_for_opening_hours(self, message: str) -> bool:
        normalized = message.lower()
        return any(
            token in normalized
            for token in (
                "open",
                "close",
                "opening",
                "hours",
                "horaire",
                "horaires",
                "ouvre",
                "samedi",
                "sunday",
                "السبت",
                "الأحد",
            )
        )

    def _asks_for_contact_or_location(self, message: str) -> bool:
        normalized = message.lower()
        return any(
            token in normalized
            for token in (
                "address",
                "adresse",
                "store",
                "magasin",
                "phone",
                "telephone",
                "numéro",
                "numero",
                "whatsapp",
                "email",
                "contact",
                "location",
                "فين",
                "adresse",
            )
        )

    def _asks_for_tracking(self, message: str) -> bool:
        normalized = message.lower()
        return any(token in normalized for token in ("tracking", "suivi", "ntb3", "نتبع", "colis"))

    def _find_delivery_zone(self, business_profile, message: str) -> dict[str, Any] | None:
        normalized = message.lower()
        aliases = {"tanger": "Tangier", "casa": "Casablanca"}
        for zone in business_profile.delivery_zone_details:
            city = str(zone.get("city") or "")
            if city and city.lower() in normalized:
                return zone
        for alias, city in aliases.items():
            if alias in normalized:
                return next(
                    (zone for zone in business_profile.delivery_zone_details if zone.get("city") == city),
                    None,
                )
        return None

    def _contact_channels(self, business_profile) -> dict[str, str | None]:
        return {
            "phone": business_profile.support_phone,
            "whatsapp": business_profile.whatsapp_number,
            "email": business_profile.support_email,
        }

    def _format_contact_line(self, business_profile, language: str) -> str:
        channels = self._contact_channels(business_profile)
        pairs = [f"WhatsApp: {channels['whatsapp']}" if channels["whatsapp"] else None]
        pairs.append(f"Phone: {channels['phone']}" if channels["phone"] else None)
        pairs.append(f"Email: {channels['email']}" if channels["email"] else None)
        details = "; ".join(item for item in pairs if item)
        if language == "french":
            return f"Contacts support: {details}"
        if language == "darija":
            return f"Contacts dyal support: {details}"
        return f"Support contacts: {details}"

    async def _send_generated_reply(
        self,
        *,
        business_id: int,
        phone: str,
        reply: AIModelReply,
        connection: dict[str, Any],
    ) -> dict[str, Any] | None:
        config = dict(connection.get("config") or {})
        if not await self._is_free_text_allowed(business_id, phone):
            return None
        result = await self.messaging_provider.send_text(
            SendMessageCommand(
                business_id=business_id,
                phone=phone,
                text=reply.reply_text or "",
                config=config,
                subaccount_sid=str(config["subaccount_sid"]),
            )
        )
        row = await self.chat_repository.upsert_message(
            business_id=business_id,
            phone=result.to_phone,
            customer_name=None,
            text=reply.reply_text or "",
            direction="outbound",
            intent=reply.intent,
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

    def _result_to_response(self, result: dict[str, Any]) -> AIReplyResponse:
        run = result["run"]
        reply: AIModelReply = result["reply"]
        outbound_message = (
            chat_row_to_message(result["outbound_message"]) if result.get("outbound_message") else None
        )
        return AIReplyResponse(
            run_id=str(run["id"]),
            business_id=int(run["business_id"]),
            phone=run.get("phone"),
            customer_message=run.get("customer_message") or "",
            reply_text=reply.reply_text,
            intent=reply.intent,
            language=reply.language,
            grounded=reply.grounded,
            needs_human=reply.needs_human,
            confidence=reply.confidence,
            reason_code=reply.reason_code,
            follow_up_question=reply.follow_up_question,
            decision=result["decision"],
            used_sources=reply.used_sources,
            retrieved_sources=[
                AISourceReference.model_validate(item)
                for item in result.get("retrieved_sources", [])
            ],
            sent=bool(result.get("sent")),
            outbound_message=outbound_message,
            created_at=to_iso(run.get("created_at")) or "",
            updated_at=to_iso(run.get("updated_at")) or "",
        )

    def _write_audit_log(
        self,
        *,
        run_row: dict[str, Any],
        customer_message: str,
        request_payload: dict[str, Any],
        retrieval_summary: dict[str, Any],
        response_payload: dict[str, Any],
        decision: str,
        fallback_reason: str | None,
        sent: bool,
        outbound_row: dict[str, Any] | None,
    ) -> None:
        if not self.settings.ai_reply_audit_log_enabled:
            return

        record = {
            "event": "ai_reply_run",
            "run_id": run_row.get("id"),
            "business_id": run_row.get("business_id"),
            "phone": run_row.get("phone"),
            "status": run_row.get("status"),
            "decision": decision,
            "sent": sent,
            "fallback_reason": fallback_reason,
            "customer_message": customer_message,
            "intent": run_row.get("intent"),
            "language": run_row.get("language"),
            "confidence": float(run_row.get("confidence") or 0.0),
            "needs_human": bool(run_row.get("needs_human")),
            "reply_text": run_row.get("reply_text"),
            "provider": run_row.get("provider"),
            "model": run_row.get("model"),
            "prompt_version": run_row.get("prompt_version"),
            "inbound_chat_message_id": run_row.get("inbound_chat_message_id"),
            "outbound_chat_message_id": run_row.get("outbound_chat_message_id"),
            "outbound_message_provider_sid": outbound_row.get("provider_message_sid") if outbound_row else None,
            "created_at": to_iso(run_row.get("created_at")),
            "updated_at": to_iso(run_row.get("updated_at")),
            "retrieval_summary": retrieval_summary,
            "request_payload": request_payload,
            "response_payload": response_payload,
        }
        audit_logger.info(json.dumps(record, ensure_ascii=True, default=str))

    def _row_to_run_summary(self, row: dict[str, Any]) -> AIRunSummary:
        return AIRunSummary(
            id=str(row["id"]),
            business_id=int(row["business_id"]),
            phone=row.get("phone"),
            status=row["status"],
            customer_message=row.get("customer_message") or "",
            reply_text=row.get("reply_text"),
            language=row.get("language"),
            intent=row.get("intent"),
            needs_human=bool(row.get("needs_human")),
            confidence=float(row.get("confidence") or 0.0),
            fallback_reason=row.get("fallback_reason"),
            created_at=to_iso(row.get("created_at")) or "",
            updated_at=to_iso(row.get("updated_at")) or "",
        )

    def _row_to_run_detail(self, row: dict[str, Any]) -> AIRunDetail:
        return AIRunDetail(
            **self._row_to_run_summary(row).model_dump(),
            provider=row.get("provider") or "",
            model=row.get("model") or "",
            prompt_version=row.get("prompt_version") or PROMPT_VERSION,
            inbound_chat_message_id=str(row["inbound_chat_message_id"])
            if row.get("inbound_chat_message_id") is not None
            else None,
            outbound_chat_message_id=str(row["outbound_chat_message_id"])
            if row.get("outbound_chat_message_id") is not None
            else None,
            retrieval_summary=dict(row.get("retrieval_summary") or {}),
            request_payload=dict(row.get("request_payload") or {}),
            response_payload=dict(row.get("response_payload") or {}),
        )
