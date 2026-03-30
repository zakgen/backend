from __future__ import annotations

from collections.abc import Mapping
import json
import logging
from typing import Any

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
from app.services.ai_helpers import detect_language_hint, infer_intent_hint, source_preference
from app.services.ai_prompt_builder import PROMPT_VERSION, build_ai_reply_prompts
from app.services.dashboard_service import business_row_to_profile, chat_row_to_message, to_iso
from app.services.embedding_service import EmbeddingService
from app.services.llm_provider import AbstractLLMProvider
from app.services.openai_llm_provider import OpenAILLMProvider
from app.services.reply_validation_service import ReplyValidationService
from app.services.repositories import (
    AIRunRepository,
    BusinessRepository,
    ChatRepository,
    FAQRepository,
    IntegrationRepository,
    ProductRepository,
)
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
    "delivery_zones": "delivery_zones",
    "delivery_time": "delivery_time",
    "shipping_policy": "shipping_policy",
    "return_policy": "return_policy",
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
        self.business_repository = BusinessRepository(session)
        self.product_repository = ProductRepository(session)
        self.faq_repository = FAQRepository(session)
        self.chat_repository = ChatRepository(session)
        self.integration_repository = IntegrationRepository(session)
        self.ai_run_repository = AIRunRepository(session)

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
        language_hint = detect_language_hint(customer_message)
        retrieval_summary: dict[str, Any] = {
            "intent_hint": intent_hint,
            "language_hint": language_hint,
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
            selected_context_items = await self._select_context(
                business_id=business_id,
                customer_message=customer_message,
                intent_hint=intent_hint,
                business_profile=business_profile,
            )
            retrieval_summary["selected_sources"] = [
                self._source_to_summary(item) for item in selected_context_items
            ]
            retrieval_summary["history_messages"] = recent_messages

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
        if not reply.language:
            reply.language = language_hint

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
            "livraison": ("delivery_zones", "delivery_time", "shipping_policy", "city"),
            "paiement": ("payment_methods",),
            "retour": ("return_policy",),
            "infos_produit": ("summary", "niche"),
            "autre": ("summary", "niche"),
        }.get(
            intent_hint,
            ("summary", "niche"),
        )

        items: list[dict[str, Any]] = []
        for key in candidate_keys:
            value = getattr(business_profile, key, None)
            if isinstance(value, list):
                content = ", ".join(str(item) for item in value if item)
            else:
                content = str(value or "").strip()
            if not content:
                continue
            items.append(
                {
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
            )
        return items

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

    async def _send_generated_reply(
        self,
        *,
        business_id: int,
        phone: str,
        reply: AIModelReply,
        connection: dict[str, Any],
    ) -> dict[str, Any]:
        config = dict(connection.get("config") or {})
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
