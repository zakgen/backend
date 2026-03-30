from __future__ import annotations

import logging
from typing import Any

from app.config import Settings, get_settings
from app.schemas.ai import AIModelReply


FACTUAL_INTENTS = {"livraison", "prix", "disponibilite", "retour", "paiement", "infos_produit"}
logger = logging.getLogger(__name__)


class ReplyValidationService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def validate(
        self,
        reply: AIModelReply,
        *,
        available_sources: list[dict[str, Any]],
    ) -> tuple[AIModelReply, str, str | None]:
        source_index = {
            (str(item["type"]), str(item["id"])): item for item in available_sources
        }
        filtered_sources = []
        for source in reply.used_sources:
            if (source.type, str(source.id)) in source_index:
                filtered_sources.append(source)
        reply.used_sources = filtered_sources

        if not (reply.reply_text or reply.follow_up_question):
            reply.needs_human = True
            reply.grounded = False
            logger.warning("AI reply validation failed: empty_reply")
            return reply, "failed", "empty_reply"

        if reply.follow_up_question and not reply.reply_text:
            reply.reply_text = reply.follow_up_question

        if not reply.grounded:
            reply.needs_human = True
            reason = reply.reason_code or "ungrounded_reply"
            logger.info("AI reply escalated: %s", reason)
            return reply, "needs_human", reason

        if reply.confidence < self.settings.ai_reply_confidence_threshold:
            reply.needs_human = True
            reason = reply.reason_code or "low_confidence"
            logger.info(
                "AI reply escalated: %s (confidence=%.3f threshold=%.3f)",
                reason,
                reply.confidence,
                self.settings.ai_reply_confidence_threshold,
            )
            return reply, "needs_human", reason

        if reply.intent in FACTUAL_INTENTS and not reply.used_sources:
            reply.needs_human = True
            reply.grounded = False
            logger.info("AI reply escalated: missing_sources")
            return reply, "needs_human", "missing_sources"

        if reply.needs_human:
            reason = reply.reason_code or "model_escalation"
            logger.info("AI reply escalated: %s", reason)
            return reply, "needs_human", reason

        reason = reply.reason_code or "grounded_answer"
        logger.info(
            "AI reply validated for send: %s (intent=%s confidence=%.3f used_sources=%d)",
            reason,
            reply.intent,
            reply.confidence,
            len(reply.used_sources),
        )
        return reply, "send", reason
