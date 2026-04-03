from __future__ import annotations

import logging
from typing import Any

from app.config import Settings, get_settings
from app.schemas.ai import AIModelReply
from app.services.ai_helpers import normalize_language_label


FACTUAL_INTENTS = {
    "livraison",
    "prix",
    "disponibilite",
    "retour",
    "paiement",
    "infos_produit",
    "infos_boutique",
}
logger = logging.getLogger(__name__)
MISSING_INFO_PHRASES = (
    "i don't have",
    "i do not have",
    "information unavailable",
    "unfortunately, i don't have",
    "ma3ndich",
    "ما عنديش",
    "je n'ai pas",
    "pas d'information",
)


class ReplyValidationService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def validate(
        self,
        reply: AIModelReply,
        *,
        available_sources: list[dict[str, Any]],
    ) -> tuple[AIModelReply, str, str | None]:
        reply.language = normalize_language_label(reply.language)
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

        if self._claims_missing_information(reply.reply_text) and available_sources:
            reply.needs_human = True
            reply.grounded = False
            logger.info("AI reply escalated: contradicted_by_available_facts")
            return reply, "needs_human", "contradicted_by_available_facts"

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

    def _claims_missing_information(self, reply_text: str | None) -> bool:
        normalized = (reply_text or "").strip().lower()
        if not normalized:
            return False
        return any(phrase in normalized for phrase in MISSING_INFO_PHRASES)
