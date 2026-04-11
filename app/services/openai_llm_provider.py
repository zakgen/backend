from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, status
from openai import AsyncOpenAI

from app.config import Settings, get_settings
from app.schemas.ai import AIModelReply
from app.schemas.order_confirmation import OrderSessionInterpretation
from app.services.llm_provider import AbstractLLMProvider


class OpenAILLMProvider(AbstractLLMProvider):
    provider_name = "openai"
    _LANGUAGE_SYSTEM_PROMPT = """
You classify the customer's message language for a Moroccan ecommerce assistant.

Return valid JSON with exactly one key:
- language: one of english, french, darija

Rules:
- Use darija for Moroccan Arabic, including Arabic script, Latin transliteration, or mixed-script Darija.
- Use french for French.
- Use english for English.
- Output only JSON.
""".strip()
    _ORDER_SESSION_SYSTEM_PROMPT = """
You interpret customer replies during an ecommerce WhatsApp order confirmation session.

Return valid JSON with exactly these keys:
- language: one of english, french, darija
- primary_action: one of confirm, decline, edit_request, delivery_question, payment_question, return_policy_question, support_request, unknown
- secondary_actions: array of secondary actions using the same allowed values
- confidence: number between 0 and 1
- needs_human: boolean
- question_type: string or null
- edits: array of objects with:
  - field: one of delivery_address, delivery_city, customer_phone, quantity, variant, product_name
  - value: string
- cancellation_reason: string or null
- reply_summary: short string summary

Rules:
- Use the order snapshot as the source of truth.
- Detect mixed intents when present.
- If language=darija, write any customer-facing Darija content in Arabic script, not Latin transliteration.
- If the customer confirms but also asks to change an address/variant/quantity, use primary_action=edit_request and include confirm in secondary_actions.
- If the customer asks about delivery ETA/fee/city/address confirmation, use delivery_question.
- If the customer asks about payment, use payment_question.
- If the customer asks about return or refund, use return_policy_question.
- If the request is ambiguous or risky, set needs_human=true.
- Output only JSON.
""".strip()

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.model_name = self.settings.openai_chat_model
        self._client: AsyncOpenAI | None = None

    def _get_client(self) -> AsyncOpenAI:
        if self.settings.openai_api_key is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="OPENAI_API_KEY is required for AI replies.",
            )
        if self._client is None:
            self._client = AsyncOpenAI(
                api_key=self.settings.openai_api_key.get_secret_value()
            )
        return self._client

    async def generate_structured_reply(
        self, *, system_prompt: str, user_prompt: str
    ) -> tuple[AIModelReply, dict]:
        try:
            response = await self._get_client().chat.completions.create(
                model=self.model_name,
                temperature=0.2,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"OpenAI reply generation failed: {exc}",
            ) from exc

        content = response.choices[0].message.content if response.choices else None
        if not content:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="OpenAI returned an empty AI reply payload.",
            )

        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="OpenAI returned invalid JSON for the AI reply payload.",
            ) from exc

        structured = AIModelReply.model_validate(payload)
        response_payload: dict[str, Any] = {
            "id": response.id,
            "model": response.model,
            "finish_reason": response.choices[0].finish_reason if response.choices else None,
            "structured_reply": payload,
        }
        return structured, response_payload

    async def detect_language(self, *, message: str) -> tuple[str, dict]:
        try:
            response = await self._get_client().chat.completions.create(
                model=self.model_name,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": self._LANGUAGE_SYSTEM_PROMPT},
                    {"role": "user", "content": message},
                ],
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"OpenAI language detection failed: {exc}",
            ) from exc

        content = response.choices[0].message.content if response.choices else None
        if not content:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="OpenAI returned an empty language detection payload.",
            )

        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="OpenAI returned invalid JSON for language detection.",
            ) from exc

        language = str(payload.get("language") or "").strip().lower()
        if language not in {"english", "french", "darija"}:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"OpenAI returned unsupported language label: {language or 'empty'}.",
            )

        response_payload: dict[str, Any] = {
            "id": response.id,
            "model": response.model,
            "finish_reason": response.choices[0].finish_reason if response.choices else None,
            "language_detection": payload,
        }
        return language, response_payload

    async def interpret_order_session(
        self,
        *,
        customer_message: str,
        preferred_language: str | None,
        session_status: str,
        order_snapshot: dict,
    ) -> tuple[OrderSessionInterpretation, dict]:
        user_prompt = json.dumps(
            {
                "customer_message": customer_message,
                "preferred_language": preferred_language,
                "session_status": session_status,
                "order_snapshot": order_snapshot,
            },
            ensure_ascii=False,
        )
        try:
            response = await self._get_client().chat.completions.create(
                model=self.model_name,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": self._ORDER_SESSION_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"OpenAI order session interpretation failed: {exc}",
            ) from exc

        content = response.choices[0].message.content if response.choices else None
        if not content:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="OpenAI returned an empty order session interpretation payload.",
            )

        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="OpenAI returned invalid JSON for order session interpretation.",
            ) from exc

        interpretation = OrderSessionInterpretation.model_validate(payload)
        response_payload: dict[str, Any] = {
            "id": response.id,
            "model": response.model,
            "finish_reason": response.choices[0].finish_reason if response.choices else None,
            "order_session_interpretation": payload,
        }
        return interpretation, response_payload
