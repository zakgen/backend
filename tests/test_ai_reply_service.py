from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.config import Settings
from app.schemas.ai import AIModelReply, AIReplyRequest, AISourceReference
from app.schemas.business import BusinessProfile
from app.schemas.order_confirmation import OrderSessionInterpretation
from app.services.ai_reply_service import AIReplyService
from app.services.ai_prompt_builder import build_ai_reply_prompts


class DummyLLMProvider:
    provider_name = "openai"
    model_name = "gpt-4.1-mini"

    def __init__(self) -> None:
        self.last_system_prompt: str | None = None
        self.last_user_prompt: str | None = None
        self.reply = AIModelReply(
            reply_text="Delivery usually takes 24 to 48 hours.",
            intent="livraison",
            language="english",
            grounded=True,
            needs_human=False,
            confidence=0.91,
            reason_code="grounded_answer",
            used_sources=[],
        )

    async def generate_structured_reply(self, *, system_prompt: str, user_prompt: str):
        self.last_system_prompt = system_prompt
        self.last_user_prompt = user_prompt
        return self.reply.model_copy(deep=True), {"structured_reply": self.reply.model_dump(mode="json")}

    async def detect_language(self, *, message: str):
        return "english", {"language_detection": {"language": "english"}}

    async def interpret_order_session(
        self,
        *,
        customer_message: str,
        preferred_language: str | None,
        session_status: str,
        order_snapshot: dict,
    ):
        return (
            OrderSessionInterpretation(
                language="english",
                primary_action="unknown",
                confidence=0.0,
                needs_human=True,
            ),
            {"order_session_interpretation": {"primary_action": "unknown"}},
        )


class DummyAIRunRepository:
    async def create_run(self, **kwargs):
        return {
            "id": 10,
            "business_id": kwargs["business_id"],
            "phone": kwargs["phone"],
            "customer_message": kwargs["customer_message"],
            "language": kwargs["language"],
            "intent": kwargs["intent"],
            "needs_human": kwargs["needs_human"],
            "confidence": kwargs["confidence"],
            "reply_text": kwargs["reply_text"],
            "fallback_reason": kwargs["fallback_reason"],
            "created_at": "2026-04-03T12:00:00Z",
            "updated_at": "2026-04-03T12:00:00Z",
        }

    async def update_run(self, run_id: int, **kwargs):
        return {"id": run_id, **kwargs}


@pytest.fixture
def business_profile() -> BusinessProfile:
    return BusinessProfile(
        id=2,
        name="Atlas Gadget Hub",
        summary="Electronics store",
        niche="electronics",
        city="Casablanca",
        supported_languages=["english", "french", "darija"],
        tone_of_voice="professional",
        opening_hours=[
            "Monday to Friday: 09:00-19:00",
            "Saturday: 10:00-17:00",
            "Sunday: Closed",
        ],
        store_address="27 Rue Al Massira, Maarif, Casablanca, Morocco",
        support_phone="+212522450980",
        whatsapp_number="+212661234567",
        support_email="support@atlasgadgethub.ma",
        delivery_zones=["Casablanca", "Rabat"],
        delivery_time="24 to 48 hours",
        delivery_tracking_method="WhatsApp tracking message",
        delivery_zone_details=[
            {"city": "Casablanca", "fee_mad": 20, "estimated_time": "Same day or next day"},
            {"city": "Rabat", "fee_mad": 35, "estimated_time": "24 to 48 hours"},
        ],
        shipping_policy="Delivery available",
        return_policy="Returns accepted within 7 days.",
        return_window_days=7,
        return_conditions=["Unused product", "Original packaging"],
        payment_methods=["cash_on_delivery"],
        faq=[],
        order_rules=[],
        escalation_contact="WhatsApp: +212661234567",
        upsell_rules=[],
        updated_at="2026-04-03T12:00:00Z",
    )


def _service() -> tuple[AIReplyService, DummyLLMProvider]:
    settings = Settings(
        database_backend="postgres",
        db_url="postgresql+asyncpg://postgres:postgres@localhost:5432/zakbot",
    )
    provider = DummyLLMProvider()
    service = AIReplyService(
        session=SimpleNamespace(),
        llm_provider=provider,
        settings=settings,
    )
    service.ai_run_repository = DummyAIRunRepository()
    return service, provider


@pytest.mark.asyncio
async def test_generate_preview_uses_llm_grounded_flow_for_delivery_question(
    monkeypatch, business_profile: BusinessProfile
) -> None:
    service, provider = _service()
    provider.reply = AIModelReply(
        reply_text="Delivery usually takes 24 to 48 hours.",
        intent="livraison",
        language="english",
        grounded=True,
        needs_human=False,
        confidence=0.93,
        reason_code="grounded_answer",
        used_sources=[
            AISourceReference(
                type="business_fact",
                id="delivery_time",
                name="Delivery Time",
                score=1.0,
                metadata={"source_type": "business_profile", "fact_key": "delivery_time"},
            )
        ],
    )

    async def fake_profile_loader(business_id: int):
        return business_profile

    async def fake_recent_messages(**kwargs):
        return []

    async def fake_select_context(**kwargs):
        return [
            {
                "type": "business_fact",
                "id": "delivery_time",
                "name": "Delivery Time",
                "content": "24 to 48 hours",
                "score": 1.0,
                "metadata": {"source_type": "business_profile", "fact_key": "delivery_time"},
            },
            {
                "type": "business_fact",
                "id": "shipping_policy",
                "name": "Shipping Policy",
                "content": "Delivery available",
                "score": 1.0,
                "metadata": {"source_type": "business_profile", "fact_key": "shipping_policy"},
            },
        ]

    monkeypatch.setattr(service, "_load_business_profile", fake_profile_loader)
    monkeypatch.setattr(service, "_load_recent_messages", fake_recent_messages)
    monkeypatch.setattr(service, "_select_context", fake_select_context)

    response = await service.generate_preview(
        2,
        AIReplyRequest(message="How long does delivery take?"),
    )

    assert response.intent == "livraison"
    assert response.reply_text == "Delivery usually takes 24 to 48 hours."
    assert response.needs_human is False
    assert provider.last_system_prompt is not None
    assert provider.last_user_prompt is not None
    assert "Delivery Time" in provider.last_user_prompt
    assert "How long does delivery take?" in provider.last_user_prompt


@pytest.mark.asyncio
async def test_generate_preview_escalates_when_grounded_evidence_is_missing(
    monkeypatch, business_profile: BusinessProfile
) -> None:
    service, provider = _service()
    provider.reply = AIModelReply(
        reply_text="Please contact support for that request.",
        intent="autre",
        language="english",
        grounded=False,
        needs_human=True,
        confidence=0.35,
        reason_code="missing_evidence",
        used_sources=[],
    )

    async def fake_profile_loader(business_id: int):
        return business_profile

    async def fake_recent_messages(**kwargs):
        return []

    async def fake_select_context(**kwargs):
        return []

    monkeypatch.setattr(service, "_load_business_profile", fake_profile_loader)
    monkeypatch.setattr(service, "_load_recent_messages", fake_recent_messages)
    monkeypatch.setattr(service, "_select_context", fake_select_context)

    response = await service.generate_preview(
        2,
        AIReplyRequest(message="Can I change my confirmed order?"),
    )

    assert response.needs_human is True
    assert response.decision == "needs_human"
    assert provider.last_user_prompt is not None
    assert "No retrieved business evidence was found." in provider.last_user_prompt


def test_business_fact_context_is_not_intent_gated(
    business_profile: BusinessProfile,
) -> None:
    service, _ = _service()

    fact_keys = {
        item["metadata"]["fact_key"] for item in service._business_fact_context(business_profile)
    }

    assert "delivery_time" in fact_keys
    assert "shipping_policy" in fact_keys
    assert "return_policy" in fact_keys
    assert "opening_hours" in fact_keys
    assert "support_phone" in fact_keys


def test_build_ai_reply_prompts_require_arabic_script_for_darija(
    business_profile: BusinessProfile,
) -> None:
    system_prompt, _ = build_ai_reply_prompts(
        business_profile=business_profile,
        customer_message="wach kayn delivery",
        recent_messages=[],
        selected_sources=[],
        language_hint="darija",
        intent_hint="autre",
    )

    assert "Arabic script" in system_prompt
    assert "Do not use Latin transliteration" in system_prompt
