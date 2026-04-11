from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.config import Settings
from app.schemas.ai import AIReplyRequest
from app.schemas.business import BusinessProfile
from app.schemas.order_confirmation import OrderSessionInterpretation
from app.services.ai_reply_service import AIReplyService
from app.services.ai_prompt_builder import build_ai_reply_prompts


class DummyLLMProvider:
    provider_name = "openai"
    model_name = "gpt-4.1-mini"

    async def generate_structured_reply(self, *, system_prompt: str, user_prompt: str):
        raise AssertionError("LLM should not be called for deterministic replies.")

    async def detect_language(self, *, message: str):
        lowered = message.lower()
        if "cancel my order" in lowered:
            return "english", {"language_detection": {"language": "english"}}
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


def _service() -> AIReplyService:
    settings = Settings(
        database_backend="postgres",
        db_url="postgresql+asyncpg://postgres:postgres@localhost:5432/zakbot",
    )
    service = AIReplyService(
        session=SimpleNamespace(),
        llm_provider=DummyLLMProvider(),
        settings=settings,
    )
    service.ai_run_repository = DummyAIRunRepository()
    return service


@pytest.mark.asyncio
async def test_generate_preview_returns_rule_based_hours_reply(monkeypatch, business_profile: BusinessProfile) -> None:
    service = _service()

    async def fake_profile_loader(business_id: int):
        return business_profile

    async def fake_recent_messages(**kwargs):
        return []

    monkeypatch.setattr(service, "_load_business_profile", fake_profile_loader)
    monkeypatch.setattr(service, "_load_recent_messages", fake_recent_messages)

    response = await service.generate_preview(
        2,
        AIReplyRequest(message="What time do you open on Saturday and are you closed on Sunday?"),
    )

    assert response.intent == "infos_boutique"
    assert "Saturday: 10:00-17:00" in (response.reply_text or "")
    assert "Sunday: Closed" in (response.reply_text or "")
    assert response.needs_human is False


@pytest.mark.asyncio
async def test_generate_preview_returns_order_handoff(monkeypatch, business_profile: BusinessProfile) -> None:
    service = _service()

    async def fake_profile_loader(business_id: int):
        return business_profile

    async def fake_recent_messages(**kwargs):
        return []

    monkeypatch.setattr(service, "_load_business_profile", fake_profile_loader)
    monkeypatch.setattr(service, "_load_recent_messages", fake_recent_messages)

    response = await service.generate_preview(
        2,
        AIReplyRequest(message="Can I cancel my order and make a complaint here?"),
    )

    assert response.intent == "autre"
    assert response.needs_human is True
    assert "support" in (response.reply_text or "").lower()
    assert "whatsapp" in (response.reply_text or "").lower()


def test_build_ai_reply_prompts_require_arabic_script_for_darija(
    business_profile: BusinessProfile,
) -> None:
    system_prompt, _ = build_ai_reply_prompts(
        business_profile=business_profile,
        customer_message="wach kayn delivery",
        recent_messages=[],
        selected_sources=[],
        language_hint="darija",
        intent_hint="livraison",
    )

    assert "Arabic script" in system_prompt
    assert "Do not use Latin transliteration" in system_prompt


def test_rule_based_darija_contact_reply_uses_arabic_script(
    business_profile: BusinessProfile,
) -> None:
    service = _service()
    reply, _, _, _ = service._build_contact_reply(
        business_profile,
        "darija",
        "فين العنوان؟",
    )

    assert reply.language == "darija"
    assert "العنوان ديالنا هو" in (reply.reply_text or "")
    assert "dyalna" not in (reply.reply_text or "")
