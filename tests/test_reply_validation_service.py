from app.config import Settings
from app.schemas.ai import AIModelReply, AISourceReference
from app.services.reply_validation_service import ReplyValidationService


def _settings() -> Settings:
    return Settings(
        db_url="postgresql+asyncpg://postgres:postgres@localhost:5432/zakbot",
        ai_reply_confidence_threshold=0.7,
    )


def test_validation_escalates_factual_reply_without_matching_sources() -> None:
    service = ReplyValidationService(_settings())
    reply = AIModelReply(
        reply_text="Oui, livraison disponible a Rabat.",
        intent="livraison",
        language="french",
        grounded=True,
        confidence=0.92,
        used_sources=[
            AISourceReference(type="faq", id=77, name="shipping", score=0.9)
        ],
    )

    validated, decision, reason = service.validate(reply, available_sources=[])

    assert decision == "needs_human"
    assert reason == "missing_sources"
    assert validated.needs_human is True
    assert validated.grounded is False


def test_validation_allows_grounded_reply_with_sufficient_confidence() -> None:
    service = ReplyValidationService(_settings())
    reply = AIModelReply(
        reply_text="Oui, livraison disponible a Rabat sous 24 a 72h.",
        intent="livraison",
        language="french",
        grounded=True,
        confidence=0.88,
        used_sources=[
            AISourceReference(type="faq", id=4, name="Kayn livraison ?", score=0.94)
        ],
    )
    available_sources = [
        {
            "type": "faq",
            "id": 4,
            "name": "Kayn livraison ?",
            "score": 0.94,
            "metadata": {},
        }
    ]

    validated, decision, reason = service.validate(reply, available_sources=available_sources)

    assert decision == "send"
    assert reason == "grounded_answer"
    assert validated.needs_human is False
    assert len(validated.used_sources) == 1


def test_validation_accepts_business_fact_sources_when_available() -> None:
    service = ReplyValidationService(_settings())
    reply = AIModelReply(
        reply_text="Oui, livraison disponible a Rabat.",
        intent="livraison",
        language="french",
        grounded=True,
        confidence=0.91,
        used_sources=[
            AISourceReference(
                type="business_fact",
                id="delivery_zones",
                name="Delivery Zones",
                score=1.0,
            )
        ],
    )
    available_sources = [
        {
            "type": "business_fact",
            "id": "delivery_zones",
            "name": "Delivery Zones",
            "score": 1.0,
            "metadata": {"source_type": "business_profile", "fact_key": "delivery_zones"},
        }
    ]

    validated, decision, reason = service.validate(reply, available_sources=available_sources)

    assert decision == "send"
    assert reason == "grounded_answer"
    assert validated.used_sources[0].type == "business_fact"
