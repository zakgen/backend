from app.services.ai_helpers import detect_language_hint, infer_intent_hint, source_preference


def test_detect_language_hint_prefers_darija_and_french_markers() -> None:
    assert detect_language_hint("Salam, wach kayna livraison ?") == "darija"
    assert detect_language_hint("Bonjour, quel est le prix ?") == "french"
    assert detect_language_hint("مرحبا واش كاين التوصيل") == "arabic"


def test_infer_intent_hint_matches_common_commerce_questions() -> None:
    assert infer_intent_hint("Chhal taman dial had produit ?") == "prix"
    assert infer_intent_hint("Kayn livraison l Rabat ?") == "livraison"
    assert infer_intent_hint("Est-ce que c'est disponible ?") == "disponibilite"
    assert infer_intent_hint("Salam") == "autre"


def test_source_preference_prioritizes_policy_for_shipping_questions() -> None:
    assert source_preference("livraison") == ("faq", "business_knowledge", "product")
    assert source_preference("prix") == ("product", "faq", "business_knowledge")
