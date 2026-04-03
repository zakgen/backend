from app.services.ai_helpers import (
    infer_intent_hint,
    is_order_management_request,
    normalize_language_label,
    source_preference,
)


def test_normalize_language_label_maps_supported_variants() -> None:
    assert normalize_language_label("ar") == "darija"
    assert normalize_language_label("french") == "french"
    assert normalize_language_label("en") == "english"


def test_infer_intent_hint_matches_common_commerce_questions() -> None:
    assert infer_intent_hint("Chhal taman dial had produit ?") == "prix"
    assert infer_intent_hint("Kayn livraison l Rabat ?") == "livraison"
    assert infer_intent_hint("Est-ce que c'est disponible ?") == "disponibilite"
    assert infer_intent_hint("What time do you open on Saturday?") == "infos_boutique"
    assert infer_intent_hint("Can you send me your exact store address in Casablanca and the best phone number to reach support?") == "infos_boutique"
    assert infer_intent_hint("ila chrit produit w ma3jbnich, واش عندي 7 أيام باش نرجعو؟") == "retour"
    assert infer_intent_hint("هاد JBL Go 3 Speaker باقي مقطوع ولا رجع؟") == "disponibilite"
    assert infer_intent_hint("Salam") == "autre"


def test_source_preference_prioritizes_policy_for_shipping_questions() -> None:
    assert source_preference("livraison") == ("faq", "business_knowledge", "product")
    assert source_preference("prix") == ("product", "faq", "business_knowledge")
    assert source_preference("infos_boutique") == ("business_fact", "faq", "business_knowledge")


def test_order_management_detection_marks_manual_support_cases() -> None:
    assert is_order_management_request("Can I cancel my order?") is True
    assert is_order_management_request("بغيت نبدل commande") is True
    assert is_order_management_request("What is your phone number?") is False
