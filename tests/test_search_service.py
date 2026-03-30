from app.services.search_formatting import (
    confidence_label,
    format_business_match,
    format_faq_match,
    format_product_match,
)


def test_confidence_label_thresholds() -> None:
    assert confidence_label(0.9) == "high"
    assert confidence_label(0.75) == "medium"
    assert confidence_label(0.6) == "low"


def test_format_product_match_merges_metadata() -> None:
    match = format_product_match(
        {
            "id": 12,
            "name": "Robe satin noire",
            "description": "Robe elegante",
            "price": 299,
            "currency": "MAD",
            "category": "fashion",
            "availability": "in_stock",
            "metadata": {"color": "black"},
            "score": 0.9134,
        }
    )

    assert match.type == "product"
    assert match.score == 0.9134
    assert match.metadata["category"] == "fashion"
    assert match.metadata["availability"] == "in_stock"
    assert match.metadata["confidence_label"] == "high"


def test_format_auxiliary_matches_are_stable() -> None:
    faq = format_faq_match(
        {
            "id": 7,
            "question": "Kayn livraison l Rabat?",
            "answer": "Oui",
            "metadata": {"topic": "shipping"},
            "score": 0.73,
        }
    )
    knowledge = format_business_match(
        {
            "id": 3,
            "title": "Boutique Lina",
            "content": "Shipping policy",
            "metadata": {"section": "profile"},
            "source_type": "profile",
            "score": 0.68,
        }
    )

    assert faq.type == "faq"
    assert faq.metadata["confidence_label"] == "medium"
    assert knowledge.type == "business_knowledge"
    assert knowledge.metadata["source_type"] == "profile"
    assert knowledge.metadata["confidence_label"] == "low"
