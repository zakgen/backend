from app.services.text_builder import (
    build_business_profile_text,
    build_faq_embedding_text,
    build_product_embedding_text,
)


def test_build_product_embedding_text_includes_core_fields() -> None:
    text = build_product_embedding_text(
        {
            "name": "Robe satin noire",
            "description": "Robe elegante",
            "category": "fashion",
            "price": 299,
            "currency": "MAD",
            "availability": "in_stock",
            "variants": ["S", "M"],
            "tags": ["robe", "satin"],
            "metadata": {"color": "black"},
        }
    )

    assert "Product: Robe satin noire." in text
    assert "Price: 299 MAD." in text
    assert "Metadata: color=black." in text


def test_build_business_profile_text_skips_empty_values() -> None:
    text = build_business_profile_text(
        {
            "name": "Boutique Lina",
            "city": "Rabat",
            "delivery_zones": ["Rabat", "Sale"],
            "payment_methods": ["cash_on_delivery"],
            "profile_metadata": {
                "support_phone": "+212600000000",
                "opening_hours": ["Mon-Fri 09:00-18:00"],
            },
        }
    )

    assert "Business: Boutique Lina." in text
    assert "City: Rabat." in text
    assert "Delivery zones: Rabat, Sale." in text
    assert "Support phone: +212600000000." in text
    assert "Opening hours: Mon-Fri 09:00-18:00." in text
    assert "Description:" not in text


def test_build_faq_embedding_text_includes_question_and_answer() -> None:
    text = build_faq_embedding_text(
        {
            "question": "Kayn livraison l Rabat?",
            "answer": "Oui, kayna livraison.",
            "metadata": {"topic": "shipping"},
        }
    )

    assert "FAQ question: Kayn livraison l Rabat?" in text
    assert "FAQ answer: Oui, kayna livraison." in text
    assert "Metadata: topic=shipping." in text
