from __future__ import annotations

from datetime import UTC, datetime

from app.schemas.business import BusinessProfile
from app.services.dashboard_service import (
    build_conversation_summaries,
    build_setup_checklist,
    derive_sync_status,
    product_row_to_dashboard,
)


def test_product_row_to_dashboard_maps_variant_objects_and_stock() -> None:
    product = product_row_to_dashboard(
        {
            "id": 12,
            "business_id": 2,
            "external_id": "robe-satin-noire",
            "name": "Robe satin noire",
            "description": "Robe elegante",
            "category": "fashion",
            "price": 299,
            "currency": "MAD",
            "availability": "low_stock",
            "variants": [
                {
                    "id": "v1",
                    "name": "M",
                    "additional_price": 20,
                    "stock_status": "in_stock",
                }
            ],
            "created_at": datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            "updated_at": datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        }
    )

    assert product.id == "12"
    assert product.stock_status == "low_stock"
    assert product.variants[0].id == "v1"
    assert product.variants[0].additional_price == 20.0


def test_build_conversation_summaries_groups_by_phone() -> None:
    rows = [
        {
            "id": 2,
            "phone": "+212600000001",
            "customer_name": "Lina",
            "text": "Kayn livraison?",
            "direction": "inbound",
            "intent": "livraison",
            "needs_human": False,
            "is_read": False,
            "created_at": datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
        },
        {
            "id": 3,
            "phone": "+212600000001",
            "customer_name": "Lina",
            "text": "Oui kayna",
            "direction": "outbound",
            "intent": "livraison",
            "needs_human": False,
            "is_read": True,
            "created_at": datetime(2026, 3, 29, 12, 5, tzinfo=UTC),
        },
    ]

    summaries = build_conversation_summaries(rows)

    assert len(summaries) == 1
    assert summaries[0].phone == "+212600000001"
    assert summaries[0].inbound_count == 1
    assert summaries[0].outbound_count == 1
    assert summaries[0].unread_count == 1
    assert summaries[0].intents == ["livraison"]


def test_derive_sync_status_marks_recommended_when_products_need_sync() -> None:
    status = derive_sync_status(
        business_id=2,
        snapshot_row=None,
        counts={
            "synced_products": 0,
            "synced_business_knowledge": 1,
            "synced_faqs": 0,
            "last_embedded_at": datetime(2026, 3, 29, 15, 0, tzinfo=UTC),
        },
        has_products=True,
    )

    assert status.status == "recommended"
    assert status.ai_ready is False
    assert "Products exist but are not embedded yet." in status.stale_reasons


def test_build_setup_checklist_counts_completed_items() -> None:
    business = BusinessProfile(
        id=1,
        name="Boutique Lina",
        summary="Mode feminine",
        niche="fashion",
        city="Rabat",
        supported_languages=["fr", "ar"],
        tone_of_voice="friendly",
        opening_hours=["Mon-Fri 09:00-18:00"],
        delivery_zones=["Rabat"],
        delivery_time="24h",
        shipping_policy="Livraison partout au Maroc",
        return_policy="Retour sous 7 jours",
        payment_methods=["cash_on_delivery"],
        faq=[],
        order_rules=["Confirm sizes before checkout"],
        escalation_contact="+212600000000",
        upsell_rules=["Suggest matching bags"],
        updated_at="2026-03-29T10:00:00Z",
    )

    checklist = build_setup_checklist(business, active_products=3, whatsapp_connected=False)

    assert checklist.completed_count == 2
    assert checklist.total == 3
    assert checklist.items[-1].id == "whatsapp"
    assert checklist.items[-1].completed is False
