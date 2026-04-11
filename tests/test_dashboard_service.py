from __future__ import annotations

from datetime import UTC, datetime

from app.schemas.business import BusinessProfile
from app.schemas.business import BusinessProfileUpdateRequest
from app.services.dashboard_service import (
    build_conversation_summaries,
    build_conversation_thread,
    build_setup_checklist,
    business_row_to_profile,
    derive_sync_status,
    merge_business_update,
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
    assert summaries[0].message_context == "general"
    assert summaries[0].order_window_status is None


def test_build_conversation_summaries_marks_latest_order_confirmation_context() -> None:
    rows = [
        {
            "id": 2,
            "phone": "+212600000001",
            "customer_name": "Lina",
            "text": "1",
            "direction": "inbound",
            "intent": "autre",
            "needs_human": False,
            "is_read": False,
            "created_at": datetime(2026, 3, 29, 12, 5, tzinfo=UTC),
        },
    ]

    summaries = build_conversation_summaries(
        rows,
        latest_sessions_by_phone={
            "+212600000001": {
                "id": 21,
                "order_id": 10,
                "status": "awaiting_customer",
                "started_at": datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                "structured_snapshot": {"external_order_id": "WC-1001"},
            }
        },
    )

    assert summaries[0].message_context == "order_confirmation"
    assert summaries[0].order_window_status == "ongoing"
    assert summaries[0].order_session_id == "21"
    assert summaries[0].order_id == "10"
    assert summaries[0].order_external_id == "WC-1001"


def test_build_conversation_thread_marks_messages_before_session_start_as_general() -> None:
    thread = build_conversation_thread(
        "+212600000001",
        [
            {
                "id": 1,
                "phone": "+212600000001",
                "customer_name": "Lina",
                "text": "hello",
                "direction": "inbound",
                "intent": None,
                "needs_human": False,
                "created_at": datetime(2026, 3, 29, 11, 0, tzinfo=UTC),
            },
            {
                "id": 2,
                "phone": "+212600000001",
                "customer_name": "Lina",
                "text": "1",
                "direction": "inbound",
                "intent": "autre",
                "needs_human": False,
                "created_at": datetime(2026, 3, 29, 12, 5, tzinfo=UTC),
            },
        ],
        latest_session={
            "id": 21,
            "order_id": 10,
            "status": "confirmed",
            "started_at": datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            "structured_snapshot": {"external_order_id": "WC-1001"},
        },
    )

    assert thread.messages[0].message_context == "general"
    assert thread.messages[0].order_window_status is None
    assert thread.messages[1].message_context == "order_confirmation"
    assert thread.messages[1].order_window_status == "closed"
    assert thread.messages[1].order_session_id == "21"


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


def test_business_row_to_profile_maps_extended_support_fields() -> None:
    profile = business_row_to_profile(
        {
            "id": 5,
            "name": "Atlas Gadget Hub",
            "description": "Electronics store",
            "city": "Casablanca",
            "shipping_policy": "Delivery available",
            "delivery_zones": ["Casablanca", "Rabat"],
            "payment_methods": ["cash_on_delivery"],
            "profile_metadata": {
                "supported_languages": ["english", "darija"],
                "tone_of_voice": "professional",
                "opening_hours": ["Monday to Friday: 09:00-19:00", "Saturday: 10:00-17:00"],
                "store_address": "27 Rue Al Massira, Maarif, Casablanca, Morocco",
                "support_phone": "+212522450980",
                "whatsapp_number": "+212661234567",
                "support_email": "support@atlasgadgethub.ma",
                "delivery_time": "24 to 48 hours",
                "delivery_tracking_method": "WhatsApp tracking",
                "delivery_zone_details": [{"city": "Rabat", "fee_mad": 35}],
                "return_policy": "Returns accepted",
                "return_window_days": 7,
                "return_conditions": ["Unused product", "Original packaging"],
                "escalation_contact": "WhatsApp support",
            },
            "updated_at": datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        },
        [],
    )

    assert profile.store_address == "27 Rue Al Massira, Maarif, Casablanca, Morocco"
    assert profile.support_phone == "+212522450980"
    assert profile.whatsapp_number == "+212661234567"
    assert profile.support_email == "support@atlasgadgethub.ma"
    assert profile.return_window_days == 7
    assert profile.return_conditions == ["Unused product", "Original packaging"]
    assert profile.default_language == "arabic"


def test_business_row_to_profile_maps_default_language_from_metadata() -> None:
    profile = business_row_to_profile(
        {
            "id": 6,
            "name": "Boutique Lina",
            "description": "Fashion store",
            "city": "Rabat",
            "shipping_policy": "Delivery available",
            "delivery_zones": ["Rabat"],
            "payment_methods": ["cash_on_delivery"],
            "profile_metadata": {
                "default_language": "french",
            },
            "updated_at": datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        },
        [],
    )

    assert profile.default_language == "french"


def test_merge_business_update_maps_frontend_arabic_to_internal_darija() -> None:
    payload = merge_business_update(
        {
            "name": "Boutique Lina",
            "description": "Fashion store",
            "city": "Rabat",
            "shipping_policy": "Delivery available",
            "delivery_zones": ["Rabat"],
            "payment_methods": ["cash_on_delivery"],
            "profile_metadata": {},
        },
        BusinessProfileUpdateRequest(default_language="arabic"),
    )

    assert payload["profile_metadata"]["default_language"] == "darija"
