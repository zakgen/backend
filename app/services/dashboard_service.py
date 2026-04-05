from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.schemas.business import (
    AIInsight,
    BusinessFAQItem,
    BusinessProfile,
    BusinessProfileUpdateRequest,
    OverviewData,
    OverviewStats,
    SetupChecklist,
    SetupChecklistItem,
)
from app.schemas.conversation import ConversationMessage, ConversationSummary, ConversationThread
from app.schemas.integration import (
    CommerceIntegration,
    ComingSoonIntegration,
    IntegrationsData,
    WhatsAppIntegration,
)
from app.schemas.product import Product, ProductVariant
from app.schemas.search import SyncStatusResponse


VALID_PAYMENT_METHODS = {"cash_on_delivery", "card_payment", "bank_transfer"}
VALID_TONES = {"formal", "friendly", "professional"}
VALID_STOCK_STATUSES = {"in_stock", "low_stock", "out_of_stock"}

PLATFORM_CATALOG: dict[str, tuple[str, str]] = {
    "youcan": ("YouCan", "Import products and catalog updates from YouCan."),
    "shopify": ("Shopify", "Sync your Shopify catalog and future orders."),
    "woocommerce": ("WooCommerce", "Keep WooCommerce products aligned with ZakBot."),
    "zid": ("Zid", "Sync your Zid catalog for WhatsApp selling."),
}

COMING_SOON = [
    ComingSoonIntegration(
        id="jumia", name="Jumia", description="Marketplace sync for catalog and stock."
    ),
    ComingSoonIntegration(
        id="instagram",
        name="Instagram DM",
        description="Shared inbox and reply suggestions for Instagram messages.",
    ),
]


def to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(value)


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    return []


def normalize_stock_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in VALID_STOCK_STATUSES:
        return normalized
    return "in_stock"


def normalize_variants(raw_variants: Any, fallback_status: str) -> list[ProductVariant]:
    variants: list[ProductVariant] = []
    for index, raw_variant in enumerate(raw_variants or [], start=1):
        if isinstance(raw_variant, dict):
            variant_id = str(raw_variant.get("id") or f"variant-{index}")
            name = str(raw_variant.get("name") or f"Variant {index}")
            additional_price = raw_variant.get("additional_price")
            variants.append(
                ProductVariant(
                    id=variant_id,
                    name=name,
                    additional_price=float(additional_price)
                    if additional_price is not None
                    else None,
                    stock_status=normalize_stock_status(
                        raw_variant.get("stock_status") or raw_variant.get("availability")
                    ),
                )
            )
            continue

        name = str(raw_variant).strip()
        if not name:
            continue
        variants.append(
            ProductVariant(
                id=f"variant-{index}",
                name=name,
                additional_price=None,
                stock_status=normalize_stock_status(fallback_status),
            )
        )
    return variants


def product_row_to_dashboard(row: dict[str, Any]) -> Product:
    stock_status = normalize_stock_status(row.get("availability"))
    return Product(
        id=str(row["id"]),
        business_id=int(row["business_id"]),
        external_id=row.get("external_id"),
        name=row.get("name") or "",
        description=row.get("description") or "",
        category=row.get("category") or "",
        price=float(row["price"]) if row.get("price") is not None else None,
        currency=row.get("currency") or "MAD",
        stock_status=stock_status,
        variants=normalize_variants(row.get("variants"), stock_status),
        created_at=to_iso(row.get("created_at")) or "",
        updated_at=to_iso(row.get("updated_at")) or "",
    )


def faq_row_to_business_faq(row: dict[str, Any]) -> BusinessFAQItem:
    return BusinessFAQItem(
        id=str(row["id"]),
        question=row.get("question") or "",
        answer=row.get("answer") or "",
    )


def business_row_to_profile(
    business_row: dict[str, Any], faq_rows: list[dict[str, Any]]
) -> BusinessProfile:
    metadata = dict(business_row.get("profile_metadata") or {})
    payment_methods = [
        method
        for method in (business_row.get("payment_methods") or metadata.get("payment_methods") or [])
        if method in VALID_PAYMENT_METHODS
    ]
    tone = metadata.get("tone_of_voice")
    if tone not in VALID_TONES:
        tone = "friendly"

    return BusinessProfile(
        id=int(business_row["id"]),
        name=business_row.get("name") or "",
        summary=business_row.get("description") or metadata.get("summary") or "",
        niche=str(metadata.get("niche") or ""),
        city=business_row.get("city") or "",
        supported_languages=_string_list(metadata.get("supported_languages")),
        tone_of_voice=tone,
        opening_hours=_string_list(metadata.get("opening_hours")),
        store_address=str(metadata.get("store_address") or "") or None,
        support_phone=str(metadata.get("support_phone") or "") or None,
        whatsapp_number=str(metadata.get("whatsapp_number") or "") or None,
        support_email=str(metadata.get("support_email") or "") or None,
        delivery_zones=_string_list(business_row.get("delivery_zones")),
        delivery_time=str(metadata.get("delivery_time") or ""),
        delivery_tracking_method=str(metadata.get("delivery_tracking_method") or "") or None,
        delivery_zone_details=list(metadata.get("delivery_zone_details") or []),
        shipping_policy=business_row.get("shipping_policy") or "",
        return_policy=str(metadata.get("return_policy") or ""),
        return_window_days=int(metadata["return_window_days"])
        if metadata.get("return_window_days") is not None
        else None,
        return_conditions=_string_list(metadata.get("return_conditions")),
        payment_methods=payment_methods,
        faq=[faq_row_to_business_faq(row) for row in faq_rows],
        order_rules=_string_list(metadata.get("order_rules")),
        escalation_contact=str(metadata.get("escalation_contact") or ""),
        upsell_rules=_string_list(metadata.get("upsell_rules")),
        updated_at=to_iso(business_row.get("updated_at")) or "",
    )


def merge_business_update(
    existing_row: dict[str, Any], payload: BusinessProfileUpdateRequest
) -> dict[str, Any]:
    metadata = dict(existing_row.get("profile_metadata") or {})

    field_map = {
        "niche": payload.niche,
        "supported_languages": payload.supported_languages,
        "tone_of_voice": payload.tone_of_voice,
        "opening_hours": payload.opening_hours,
        "store_address": payload.store_address,
        "support_phone": payload.support_phone,
        "whatsapp_number": payload.whatsapp_number,
        "support_email": payload.support_email,
        "delivery_time": payload.delivery_time,
        "delivery_tracking_method": payload.delivery_tracking_method,
        "delivery_zone_details": payload.delivery_zone_details,
        "return_policy": payload.return_policy,
        "return_window_days": payload.return_window_days,
        "return_conditions": payload.return_conditions,
        "order_rules": payload.order_rules,
        "escalation_contact": payload.escalation_contact,
        "upsell_rules": payload.upsell_rules,
    }
    for key, value in field_map.items():
        if value is not None:
            metadata[key] = value

    return {
        "name": payload.name if payload.name is not None else existing_row.get("name") or "",
        "description": payload.summary
        if payload.summary is not None
        else existing_row.get("description") or "",
        "city": payload.city if payload.city is not None else existing_row.get("city") or "",
        "shipping_policy": payload.shipping_policy
        if payload.shipping_policy is not None
        else existing_row.get("shipping_policy") or "",
        "delivery_zones": payload.delivery_zones
        if payload.delivery_zones is not None
        else existing_row.get("delivery_zones") or [],
        "payment_methods": payload.payment_methods
        if payload.payment_methods is not None
        else existing_row.get("payment_methods") or [],
        "profile_metadata": metadata,
    }


def build_product_storage_payload(
    *,
    business_id: int,
    external_id: str | None,
    name: str,
    description: str,
    category: str,
    price: float | None,
    currency: str,
    stock_status: str,
    variants: list[dict[str, Any]],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "business_id": business_id,
        "external_id": external_id,
        "name": name,
        "description": description,
        "category": category,
        "price": price,
        "currency": currency,
        "availability": normalize_stock_status(stock_status),
        "variants": variants,
        "tags": [],
        "metadata": metadata or {},
    }


def chat_row_to_message(row: dict[str, Any]) -> ConversationMessage:
    return ConversationMessage(
        id=str(row["id"]),
        phone=row.get("phone") or "",
        text=row.get("text") or "",
        direction=row.get("direction") or "inbound",
        timestamp=to_iso(row.get("created_at")) or "",
        intent=row.get("intent"),
        needs_human=bool(row.get("needs_human")) if row.get("needs_human") is not None else False,
    )


def build_conversation_summaries(rows: list[dict[str, Any]]) -> list[ConversationSummary]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in rows:
        phone = row.get("phone") or ""
        if not phone:
            continue

        created_at = to_iso(row.get("created_at")) or ""
        summary = grouped.get(phone)
        if summary is None:
            summary = {
                "phone": phone,
                "customer_name": row.get("customer_name"),
                "last_message": row.get("text") or "",
                "last_timestamp": created_at,
                "unread_count": 0,
                "intents": set(),
                "needs_human": False,
                "inbound_count": 0,
                "outbound_count": 0,
            }
            grouped[phone] = summary

        if row.get("direction") == "inbound":
            summary["inbound_count"] += 1
            if not row.get("is_read"):
                summary["unread_count"] += 1
        else:
            summary["outbound_count"] += 1

        if row.get("intent"):
            summary["intents"].add(row["intent"])
        summary["needs_human"] = summary["needs_human"] or bool(row.get("needs_human"))
        if not summary["customer_name"] and row.get("customer_name"):
            summary["customer_name"] = row["customer_name"]

    summaries = [
        ConversationSummary(
            phone=item["phone"],
            customer_name=item["customer_name"],
            last_message=item["last_message"],
            last_timestamp=item["last_timestamp"],
            unread_count=item["unread_count"],
            intents=sorted(item["intents"]),
            needs_human=item["needs_human"],
            inbound_count=item["inbound_count"],
            outbound_count=item["outbound_count"],
        )
        for item in grouped.values()
    ]
    summaries.sort(key=lambda item: item.last_timestamp, reverse=True)
    return summaries


def build_conversation_thread(phone: str, rows: list[dict[str, Any]]) -> ConversationThread:
    ordered_rows = sorted(rows, key=lambda row: to_iso(row.get("created_at")) or "")
    customer_name = next(
        (row.get("customer_name") for row in ordered_rows if row.get("customer_name")),
        None,
    )
    first_contact_at = to_iso(ordered_rows[0].get("created_at")) if ordered_rows else None
    return ConversationThread(
        phone=phone,
        customer_name=customer_name,
        first_contact_at=first_contact_at,
        messages=[chat_row_to_message(row) for row in ordered_rows],
    )


def derive_sync_status(
    *,
    business_id: int,
    snapshot_row: dict[str, Any] | None,
    counts: dict[str, Any],
    has_products: bool,
) -> SyncStatusResponse:
    synced_products = int(counts.get("synced_products") or 0)
    synced_business_knowledge = int(counts.get("synced_business_knowledge") or 0)
    synced_faqs = int(counts.get("synced_faqs") or 0)

    stale_reasons: list[str] = []
    if synced_business_knowledge == 0:
        stale_reasons.append("Business profile knowledge has not been embedded yet.")
    if has_products and synced_products == 0:
        stale_reasons.append("Products exist but are not embedded yet.")
    if not has_products:
        stale_reasons.append("No products have been added yet.")

    snapshot_status = snapshot_row.get("status") if snapshot_row else None
    last_result = snapshot_row.get("last_result") if snapshot_row else None
    if snapshot_status == "error" and last_result:
        stale_reasons.insert(0, last_result)

    ai_ready = synced_business_knowledge > 0 and (not has_products or synced_products > 0)

    if snapshot_status == "running":
        status = "running"
    elif snapshot_status == "error":
        status = "error"
    elif stale_reasons:
        status = "recommended"
    else:
        status = "up_to_date"

    return SyncStatusResponse(
        business_id=business_id,
        status=status,
        last_synced_at=to_iso(
            (snapshot_row or {}).get("last_synced_at") or counts.get("last_embedded_at")
        ),
        last_result=last_result,
        synced_products=synced_products,
        synced_business_knowledge=synced_business_knowledge,
        synced_faqs=synced_faqs,
        embedding_model=(snapshot_row or {}).get("embedding_model") or "text-embedding-3-small",
        ai_ready=ai_ready,
        stale_reasons=stale_reasons,
    )


def build_setup_checklist(
    business_profile: BusinessProfile, active_products: int, whatsapp_connected: bool
) -> SetupChecklist:
    items = [
        SetupChecklistItem(
            id="business",
            label="Business profile",
            completed=bool(
                business_profile.name and business_profile.summary and business_profile.city
            ),
            detail="Add your store profile, policies, and response rules.",
            action_href="/dashboard/business",
            action_label="Complete profile",
        ),
        SetupChecklistItem(
            id="products",
            label="Products",
            completed=active_products > 0,
            detail="Import or create products so ZakBot can answer product questions.",
            action_href="/dashboard/products",
            action_label="Manage products",
        ),
        SetupChecklistItem(
            id="whatsapp",
            label="WhatsApp",
            completed=whatsapp_connected,
            detail="Connect your WhatsApp number to receive and reply to leads.",
            action_href="/dashboard/integrations",
            action_label="Connect WhatsApp",
        ),
    ]
    completed_count = sum(1 for item in items if item.completed)
    return SetupChecklist(completed_count=completed_count, total=len(items), items=items)


def build_ai_insight(sync_status: SyncStatusResponse, checklist: SetupChecklist) -> AIInsight:
    if sync_status.status == "error":
        return AIInsight(
            title="AI sync needs attention",
            description=sync_status.last_result or "The last embedding sync failed.",
        )
    if checklist.completed_count < checklist.total:
        return AIInsight(
            title="Finish setup to improve answers",
            description="Complete the remaining checklist items so ZakBot can answer customers reliably.",
        )
    if not sync_status.ai_ready:
        return AIInsight(
            title="Knowledge sync recommended",
            description="Run an embedding sync to make the latest products and profile available to the assistant.",
        )
    return AIInsight(
        title="ZakBot AI is ready",
        description="Your catalog and business knowledge are available for dashboard and WhatsApp flows.",
    )


def build_overview(
    *,
    total_conversations: int,
    messages_handled: int,
    active_products: int,
    recent_chats: list[ConversationSummary],
    recent_products: list[Product],
    sync_status: SyncStatusResponse,
    checklist: SetupChecklist,
) -> OverviewData:
    sync_notice = None
    if sync_status.status in {"recommended", "error"} and sync_status.stale_reasons:
        sync_notice = sync_status.stale_reasons[0]

    return OverviewData(
        stats=OverviewStats(
            total_conversations=total_conversations,
            messages_handled=messages_handled,
            active_products=active_products,
            ai_knowledge_status=sync_status.status,
        ),
        recent_chats=recent_chats,
        recent_products=recent_products,
        ai_insight=build_ai_insight(sync_status, checklist),
        sync_notice=sync_notice,
        checklist=checklist,
    )


def build_whatsapp_integration(
    business_name: str, connection_row: dict[str, Any] | None
) -> WhatsAppIntegration:
    config = dict((connection_row or {}).get("config") or {})
    metrics = dict((connection_row or {}).get("metrics") or {})
    return WhatsAppIntegration(
        phone_number=str(config.get("whatsapp_number") or config.get("phone_number") or ""),
        business_name=str(config.get("business_name") or business_name),
        status=(connection_row or {}).get("status") or "disconnected",
        health=(connection_row or {}).get("health") or "attention",
        received_messages_last_30_days=int(metrics.get("received_messages_last_30_days") or 0),
        last_activity_at=to_iso((connection_row or {}).get("last_activity_at")),
    )


def build_platform_integration(
    platform_id: str, connection_row: dict[str, Any] | None
) -> CommerceIntegration:
    name, description = PLATFORM_CATALOG[platform_id]
    metrics = dict((connection_row or {}).get("metrics") or {})
    config = dict((connection_row or {}).get("config") or {})
    return CommerceIntegration(
        id=platform_id,  # type: ignore[arg-type]
        name=name,
        description=description,
        status=(connection_row or {}).get("status") or "disconnected",
        imported_products=int(metrics.get("imported_products") or 0),
        last_sync_at=to_iso((connection_row or {}).get("last_synced_at")),
        shop_domain=str(config.get("shop_domain") or "") or None,
        last_activity_at=to_iso((connection_row or {}).get("last_activity_at")),
        last_sync_back_at=to_iso(config.get("last_sync_back_at")),
        webhook_status=str(config.get("webhook_status") or "") or None,
    )


def build_integrations_data(
    *,
    checklist: SetupChecklist,
    business_name: str,
    whatsapp_row: dict[str, Any] | None,
    platform_rows: dict[str, dict[str, Any]],
) -> IntegrationsData:
    return IntegrationsData(
        checklist=checklist,
        whatsapp=build_whatsapp_integration(business_name, whatsapp_row),
        platforms=[
            build_platform_integration(platform_id, platform_rows.get(platform_id))
            for platform_id in PLATFORM_CATALOG
        ],
        coming_soon=COMING_SOON,
    )
