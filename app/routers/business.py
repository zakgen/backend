from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.business import (
    BusinessProfile,
    BusinessProfileUpdateRequest,
    BusinessResponse,
    BusinessUpsertRequest,
    OverviewData,
)
from app.schemas.conversation import ConversationSummary, ConversationThread
from app.schemas.integration import (
    CommerceIntegration,
    IntegrationsData,
    WhatsAppConnectRequest,
    WhatsAppIntegration,
    WhatsAppTestRequest,
    WhatsAppTestResponse,
)
from app.services.database import get_session
from app.services.dashboard_service import (
    PLATFORM_CATALOG,
    build_conversation_summaries,
    build_conversation_thread,
    build_integrations_data,
    build_overview,
    build_setup_checklist,
    business_row_to_profile,
    derive_sync_status,
    merge_business_update,
    product_row_to_dashboard,
)
from app.services.embedding_service import EmbeddingService
from app.services.messaging_service import MessagingService
from app.services.repository_factory import RepositoryFactory
from app.services.sync_service import SyncService
from app.services.twilio_provider import TwilioMessagingProvider


router = APIRouter(prefix="/business", tags=["business"])


@router.post("/upsert", response_model=BusinessResponse, status_code=status.HTTP_200_OK)
async def upsert_business(
    payload: BusinessUpsertRequest, session: AsyncSession = Depends(get_session)
) -> BusinessResponse:
    repository = RepositoryFactory(session).business()
    record = await repository.upsert(payload)

    sync_service = SyncService(session=session, embedding_service=EmbeddingService())
    await sync_service.sync_business_profile(record["id"])
    await sync_service.update_status_snapshot(
        record["id"], last_result="Business profile synced successfully."
    )
    await session.commit()

    refreshed = await repository.get_by_id(record["id"])
    return BusinessResponse.model_validate(refreshed)


@router.get("/{business_id}", response_model=BusinessProfile, status_code=status.HTTP_200_OK)
async def get_business_profile(
    business_id: int, session: AsyncSession = Depends(get_session)
) -> BusinessProfile:
    factory = RepositoryFactory(session)
    business_repository = factory.business()
    faq_repository = factory.faqs()
    business_row = await business_repository.get_by_id(business_id)
    faq_rows = await faq_repository.list_by_business(business_id)
    return business_row_to_profile(business_row, faq_rows)


@router.put("/{business_id}", response_model=BusinessProfile, status_code=status.HTTP_200_OK)
async def update_business_profile(
    business_id: int,
    payload: BusinessProfileUpdateRequest,
    session: AsyncSession = Depends(get_session),
) -> BusinessProfile:
    factory = RepositoryFactory(session)
    business_repository = factory.business()
    faq_repository = factory.faqs()
    existing_row = await business_repository.get_by_id(business_id)

    merged_payload = merge_business_update(existing_row, payload)
    updated_row = await business_repository.update_dashboard_profile(
        business_id, merged_payload
    )

    faq_rows = (
        await faq_repository.replace_for_business(
            business_id, [item.model_dump() for item in payload.faq]
        )
        if payload.faq is not None
        else await faq_repository.list_by_business(business_id)
    )

    sync_service = SyncService(session=session, embedding_service=EmbeddingService())
    await sync_service.sync_business_profile(business_id)
    if payload.faq is not None:
        await sync_service.sync_faqs(business_id)
    await sync_service.update_status_snapshot(
        business_id, last_result="Business profile updated from dashboard."
    )
    await session.commit()

    return business_row_to_profile(updated_row, faq_rows)


@router.get(
    "/{business_id}/chats",
    response_model=list[ConversationSummary],
    status_code=status.HTTP_200_OK,
)
async def list_business_chats(
    business_id: int,
    phone: str | None = Query(default=None),
    intent: str | None = Query(default=None),
    direction: str | None = Query(default=None),
    needs_human: bool | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> list[ConversationSummary]:
    factory = RepositoryFactory(session)
    await factory.business().get_by_id(business_id)
    rows = await factory.chats().list_messages(
        business_id,
        phone=phone,
        intent=intent,
        direction=direction,
        needs_human=needs_human,
    )
    return build_conversation_summaries(rows)


@router.get(
    "/{business_id}/chats/{phone}",
    response_model=ConversationThread,
    status_code=status.HTTP_200_OK,
)
async def get_business_chat_thread(
    business_id: int, phone: str, session: AsyncSession = Depends(get_session)
) -> ConversationThread:
    factory = RepositoryFactory(session)
    await factory.business().get_by_id(business_id)
    rows = await factory.chats().get_thread(business_id, phone)
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No conversation found for phone {phone}.",
        )
    return build_conversation_thread(phone, rows)


@router.get(
    "/{business_id}/overview",
    response_model=OverviewData,
    status_code=status.HTTP_200_OK,
)
async def get_business_overview(
    business_id: int, session: AsyncSession = Depends(get_session)
) -> OverviewData:
    factory = RepositoryFactory(session)
    business_repository = factory.business()
    faq_repository = factory.faqs()
    product_repository = factory.products()
    chat_repository = factory.chats()
    integration_repository = factory.integrations()
    sync_repository = factory.sync_status()

    business_row = await business_repository.get_by_id(business_id)
    faq_rows = await faq_repository.list_by_business(business_id)
    business_profile = business_row_to_profile(business_row, faq_rows)

    connections = await integration_repository.list_connections(business_id)
    connections_by_type = {row["integration_type"]: row for row in connections}
    active_products = await product_repository.count_active_by_business(business_id)
    checklist = build_setup_checklist(
        business_profile,
        active_products,
        (connections_by_type.get("whatsapp") or {}).get("status") == "connected",
    )

    sync_status = derive_sync_status(
        business_id=business_id,
        snapshot_row=await sync_repository.get_status(business_id),
        counts=await sync_repository.get_embedding_counts(business_id),
        has_products=await product_repository.count_by_business(business_id) > 0,
    )

    recent_chat_rows = await chat_repository.list_messages(business_id, limit=50)
    recent_chats = build_conversation_summaries(recent_chat_rows)[:5]
    recent_products = [
        product_row_to_dashboard(row)
        for row in await product_repository.recent_by_business(business_id, limit=5)
    ]

    return build_overview(
        total_conversations=await chat_repository.count_conversations(business_id),
        messages_handled=await chat_repository.count_messages(business_id),
        active_products=active_products,
        recent_chats=recent_chats,
        recent_products=recent_products,
        sync_status=sync_status,
        checklist=checklist,
    )


@router.get(
    "/{business_id}/integrations",
    response_model=IntegrationsData,
    status_code=status.HTTP_200_OK,
)
async def get_business_integrations(
    business_id: int, session: AsyncSession = Depends(get_session)
) -> IntegrationsData:
    factory = RepositoryFactory(session)
    business_repository = factory.business()
    product_repository = factory.products()
    faq_repository = factory.faqs()
    integration_repository = factory.integrations()

    business_row = await business_repository.get_by_id(business_id)
    faq_rows = await faq_repository.list_by_business(business_id)
    business_profile = business_row_to_profile(business_row, faq_rows)
    connections = await integration_repository.list_connections(business_id)
    connections_by_type = {row["integration_type"]: row for row in connections}
    checklist = build_setup_checklist(
        business_profile,
        await product_repository.count_active_by_business(business_id),
        (connections_by_type.get("whatsapp") or {}).get("status") == "connected",
    )

    return build_integrations_data(
        checklist=checklist,
        business_name=business_row["name"],
        whatsapp_row=connections_by_type.get("whatsapp"),
        platform_rows={
            platform: connections_by_type.get(platform) for platform in PLATFORM_CATALOG
        },
    )


@router.post(
    "/{business_id}/integrations/whatsapp/connect",
    response_model=WhatsAppIntegration,
    status_code=status.HTTP_200_OK,
)
async def connect_whatsapp(
    business_id: int,
    payload: WhatsAppConnectRequest,
    session: AsyncSession = Depends(get_session),
) -> WhatsAppIntegration:
    service = MessagingService(session=session, provider=TwilioMessagingProvider())
    connection = await service.begin_whatsapp_connection(business_id, payload)
    await session.commit()
    return WhatsAppIntegration.model_validate(connection)


@router.post(
    "/{business_id}/integrations/whatsapp/disconnect",
    response_model=WhatsAppIntegration,
    status_code=status.HTTP_200_OK,
)
async def disconnect_whatsapp(
    business_id: int, session: AsyncSession = Depends(get_session)
) -> WhatsAppIntegration:
    service = MessagingService(session=session, provider=TwilioMessagingProvider())
    connection = await service.disconnect_whatsapp(business_id)
    await session.commit()
    return WhatsAppIntegration.model_validate(connection)


@router.post(
    "/{business_id}/integrations/platforms/{platform}/sync",
    response_model=CommerceIntegration,
    status_code=status.HTTP_200_OK,
)
async def sync_platform_integration(
    business_id: int, platform: str, session: AsyncSession = Depends(get_session)
) -> CommerceIntegration:
    if platform not in PLATFORM_CATALOG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unsupported platform {platform}.",
        )
    if platform == "shopify":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Use the Shopify OAuth connect flow instead of the generic platform sync route.",
        )

    factory = RepositoryFactory(session)
    await factory.business().get_by_id(business_id)
    integration_repository = factory.integrations()
    product_count = await factory.products().count_by_business(business_id)
    existing = await integration_repository.get_connection(business_id, platform)
    connection = await integration_repository.upsert_connection(
        business_id=business_id,
        integration_type=platform,
        status_value="connected",
        health="healthy",
        config=dict((existing or {}).get("config") or {}),
        metrics={"imported_products": product_count},
        last_activity_at=(existing or {}).get("last_activity_at"),
        last_synced_at=datetime.now(UTC),
    )
    await session.commit()
    name, description = PLATFORM_CATALOG[platform]
    return CommerceIntegration(
        id=platform,  # type: ignore[arg-type]
        name=name,
        description=description,
        status=connection["status"],
        imported_products=product_count,
        last_sync_at=connection["last_synced_at"].isoformat().replace("+00:00", "Z")
        if connection.get("last_synced_at")
        else None,
    )


@router.post(
    "/{business_id}/integrations/whatsapp/test",
    response_model=WhatsAppTestResponse,
    status_code=status.HTTP_200_OK,
)
async def test_whatsapp_integration(
    business_id: int,
    payload: WhatsAppTestRequest,
    session: AsyncSession = Depends(get_session),
) -> WhatsAppTestResponse:
    integration = await MessagingService(
        session=session, provider=TwilioMessagingProvider()
    ).test_whatsapp(business_id)
    await session.commit()
    return WhatsAppTestResponse(
        success=True,
        message=payload.message,
        integration=WhatsAppIntegration.model_validate(integration),
    )
