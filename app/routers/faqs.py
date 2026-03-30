from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.faq import FAQResponse, FAQUpsertRequest
from app.services.database import get_session
from app.services.embedding_service import EmbeddingService
from app.services.repository_factory import RepositoryFactory
from app.services.sync_service import SyncService


router = APIRouter(prefix="/faqs", tags=["faqs"])


@router.post("/upsert", response_model=FAQResponse, status_code=status.HTTP_200_OK)
async def upsert_faq(
    payload: FAQUpsertRequest, session: AsyncSession = Depends(get_session)
) -> FAQResponse:
    repository = RepositoryFactory(session).faqs()
    faq = await repository.upsert(payload)

    sync_service = SyncService(session=session, embedding_service=EmbeddingService())
    await sync_service.sync_faqs(payload.business_id, faq_ids=[faq["id"]])
    await sync_service.update_status_snapshot(
        payload.business_id, last_result="FAQ synced successfully."
    )
    await session.commit()

    refreshed = await repository.get_by_id(payload.business_id, faq["id"])
    return FAQResponse.model_validate(refreshed)
