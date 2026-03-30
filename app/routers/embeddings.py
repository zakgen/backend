from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.search import EmbeddingSyncResponse, SyncStatusResponse
from app.services.database import get_session
from app.services.dashboard_service import derive_sync_status
from app.services.embedding_service import EmbeddingService
from app.services.repository_factory import RepositoryFactory
from app.services.sync_service import SyncService


router = APIRouter(prefix="/embeddings", tags=["embeddings"])


@router.get(
    "/sync/business/{business_id}/status",
    response_model=SyncStatusResponse,
    status_code=status.HTTP_200_OK,
)
async def get_business_embedding_status(
    business_id: int, session: AsyncSession = Depends(get_session)
) -> SyncStatusResponse:
    factory = RepositoryFactory(session)
    sync_repository = factory.sync_status()
    return derive_sync_status(
        business_id=business_id,
        snapshot_row=await sync_repository.get_status(business_id),
        counts=await sync_repository.get_embedding_counts(business_id),
        has_products=await factory.products().count_by_business(business_id) > 0,
    )


@router.post(
    "/sync/business/{business_id}",
    response_model=EmbeddingSyncResponse,
    status_code=status.HTTP_200_OK,
)
async def sync_business_embeddings(
    business_id: int, session: AsyncSession = Depends(get_session)
) -> EmbeddingSyncResponse:
    service = SyncService(session=session, embedding_service=EmbeddingService())
    await service.mark_running(business_id)
    await session.commit()

    try:
        result = await service.sync_business_embeddings(business_id)
        await session.commit()
        return EmbeddingSyncResponse.model_validate(result)
    except Exception as exc:
        await session.rollback()
        await service.mark_error(business_id, f"Embedding sync failed: {exc}")
        await session.commit()
        raise
