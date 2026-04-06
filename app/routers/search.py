from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.search import SearchRequest, SearchResponse
from app.services.auth import AuthenticatedUser, ensure_user_can_access_business, require_authenticated_user
from app.services.database import get_session
from app.services.embedding_service import EmbeddingService
from app.services.search_service import SearchService


router = APIRouter(tags=["search"])


@router.post("/search", response_model=SearchResponse, status_code=status.HTTP_200_OK)
async def search_context(
    payload: SearchRequest,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> SearchResponse:
    await ensure_user_can_access_business(
        session=session,
        current_user=current_user,
        business_id=payload.business_id,
    )
    service = SearchService(session=session, embedding_service=EmbeddingService())
    return await service.search(payload)
