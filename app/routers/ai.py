from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.ai import AIReplyRequest, AIReplyResponse, AIRunDetail, AIRunSummary
from app.services.ai_reply_service import AIReplyService
from app.services.database import get_session


router = APIRouter(prefix="/business", tags=["ai"])


@router.post(
    "/{business_id}/ai/reply",
    response_model=AIReplyResponse,
    status_code=status.HTTP_200_OK,
)
async def generate_ai_reply(
    business_id: int,
    payload: AIReplyRequest,
    session: AsyncSession = Depends(get_session),
) -> AIReplyResponse:
    service = AIReplyService(session=session)
    response = await service.generate_preview(business_id, payload)
    await session.commit()
    return response


@router.get(
    "/{business_id}/ai/runs",
    response_model=list[AIRunSummary],
    status_code=status.HTTP_200_OK,
)
async def list_ai_runs(
    business_id: int,
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[AIRunSummary]:
    service = AIReplyService(session=session)
    return await service.list_runs(business_id, limit=limit)


@router.get(
    "/{business_id}/ai/runs/{run_id}",
    response_model=AIRunDetail,
    status_code=status.HTTP_200_OK,
)
async def get_ai_run(
    business_id: int,
    run_id: int,
    session: AsyncSession = Depends(get_session),
) -> AIRunDetail:
    service = AIReplyService(session=session)
    return await service.get_run(business_id, run_id)
