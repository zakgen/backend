from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.database import get_session


router = APIRouter(tags=["health"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check(session: AsyncSession = Depends(get_session)) -> dict:
    await session.execute(text("SELECT 1"))
    vector_result = await session.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')")
    )
    return {
        "status": "ok",
        "database": "ok",
        "pgvector_enabled": bool(vector_result.scalar()),
    }
