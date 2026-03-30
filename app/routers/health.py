from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, status
from sqlalchemy import text

from app.config import get_settings
from app.services.database import get_session


router = APIRouter(tags=["health"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check(session: Any = Depends(get_session)) -> dict:
    settings = get_settings()
    if settings.database_backend == "mongo":
        result = await session.db.command("ping")
        return {
            "status": "ok",
            "database": "ok" if result.get("ok") == 1 else "error",
            "backend": "mongo",
            "vector_search": "application_side_cosine_similarity",
        }

    await session.execute(text("SELECT 1"))
    vector_result = await session.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')")
    )
    return {
        "status": "ok",
        "database": "ok",
        "backend": "postgres",
        "pgvector_enabled": bool(vector_result.scalar()),
    }
