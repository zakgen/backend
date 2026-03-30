from __future__ import annotations

from functools import lru_cache

from fastapi import HTTPException, status

from app.config import Settings, get_settings


def _require_motor_client():
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except ImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MongoDB backend requires the motor package to be installed.",
        ) from exc
    return AsyncIOMotorClient


@lru_cache
def get_mongo_client():
    settings = get_settings()
    if not settings.mongo_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MONGO_URL is required when DATABASE_BACKEND is set to mongo.",
        )
    client_cls = _require_motor_client()
    return client_cls(settings.mongo_url)


def get_mongo_database(settings: Settings | None = None):
    resolved = settings or get_settings()
    return get_mongo_client()[resolved.mongo_database_name]
