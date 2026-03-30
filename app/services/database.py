from __future__ import annotations

from collections.abc import AsyncIterator
from functools import lru_cache

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.config import Settings, get_settings


def _build_engine_url_and_options(db_url: str) -> tuple[str, dict]:
    url = make_url(db_url)
    query = dict(url.query)
    host = url.host or ""

    engine_options: dict = {
        "pool_pre_ping": True,
        "future": True,
    }

    is_supabase = "supabase.co" in host
    is_pooler = "pooler.supabase.com" in host or url.port == 6543

    connect_args: dict = {}

    sslmode = query.pop("sslmode", None)
    ssl_value = query.get("ssl")

    if is_supabase and sslmode is None and ssl_value is None:
        sslmode = "require"

    if sslmode is not None and ssl_value is None:
        connect_args["ssl"] = sslmode

    if connect_args:
        engine_options["connect_args"] = connect_args

    if "sslmode" in url.query:
        url = url.difference_update_query(["sslmode"])
        query = dict(url.query)

    if is_pooler:
        engine_options["poolclass"] = NullPool
        if "prepared_statement_cache_size" not in query:
            url = url.update_query_dict({"prepared_statement_cache_size": "0"})

    return url.render_as_string(hide_password=False), engine_options


@lru_cache
def get_engine() -> AsyncEngine:
    settings = get_settings()
    engine_url, engine_options = _build_engine_url_and_options(settings.db_url)
    return create_async_engine(engine_url, **engine_options)


@lru_cache
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=get_engine(),
        autoflush=False,
        expire_on_commit=False,
        class_=AsyncSession,
    )


async def get_session() -> AsyncIterator[AsyncSession]:
    session_factory = get_session_factory()
    async with session_factory() as session:
        yield session


def get_settings_dependency() -> Settings:
    return get_settings()
