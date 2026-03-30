from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import asyncpg
from sqlalchemy.engine import make_url

from app.config import get_settings
from app.services.database import _build_engine_url_and_options


def _connect_kwargs_from_db_url(db_url: str) -> dict:
    engine_url, engine_options = _build_engine_url_and_options(db_url)
    url = make_url(engine_url)
    connect_args = dict(engine_options.get("connect_args") or {})
    connect_kwargs = {
        "host": url.host,
        "port": url.port,
        "user": url.username,
        "password": url.password,
        "database": url.database,
    }
    connect_kwargs.update(connect_args)
    for key, value in dict(url.query).items():
        connect_kwargs.setdefault(key, value)
    return {key: value for key, value in connect_kwargs.items() if value is not None}


async def main() -> int:
    settings = get_settings()
    connect_kwargs = _connect_kwargs_from_db_url(settings.db_url)
    migration_dir = Path(__file__).resolve().parents[1] / "migrations"
    migration_paths = sorted(migration_dir.glob("*.sql"))

    if not migration_paths:
        print("No migration files found.", file=sys.stderr)
        return 1

    connection = await asyncpg.connect(**connect_kwargs)
    try:
        for path in migration_paths:
            print(f"Applying {path.name}...")
            await connection.execute(path.read_text())
    finally:
        await connection.close()

    print(f"Applied {len(migration_paths)} migration files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
