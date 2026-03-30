from __future__ import annotations

from pathlib import Path
import sys

import psycopg2

from app.config import get_settings


def to_psycopg_dsn(db_url: str) -> str:
    if db_url.startswith("postgresql+asyncpg://"):
        return db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    if db_url.startswith("postgres+asyncpg://"):
        return db_url.replace("postgres+asyncpg://", "postgresql://", 1)
    return db_url


def main() -> int:
    settings = get_settings()
    dsn = to_psycopg_dsn(settings.db_url)
    migration_dir = Path(__file__).resolve().parents[1] / "migrations"
    migration_paths = sorted(migration_dir.glob("*.sql"))

    if not migration_paths:
        print("No migration files found.", file=sys.stderr)
        return 1

    with psycopg2.connect(dsn) as connection:
        with connection.cursor() as cursor:
            for path in migration_paths:
                print(f"Applying {path.name}...")
                cursor.execute(path.read_text())
        connection.commit()

    print(f"Applied {len(migration_paths)} migration files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
