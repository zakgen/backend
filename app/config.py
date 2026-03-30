from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "ZakBot RAG API"
    environment: str = "development"
    debug: bool = False
    port: int = 8000
    log_level: str = "INFO"
    cors_allow_origins: str = "http://localhost:3000,http://127.0.0.1:3000"

    openai_api_key: SecretStr | None = None
    supabase_url: str | None = None
    supabase_service_role_key: SecretStr | None = None
    db_url: str = Field(..., description="Supabase Postgres connection string")

    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    search_min_score: float = 0.45

    twilio_account_sid: str | None = None
    twilio_auth_token: SecretStr | None = None
    public_webhook_base_url: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
