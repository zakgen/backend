from __future__ import annotations

from functools import lru_cache
from pathlib import Path

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
    database_backend: str = "postgres"
    db_url: str | None = Field(default=None, description="Postgres connection string")
    mongo_url: str | None = None
    mongo_database_name: str = "zakbot"

    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    search_min_score: float = 0.45
    llm_provider: str = "openai"
    openai_chat_model: str = "gpt-4.1-mini"
    ai_reply_max_context_items: int = 6
    ai_reply_max_history_messages: int = 8
    ai_reply_confidence_threshold: float = 0.7
    ai_auto_reply_enabled_default: bool = True
    ai_reply_audit_log_enabled: bool = True
    ai_reply_audit_log_path: str = str(Path("logs") / "ai_reply_audit.log")
    ai_reply_audit_max_bytes: int = 5 * 1024 * 1024
    ai_reply_audit_backup_count: int = 5

    twilio_account_sid: str | None = None
    twilio_auth_token: SecretStr | None = None
    public_webhook_base_url: str | None = None
    app_encryption_key: SecretStr | None = None
    shopify_api_key: str | None = None
    shopify_api_secret: SecretStr | None = None
    shopify_app_base_url: str | None = None
    shopify_scopes: str = "read_orders,write_orders"
    shopify_api_version: str = "2025-07"


@lru_cache
def get_settings() -> Settings:
    return Settings()
