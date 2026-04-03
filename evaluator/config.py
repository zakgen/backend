from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT = Path(__file__).resolve().parents[1]


class EvalSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    base_url: str = Field(default="http://localhost:8000")
    business_id: int = 1
    request_timeout_seconds: float = 20.0
    max_concurrency: int = 5
    judge_max_concurrency: int = 3
    judge_model: str = "gpt-4.1-mini"
    openai_api_key: SecretStr | None = None
    seed_eval_data: bool = True
    cleanup_seed_data: bool = True
    database_backend: str = "postgres"

    business_profile_path: Path = ROOT / "data" / "business_profile.json"
    query_templates_path: Path = ROOT / "data" / "query_templates.json"
    generated_queries_path: Path = ROOT / "queries" / "generated_queries.json"
    raw_results_path: Path = ROOT / "reports" / "raw_results.json"
    scored_results_path: Path = ROOT / "reports" / "scored_results.json"
    report_json_path: Path = ROOT / "reports" / "report.json"
    report_md_path: Path = ROOT / "reports" / "report.md"

    @property
    def normalized_base_url(self) -> str:
        return self.base_url.rstrip("/")
