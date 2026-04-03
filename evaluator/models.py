from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, computed_field


DIMENSION_FIELDS = (
    "relevance",
    "accuracy",
    "language_match",
    "completeness",
    "tone",
    "hallucination_risk",
)


class QueryRecord(BaseModel):
    id: str
    language: str
    topic: str
    query_text: str
    expected_intent: str
    difficulty: str = "medium"
    edge_case_tags: list[str] = Field(default_factory=list)


class ServiceResult(BaseModel):
    query_id: str
    query_text: str
    language: str
    topic: str
    expected_intent: str
    status: str
    http_status: int | None = None
    latency_ms: float
    ai_response: str | None = None
    raw_payload: dict[str, Any] | None = None
    error: str | None = None
    service_language: str | None = None
    service_intent: str | None = None
    service_decision: str | None = None


class JudgeScores(BaseModel):
    relevance: int = Field(ge=1, le=5)
    accuracy: int = Field(ge=1, le=5)
    language_match: int = Field(ge=1, le=5)
    completeness: int = Field(ge=1, le=5)
    tone: int = Field(ge=1, le=5)
    hallucination_risk: int = Field(ge=1, le=5)
    failure_tags: list[str] = Field(default_factory=list)
    reasoning: str

    @computed_field
    @property
    def overall_score(self) -> float:
        total = sum(getattr(self, field) for field in DIMENSION_FIELDS)
        return round(total / len(DIMENSION_FIELDS), 2)

    @computed_field
    @property
    def passed(self) -> bool:
        scores = [getattr(self, field) for field in DIMENSION_FIELDS]
        return self.overall_score >= 4.0 and min(scores) >= 3


class ScoredResult(BaseModel):
    query: QueryRecord
    result: ServiceResult
    scores: JudgeScores


class AggregateBreakdown(BaseModel):
    count: int
    pass_rate: float
    average_overall_score: float
    average_latency_ms: float
    dimension_averages: dict[str, float]


class EvalReport(BaseModel):
    generated_at: str
    config: dict[str, Any]
    summary: dict[str, Any]
    breakdown_by_language: dict[str, AggregateBreakdown]
    breakdown_by_topic: dict[str, AggregateBreakdown]
    best_responses: list[dict[str, Any]]
    worst_responses: list[dict[str, Any]]
    failure_patterns: list[dict[str, Any]]
    recommendations: list[str]
    results: list[ScoredResult]

