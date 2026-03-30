from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class FAQUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    business_id: int = Field(gt=0)
    id: int | None = None
    external_id: str | None = Field(default=None, max_length=255)
    question: str = Field(min_length=1)
    answer: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class FAQResponse(BaseModel):
    id: int
    business_id: int
    external_id: str | None = None
    question: str
    answer: str
    metadata: dict[str, Any] = Field(default_factory=dict)
