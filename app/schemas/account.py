from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class MyBusinessCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    city: str | None = Field(default=None, max_length=120)
    shipping_policy: str | None = None
    delivery_zones: list[str] = Field(default_factory=list)
    payment_methods: list[str] = Field(default_factory=list)
    profile_metadata: dict[str, Any] = Field(default_factory=dict)


class MyBusinessesResponse(BaseModel):
    businesses: list["BusinessResponse"] = Field(default_factory=list)
    current_business_id: int | None = None


from app.schemas.business import BusinessResponse

MyBusinessesResponse.model_rebuild()
