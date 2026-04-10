from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


ProductVariantValue = str | dict[str, Any]


class ProductUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    business_id: int = Field(gt=0)
    id: int | None = None
    external_id: str | None = Field(default=None, max_length=255)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    price: float | None = Field(default=None, ge=0)
    currency: str = Field(default="MAD", min_length=3, max_length=10)
    category: str | None = Field(default=None, max_length=120)
    availability: str | None = Field(default="in_stock", max_length=50)
    variants: list[ProductVariantValue] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProductBulkItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    external_id: str | None = Field(default=None, max_length=255)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    price: float | None = Field(default=None, ge=0)
    currency: str = Field(default="MAD", min_length=3, max_length=10)
    category: str | None = Field(default=None, max_length=120)
    availability: str | None = Field(default="in_stock", max_length=50)
    variants: list[ProductVariantValue] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BulkProductUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    business_id: int = Field(gt=0)
    products: list[ProductBulkItem] = Field(min_length=1)


class ProductResponse(BaseModel):
    id: int
    business_id: int
    external_id: str | None = None
    name: str
    description: str | None = None
    price: float | None = None
    currency: str | None = None
    category: str | None = None
    availability: str | None = None
    variants: list[ProductVariantValue] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BulkProductUpsertResponse(BaseModel):
    business_id: int
    count: int
    products: list[ProductResponse]


StockStatus = Literal["in_stock", "low_stock", "out_of_stock"]


class ProductVariant(BaseModel):
    id: str
    name: str
    additional_price: float | None = None
    stock_status: StockStatus


class ProductVariantInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    name: str = Field(min_length=1, max_length=255)
    additional_price: float | None = None
    stock_status: StockStatus = "in_stock"


class Product(BaseModel):
    id: str
    business_id: int
    external_id: str | None = None
    name: str
    description: str
    category: str
    price: float | None = None
    currency: str
    stock_status: StockStatus
    variants: list[ProductVariant] = Field(default_factory=list)
    created_at: str
    updated_at: str


class ProductListResult(BaseModel):
    products: list[Product]
    total: int
    categories: list[str]


class DashboardProductCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    business_id: int = Field(gt=0)
    external_id: str | None = None
    name: str = Field(min_length=1, max_length=255)
    description: str = ""
    category: str = ""
    price: float | None = Field(default=None, ge=0)
    currency: str = "MAD"
    stock_status: StockStatus = "in_stock"
    variants: list[ProductVariantInput] = Field(default_factory=list)


class DashboardProductUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_id: str | None = None
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    category: str | None = None
    price: float | None = Field(default=None, ge=0)
    currency: str | None = None
    stock_status: StockStatus | None = None
    variants: list[ProductVariantInput] | None = None


class DashboardProductBulkItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_id: str | None = None
    name: str = Field(min_length=1, max_length=255)
    description: str = ""
    category: str = ""
    price: float | None = Field(default=None, ge=0)
    currency: str = "MAD"
    stock_status: StockStatus = "in_stock"
    variants: list[ProductVariantInput] = Field(default_factory=list)


class DashboardProductBulkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    business_id: int = Field(gt=0)
    products: list[DashboardProductBulkItem] = Field(min_length=1)
