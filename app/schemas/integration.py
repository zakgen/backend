from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.business import SetupChecklist


IntegrationStatus = Literal["connected", "disconnected"]
IntegrationHealth = Literal["healthy", "attention"]
CommercePlatform = Literal["youcan", "shopify", "woocommerce", "zid"]


class WhatsAppIntegration(BaseModel):
    phone_number: str
    business_name: str
    status: IntegrationStatus
    health: IntegrationHealth
    received_messages_last_30_days: int
    last_activity_at: str | None = None


class CommerceIntegration(BaseModel):
    id: CommercePlatform
    name: str
    description: str
    status: IntegrationStatus
    imported_products: int
    last_sync_at: str | None = None
    shop_domain: str | None = None
    last_activity_at: str | None = None
    last_sync_back_at: str | None = None
    webhook_status: str | None = None


class ComingSoonIntegration(BaseModel):
    id: str
    name: str
    description: str


class IntegrationsData(BaseModel):
    checklist: SetupChecklist
    whatsapp: WhatsAppIntegration
    platforms: list[CommerceIntegration]
    coming_soon: list[ComingSoonIntegration]


class WhatsAppConnectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phone_number: str = Field(min_length=6)
    business_name: str = Field(min_length=1)


class WhatsAppTestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = Field(default="ZakBot WhatsApp integration test.", min_length=1)


class WhatsAppTestResponse(BaseModel):
    success: bool
    message: str
    integration: WhatsAppIntegration


class ShopifyConnectResponse(BaseModel):
    auth_url: str
