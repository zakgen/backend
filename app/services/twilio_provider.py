from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException, status

from app.config import Settings, get_settings
from app.services.messaging_provider import AbstractMessagingProvider
from app.services.messaging_types import (
    ConnectionState,
    DeliveryStatusEvent,
    InboundMessageEvent,
    SendMessageCommand,
    SentMessageResult,
)
from app.utils.phones import normalize_phone_number


class TwilioMessagingProvider(AbstractMessagingProvider):
    provider_name = "twilio"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._validator: RequestValidator | None = None

    def _require_master_credentials(self) -> tuple[str, str]:
        if self.settings.twilio_account_sid is None or self.settings.twilio_auth_token is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Twilio credentials are not configured.",
            )
        return (
            self.settings.twilio_account_sid,
            self.settings.twilio_auth_token.get_secret_value(),
        )

    def _master_client(self) -> Client:
        account_sid, auth_token = self._require_master_credentials()
        return self._client_class()(account_sid, auth_token)

    def _subaccount_client(self, subaccount_sid: str) -> Client:
        account_sid, auth_token = self._require_master_credentials()
        return self._client_class()(account_sid, auth_token, account_sid=subaccount_sid)

    def _client_class(self):
        try:
            from twilio.rest import Client
        except ImportError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Twilio SDK is not installed.",
            ) from exc
        return Client

    def _validator_class(self):
        try:
            from twilio.request_validator import RequestValidator
        except ImportError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Twilio SDK is not installed.",
            ) from exc
        return RequestValidator

    def _twilio_exception_class(self):
        try:
            from twilio.base.exceptions import TwilioRestException
        except ImportError:
            class TwilioRestExceptionFallback(Exception):
                msg = "Twilio SDK is not installed."

            return TwilioRestExceptionFallback
        return TwilioRestException

    async def begin_connection(
        self,
        business_id: int,
        connect_payload: dict[str, Any],
        existing_connection: ConnectionState | None = None,
    ) -> ConnectionState:
        existing_config = dict((existing_connection or ConnectionState(0, "", "", "")).config or {})
        subaccount_sid = str(existing_config.get("subaccount_sid") or "")

        if not subaccount_sid:
            try:
                account = self._master_client().api.accounts.create(
                    friendly_name=f"ZakBot business {business_id} - {connect_payload['business_name']}"
                )
                subaccount_sid = account.sid
            except self._twilio_exception_class() as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Twilio subaccount creation failed: {getattr(exc, 'msg', str(exc))}",
                ) from exc

        config = {
            "provider": self.provider_name,
            "subaccount_sid": subaccount_sid,
            "sender_sid": existing_config.get("sender_sid"),
            "phone_number": normalize_phone_number(connect_payload.get("phone_number")),
            "whatsapp_number": normalize_phone_number(connect_payload.get("phone_number")),
            "business_name": connect_payload.get("business_name") or existing_config.get("business_name"),
            "onboarding_status": "pending_admin",
            "last_webhook_validation_at": existing_config.get("last_webhook_validation_at"),
        }
        metrics = {
            "received_messages_last_30_days": 0,
            "sent_messages_last_30_days": 0,
            "failed_messages_last_30_days": 0,
        }
        if existing_connection is not None:
            metrics.update(existing_connection.metrics)

        return ConnectionState(
            business_id=business_id,
            integration_type="whatsapp",
            status="disconnected",
            health="attention",
            config=config,
            metrics=metrics,
        )

    async def disconnect(self, connection_state: ConnectionState) -> ConnectionState:
        config = dict(connection_state.config)
        config["onboarding_status"] = config.get("onboarding_status") or "pending_admin"
        return ConnectionState(
            business_id=connection_state.business_id,
            integration_type=connection_state.integration_type,
            status="disconnected",
            health="attention",
            config=config,
            metrics=dict(connection_state.metrics),
        )

    async def send_text(self, command: SendMessageCommand) -> SentMessageResult:
        sender_phone = normalize_phone_number(
            command.config.get("whatsapp_number") or command.config.get("phone_number")
        )
        sender_sid = command.config.get("sender_sid")
        onboarding_status = command.config.get("onboarding_status")
        if not sender_phone or not sender_sid or onboarding_status != "connected":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Twilio WhatsApp integration is not finalized for this business.",
            )

        status_callback = None
        if self.settings.public_webhook_base_url:
            status_callback = (
                f"{self.settings.public_webhook_base_url.rstrip('/')}"
                "/webhooks/twilio/whatsapp/status"
            )

        try:
            message = self._subaccount_client(command.subaccount_sid).messages.create(
                from_=f"whatsapp:{sender_phone}",
                to=f"whatsapp:{normalize_phone_number(command.phone)}",
                body=command.text,
                status_callback=status_callback,
            )
        except self._twilio_exception_class() as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Twilio send failed: {getattr(exc, 'msg', str(exc))}",
            ) from exc

        return SentMessageResult(
            provider=self.provider_name,
            provider_message_sid=message.sid,
            provider_status=getattr(message, "status", None),
            raw_payload=self._message_to_payload(message),
            from_phone=sender_phone,
            to_phone=normalize_phone_number(command.phone),
            error_code=str(getattr(message, "error_code", "") or "") or None,
        )

    @staticmethod
    def _message_to_payload(message: Any) -> dict[str, Any]:
        if hasattr(message, "to_dict"):
            payload = message.to_dict()
            if isinstance(payload, dict):
                return payload

        fields = (
            "sid",
            "status",
            "account_sid",
            "api_version",
            "body",
            "date_created",
            "date_sent",
            "date_updated",
            "direction",
            "error_code",
            "error_message",
            "from_",
            "messaging_service_sid",
            "num_media",
            "num_segments",
            "price",
            "price_unit",
            "subresource_uris",
            "to",
            "uri",
        )
        payload: dict[str, Any] = {}
        for field in fields:
            value = getattr(message, field, None)
            if value is not None:
                payload[field] = str(value) if hasattr(value, "isoformat") else value
        return payload

    def validate_webhook(
        self, headers: Mapping[str, str], url: str, params: Mapping[str, Any]
    ) -> None:
        _, auth_token = self._require_master_credentials()
        validator_cls = self._validator_class()
        validator = self._validator or validator_cls(auth_token)
        self._validator = validator
        signature = headers.get("x-twilio-signature") or headers.get("X-Twilio-Signature")
        if not signature or not validator.validate(url, dict(params), signature):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid Twilio webhook signature.",
            )

    def parse_inbound_webhook(self, params: Mapping[str, Any]) -> InboundMessageEvent:
        return InboundMessageEvent(
            provider=self.provider_name,
            provider_message_sid=str(params.get("MessageSid") or params.get("SmsSid") or ""),
            from_phone=normalize_phone_number(str(params.get("From") or "")),
            to_phone=normalize_phone_number(str(params.get("To") or "")),
            text=str(params.get("Body") or ""),
            customer_name=str(params.get("ProfileName") or "") or None,
            raw_payload=dict(params),
        )

    def parse_status_webhook(self, params: Mapping[str, Any]) -> DeliveryStatusEvent:
        return DeliveryStatusEvent(
            provider=self.provider_name,
            provider_message_sid=str(params.get("MessageSid") or ""),
            provider_status=str(params.get("MessageStatus") or "") or None,
            error_code=str(params.get("ErrorCode") or "") or None,
            raw_payload=dict(params),
        )
