from __future__ import annotations

from types import SimpleNamespace

from app.config import Settings
from app.services.messaging_types import ConnectionState, SendMessageCommand
from app.services.twilio_provider import TwilioMessagingProvider


class FakeAccountsAPI:
    def create(self, *, friendly_name: str) -> SimpleNamespace:
        return SimpleNamespace(sid="ACSUBACCOUNT123", friendly_name=friendly_name)


class FakeMasterClient:
    def __init__(self) -> None:
        self.api = SimpleNamespace(accounts=FakeAccountsAPI())


class FakeMessagesAPI:
    def create(self, **kwargs) -> SimpleNamespace:
        return SimpleNamespace(
            sid="MM123",
            status="queued",
            error_code=None,
            to_dict=lambda: {
                "sid": "MM123",
                "status": "queued",
                "to": kwargs["to"],
                "from_": kwargs["from_"],
                "body": kwargs["body"],
            },
        )


class FakeSubaccountClient:
    def __init__(self) -> None:
        self.messages = FakeMessagesAPI()


def test_begin_connection_creates_pending_twilio_state() -> None:
    provider = TwilioMessagingProvider(
        Settings(
            db_url="postgresql+asyncpg://postgres:pass@db.example.com:5432/postgres",
            twilio_account_sid="ACMASTER",
            twilio_auth_token="secret",
        )
    )
    provider._master_client = lambda: FakeMasterClient()  # type: ignore[method-assign]

    result = __import__("asyncio").run(
        provider.begin_connection(
            7,
            {"phone_number": "whatsapp:+212600000001", "business_name": "Boutique Lina"},
            None,
        )
    )

    assert result.status == "disconnected"
    assert result.health == "attention"
    assert result.config["provider"] == "twilio"
    assert result.config["subaccount_sid"] == "ACSUBACCOUNT123"
    assert result.config["onboarding_status"] == "pending_admin"
    assert result.config["whatsapp_number"] == "+212600000001"


def test_parse_twilio_webhooks_normalizes_numbers() -> None:
    provider = TwilioMessagingProvider(
        Settings(
            db_url="postgresql+asyncpg://postgres:pass@db.example.com:5432/postgres",
            twilio_account_sid="ACMASTER",
            twilio_auth_token="secret",
        )
    )

    inbound = provider.parse_inbound_webhook(
        {
            "MessageSid": "SM123",
            "From": "whatsapp:+212600000001",
            "To": "whatsapp:+212700000001",
            "Body": "Salam",
            "ProfileName": "Lina",
        }
    )
    status_event = provider.parse_status_webhook(
        {"MessageSid": "SM123", "MessageStatus": "delivered", "ErrorCode": ""}
    )

    assert inbound.from_phone == "+212600000001"
    assert inbound.to_phone == "+212700000001"
    assert inbound.customer_name == "Lina"
    assert status_event.provider_message_sid == "SM123"
    assert status_event.provider_status == "delivered"
    assert status_event.error_code is None


def test_send_text_serializes_message_without_private_properties() -> None:
    provider = TwilioMessagingProvider(
        Settings(
            db_url="postgresql+asyncpg://postgres:pass@db.example.com:5432/postgres",
            twilio_account_sid="ACMASTER",
            twilio_auth_token="secret",
            public_webhook_base_url="https://example.ngrok-free.app",
        )
    )
    provider._subaccount_client = lambda _sid: FakeSubaccountClient()  # type: ignore[method-assign]

    result = __import__("asyncio").run(
        provider.send_text(
            SendMessageCommand(
                business_id=7,
                phone="+212600000001",
                text="Salam",
                config={
                    "whatsapp_number": "+14155238886",
                    "sender_sid": "sandbox",
                    "onboarding_status": "connected",
                },
                subaccount_sid="ACSUBACCOUNT123",
            )
        )
    )

    assert result.provider_message_sid == "MM123"
    assert result.provider_status == "queued"
    assert result.raw_payload["sid"] == "MM123"
    assert result.raw_payload["to"] == "whatsapp:+212600000001"
