from __future__ import annotations

from app.services import messaging_service as messaging_service_module
from app.services.messaging_service import MessagingService


class FakeProvider:
    provider_name = "twilio"

    def validate_webhook(self, headers, url, params) -> None:
        return None

    def parse_inbound_webhook(self, params):
        return type(
            "InboundEvent",
            (),
            {
                "to_phone": "+14155238886",
                "from_phone": "+212600000001",
                "text": "Kayn livraison l Rabat ?",
                "customer_name": "Lina",
                "provider": "twilio",
                "provider_message_sid": "SM123",
                "raw_payload": dict(params),
            },
        )()


class FakeIntegrationRepository:
    def __init__(self) -> None:
        self.metrics_called = False

    async def find_whatsapp_connection(self, sender_phone: str, subaccount_sid: str):
        return {
            "business_id": 2,
            "status": "connected",
            "health": "healthy",
            "config": {
                "provider": "twilio",
                "onboarding_status": "connected",
                "whatsapp_number": "+14155238886",
                "ai_auto_reply_enabled": True,
            },
            "metrics": {},
            "last_synced_at": None,
        }

    async def upsert_connection(self, **kwargs):
        return kwargs

    async def increment_whatsapp_metrics(self, *args, **kwargs):
        self.metrics_called = True


class FakeChatRepository:
    def __init__(self) -> None:
        self.analysis_updates: list[tuple[int, str | None, bool]] = []

    async def upsert_message(self, **kwargs):
        return {
            "id": 55,
            "business_id": kwargs["business_id"],
            "phone": kwargs["phone"],
            "customer_name": kwargs["customer_name"],
            "text": kwargs["text"],
            "direction": kwargs["direction"],
            "intent": kwargs["intent"],
            "needs_human": kwargs["needs_human"],
            "is_read": kwargs["is_read"],
            "provider": kwargs["provider"],
            "provider_message_sid": kwargs["provider_message_sid"],
            "provider_status": kwargs["provider_status"],
            "error_code": kwargs["error_code"],
            "raw_payload": kwargs["raw_payload"],
            "created_at": None,
            "updated_at": None,
        }

    async def update_message_analysis(self, message_id: int, *, intent: str | None, needs_human: bool):
        self.analysis_updates.append((message_id, intent, needs_human))
        return {"id": message_id, "intent": intent, "needs_human": needs_human}


class FakeOrderConfirmationRepository:
    def __init__(self, latest_session=None) -> None:
        self.latest_session = latest_session

    async def find_latest_by_phone(self, business_id: int, phone: str):
        return self.latest_session


def test_handle_inbound_webhook_triggers_ai_processing(monkeypatch) -> None:
    ai_calls: list[tuple[dict, dict]] = []

    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def handle_inbound_message(self, *, connection, inbound_row):
            return False

    class FakeAIReplyService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def process_inbound_message(self, *, connection, inbound_row):
            ai_calls.append((connection, inbound_row))
            return None

    service = MessagingService(
        session=type("DummyRepoSession", (), {"db": None})(),
        provider=FakeProvider(),
    )
    service.integration_repository = FakeIntegrationRepository()
    service.chat_repository = FakeChatRepository()
    service.order_confirmation_repository = FakeOrderConfirmationRepository()
    monkeypatch.setattr(
        messaging_service_module, "OrderConfirmationService", FakeOrderConfirmationService
    )
    monkeypatch.setattr(messaging_service_module, "AIReplyService", FakeAIReplyService)

    import asyncio

    row = asyncio.run(
        service.handle_inbound_webhook(
            url="https://example.com/webhooks/twilio/whatsapp/inbound",
            headers={},
            params={"MessageSid": "SM123", "AccountSid": "AC123"},
        )
    )

    assert row["id"] == 55
    assert ai_calls
    assert ai_calls[0][0]["business_id"] == 2
    assert ai_calls[0][1]["id"] == 55


def test_handle_inbound_webhook_marks_message_for_human_when_ai_fails(monkeypatch) -> None:
    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def handle_inbound_message(self, *, connection, inbound_row):
            return False

    class FakeAIReplyService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def process_inbound_message(self, *, connection, inbound_row):
            raise RuntimeError("model failure")

    service = MessagingService(
        session=type("DummyRepoSession", (), {"db": None})(),
        provider=FakeProvider(),
    )
    service.integration_repository = FakeIntegrationRepository()
    chat_repository = FakeChatRepository()
    service.chat_repository = chat_repository
    service.order_confirmation_repository = FakeOrderConfirmationRepository()
    monkeypatch.setattr(
        messaging_service_module, "OrderConfirmationService", FakeOrderConfirmationService
    )
    monkeypatch.setattr(messaging_service_module, "AIReplyService", FakeAIReplyService)

    import asyncio

    row = asyncio.run(
        service.handle_inbound_webhook(
            url="https://example.com/webhooks/twilio/whatsapp/inbound",
            headers={},
            params={"MessageSid": "SM123", "AccountSid": "AC123"},
        )
    )

    assert row["id"] == 55
    assert chat_repository.analysis_updates == [(55, None, True)]


def test_handle_inbound_webhook_uses_order_confirmation_before_ai(monkeypatch) -> None:
    ai_calls: list[tuple[dict, dict]] = []
    confirmation_calls: list[tuple[dict, dict]] = []

    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def handle_inbound_message(self, *, connection, inbound_row):
            confirmation_calls.append((connection, inbound_row))
            return True

    class FakeAIReplyService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def process_inbound_message(self, *, connection, inbound_row):
            ai_calls.append((connection, inbound_row))
            return None

    service = MessagingService(
        session=type("DummyRepoSession", (), {"db": None})(),
        provider=FakeProvider(),
    )
    service.integration_repository = FakeIntegrationRepository()
    service.chat_repository = FakeChatRepository()
    service.order_confirmation_repository = FakeOrderConfirmationRepository()
    monkeypatch.setattr(
        messaging_service_module, "OrderConfirmationService", FakeOrderConfirmationService
    )
    monkeypatch.setattr(messaging_service_module, "AIReplyService", FakeAIReplyService)

    import asyncio

    row = asyncio.run(
        service.handle_inbound_webhook(
            url="https://example.com/webhooks/twilio/whatsapp/inbound",
            headers={},
            params={"MessageSid": "SM123", "AccountSid": "AC123"},
        )
    )

    assert row["id"] == 55
    assert confirmation_calls
    assert not ai_calls


def test_handle_inbound_webhook_skips_ai_after_declined_order_session(monkeypatch) -> None:
    ai_calls: list[tuple[dict, dict]] = []

    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def handle_inbound_message(self, *, connection, inbound_row):
            return False

    class FakeAIReplyService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def process_inbound_message(self, *, connection, inbound_row):
            ai_calls.append((connection, inbound_row))
            return None

    service = MessagingService(
        session=type("DummyRepoSession", (), {"db": None})(),
        provider=FakeProvider(),
    )
    service.integration_repository = FakeIntegrationRepository()
    service.chat_repository = FakeChatRepository()
    service.order_confirmation_repository = FakeOrderConfirmationRepository(
        latest_session={"id": 21, "status": "declined"}
    )
    monkeypatch.setattr(
        messaging_service_module, "OrderConfirmationService", FakeOrderConfirmationService
    )
    monkeypatch.setattr(messaging_service_module, "AIReplyService", FakeAIReplyService)

    import asyncio

    row = asyncio.run(
        service.handle_inbound_webhook(
            url="https://example.com/webhooks/twilio/whatsapp/inbound",
            headers={},
            params={"MessageSid": "SM123", "AccountSid": "AC123"},
        )
    )

    assert row["id"] == 55
    assert not ai_calls


def test_handle_inbound_webhook_skips_ai_after_confirmed_order_session(monkeypatch) -> None:
    ai_calls: list[tuple[dict, dict]] = []

    class FakeOrderConfirmationService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def handle_inbound_message(self, *, connection, inbound_row):
            return False

    class FakeAIReplyService:
        def __init__(self, *, session, messaging_provider) -> None:
            self.session = session
            self.messaging_provider = messaging_provider

        async def process_inbound_message(self, *, connection, inbound_row):
            ai_calls.append((connection, inbound_row))
            return None

    service = MessagingService(
        session=type("DummyRepoSession", (), {"db": None})(),
        provider=FakeProvider(),
    )
    service.integration_repository = FakeIntegrationRepository()
    service.chat_repository = FakeChatRepository()
    service.order_confirmation_repository = FakeOrderConfirmationRepository(
        latest_session={"id": 22, "status": "confirmed"}
    )
    monkeypatch.setattr(
        messaging_service_module, "OrderConfirmationService", FakeOrderConfirmationService
    )
    monkeypatch.setattr(messaging_service_module, "AIReplyService", FakeAIReplyService)

    import asyncio

    row = asyncio.run(
        service.handle_inbound_webhook(
            url="https://example.com/webhooks/twilio/whatsapp/inbound",
            headers={},
            params={"MessageSid": "SM123", "AccountSid": "AC123"},
        )
    )

    assert row["id"] == 55
    assert not ai_calls
