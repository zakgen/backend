from __future__ import annotations

from datetime import UTC, datetime

from app.schemas.order_confirmation import OrderConfirmationActionRequest, StoreOrderIngestRequest
from app.services.order_confirmation_service import OrderConfirmationService


class FakeProvider:
    provider_name = "twilio"

    async def send_text(self, command):
        return type(
            "SentResult",
            (),
            {
                "provider": "twilio",
                "provider_message_sid": f"SM-{command.phone[-4:]}",
                "provider_status": "sent",
                "raw_payload": {"body": command.text},
                "from_phone": "+14155238886",
                "to_phone": command.phone,
                "error_code": None,
            },
        )()


class FakeLLMProvider:
    async def detect_language(self, *, message: str):
        return "darija", {"language_detection": {"language": "darija"}}


class FakeBusinessRepository:
    async def get_by_id(self, business_id: int):
        return {"id": business_id, "name": "Atlas Gadget Hub"}


class FakeIntegrationRepository:
    def __init__(self) -> None:
        self.sent_metrics = 0

    async def get_connection(self, business_id: int, integration_type: str):
        return {
            "business_id": business_id,
            "status": "connected",
            "config": {
                "provider": "twilio",
                "onboarding_status": "connected",
                "subaccount_sid": "AC123",
                "sender_sid": "PN123",
                "whatsapp_number": "+14155238886",
            },
            "metrics": {},
        }

    async def increment_whatsapp_metrics(self, business_id: int, **kwargs):
        self.sent_metrics += int(kwargs.get("sent_delta") or 0)


class FakeChatRepository:
    def __init__(self) -> None:
        self.messages: list[dict] = []
        self.analysis_updates: list[tuple[int, str | None, bool]] = []

    async def upsert_message(self, **kwargs):
        row = {
            "id": len(self.messages) + 1,
            **kwargs,
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
        self.messages.append(row)
        return row

    async def update_message_analysis(self, message_id: int, *, intent: str | None, needs_human: bool):
        self.analysis_updates.append((message_id, intent, needs_human))
        return {"id": message_id, "intent": intent, "needs_human": needs_human}


class FakeOrderRepository:
    def __init__(self) -> None:
        self.row = None

    async def upsert_order(self, *, business_id: int, payload: dict):
        self.row = {
            "id": 10,
            "business_id": business_id,
            "source_store": payload["source_store"],
            "external_order_id": payload["external_order_id"],
            "customer_name": payload["customer_name"],
            "customer_phone": payload["customer_phone"],
            "preferred_language": payload.get("preferred_language"),
            "total_amount": payload["total_amount"],
            "currency": payload["currency"],
            "payment_method": payload.get("payment_method"),
            "delivery_city": payload.get("delivery_city"),
            "delivery_address": payload.get("delivery_address"),
            "order_notes": payload.get("order_notes"),
            "items": payload["items"],
            "metadata": payload.get("metadata") or {},
            "raw_payload": payload.get("raw_payload") or {},
            "status": payload["status"],
            "confirmation_status": payload["confirmation_status"],
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
        return self.row

    async def get_by_id(self, business_id: int, order_id: int):
        assert self.row is not None
        return self.row

    async def update_order_status(self, *, business_id: int, order_id: int, status_value: str, confirmation_status: str, metadata: dict | None = None):
        assert self.row is not None
        self.row = {
            **self.row,
            "status": status_value,
            "confirmation_status": confirmation_status,
            "metadata": metadata or {},
            "updated_at": datetime.now(UTC),
        }
        return self.row


class FakeOrderConfirmationRepository:
    def __init__(self) -> None:
        self.session = None
        self.events: list[dict] = []

    async def find_latest_by_order(self, business_id: int, order_id: int):
        return self.session

    async def create_session(self, **kwargs):
        self.session = {
            "id": 21,
            "business_id": kwargs["business_id"],
            "order_id": kwargs["order_id"],
            "phone": kwargs["phone"],
            "customer_name": kwargs["customer_name"],
            "preferred_language": kwargs["preferred_language"],
            "status": kwargs["status_value"],
            "needs_human": kwargs["needs_human"],
            "last_detected_intent": kwargs["last_detected_intent"],
            "started_at": datetime.now(UTC),
            "last_customer_message_at": None,
            "confirmed_at": None,
            "declined_at": None,
            "updated_at": datetime.now(UTC),
            "structured_snapshot": kwargs["structured_snapshot"],
        }
        return self.session

    async def add_event(self, **kwargs):
        event = {
            "id": len(self.events) + 1,
            "session_id": kwargs["session_id"],
            "event_type": kwargs["event_type"],
            "payload": kwargs["payload"],
            "created_at": datetime.now(UTC),
        }
        self.events.append(event)
        return event

    async def update_session(self, session_id: int, payload: dict):
        assert self.session is not None
        self.session = {
            **self.session,
            **payload,
            "updated_at": datetime.now(UTC),
        }
        return self.session

    async def find_active_session(self, business_id: int, phone: str):
        return self.session

    async def get_session(self, business_id: int, session_id: int):
        return self.session

    async def list_events(self, session_id: int, limit: int = 50):
        return self.events

    async def list_sessions(self, business_id: int, *, status_value: str | None = None, limit: int = 50):
        if self.session is None:
            return []
        return [self.session]


def _build_service() -> tuple[OrderConfirmationService, FakeChatRepository, FakeOrderRepository, FakeOrderConfirmationRepository]:
    session = type("DummyRepoSession", (), {"db": None})()
    service = OrderConfirmationService(
        session=session,
        messaging_provider=FakeProvider(),
        llm_provider=FakeLLMProvider(),
    )
    service.business_repository = FakeBusinessRepository()
    service.integration_repository = FakeIntegrationRepository()
    chat_repository = FakeChatRepository()
    service.chat_repository = chat_repository
    order_repository = FakeOrderRepository()
    service.order_repository = order_repository
    confirmation_repository = FakeOrderConfirmationRepository()
    service.order_confirmation_repository = confirmation_repository
    return service, chat_repository, order_repository, confirmation_repository


def test_ingest_store_order_creates_session_and_sends_confirmation() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()

    import asyncio

    result = asyncio.run(
        service.ingest_store_order(
            2,
            StoreOrderIngestRequest(
                source_store="generic",
                external_order_id="WC-1001",
                customer_name="Lina",
                customer_phone="+212600000001",
                preferred_language="french",
                total_amount=3499,
                currency="MAD",
                delivery_city="Casablanca",
                delivery_address="Maarif",
                items=[{"product_name": "Redmi Note 13", "quantity": 1}],
            ),
        )
    )

    assert result["confirmation_message_sent"] is True
    assert order_repository.row["confirmation_status"] == "awaiting_customer"
    assert confirmation_repository.session["status"] == "awaiting_customer"
    assert len(chat_repository.messages) == 1
    assert "Répondez 1 pour confirmer" in chat_repository.messages[0]["text"]


def test_handle_inbound_confirm_marks_session_confirmed() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()
    confirmation_repository.session = {
        "id": 21,
        "business_id": 2,
        "order_id": 10,
        "phone": "+212600000001",
        "customer_name": "Lina",
        "preferred_language": "french",
        "status": "awaiting_customer",
        "needs_human": False,
        "last_detected_intent": "order_confirmation_pending",
        "started_at": datetime.now(UTC),
        "last_customer_message_at": None,
        "confirmed_at": None,
        "declined_at": None,
        "updated_at": datetime.now(UTC),
        "structured_snapshot": {},
    }
    order_repository.row = {
        "id": 10,
        "business_id": 2,
        "source_store": "generic",
        "external_order_id": "WC-1001",
        "customer_name": "Lina",
        "customer_phone": "+212600000001",
        "preferred_language": "french",
        "total_amount": 3499,
        "currency": "MAD",
        "payment_method": None,
        "delivery_city": "Casablanca",
        "delivery_address": "Maarif",
        "order_notes": None,
        "items": [{"product_name": "Redmi Note 13", "quantity": 1}],
        "metadata": {},
        "raw_payload": {},
        "status": "pending_confirmation",
        "confirmation_status": "awaiting_customer",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }

    import asyncio

    handled = asyncio.run(
        service.handle_inbound_message(
            connection={
                "business_id": 2,
                "config": {
                    "provider": "twilio",
                    "onboarding_status": "connected",
                    "subaccount_sid": "AC123",
                    "sender_sid": "PN123",
                    "whatsapp_number": "+14155238886",
                },
            },
            inbound_row={"id": 55, "phone": "+212600000001", "text": "1"},
        )
    )

    assert handled is True
    assert confirmation_repository.session["status"] == "confirmed"
    assert order_repository.row["confirmation_status"] == "confirmed"
    assert chat_repository.analysis_updates == [(55, "autre", False)]
    assert "tconfirmat" in chat_repository.messages[0]["text"].lower()


def test_apply_action_resend_reopens_session() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()
    confirmation_repository.session = {
        "id": 21,
        "business_id": 2,
        "order_id": 10,
        "phone": "+212600000001",
        "customer_name": "Lina",
        "preferred_language": "french",
        "status": "human_requested",
        "needs_human": True,
        "last_detected_intent": "request_human",
        "started_at": datetime.now(UTC),
        "last_customer_message_at": None,
        "confirmed_at": None,
        "declined_at": None,
        "updated_at": datetime.now(UTC),
        "structured_snapshot": {},
    }
    order_repository.row = {
        "id": 10,
        "business_id": 2,
        "source_store": "generic",
        "external_order_id": "WC-1001",
        "customer_name": "Lina",
        "customer_phone": "+212600000001",
        "preferred_language": "french",
        "total_amount": 3499,
        "currency": "MAD",
        "payment_method": None,
        "delivery_city": "Casablanca",
        "delivery_address": "Maarif",
        "order_notes": None,
        "items": [{"product_name": "Redmi Note 13", "quantity": 1}],
        "metadata": {},
        "raw_payload": {},
        "status": "needs_review",
        "confirmation_status": "human_requested",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }

    import asyncio

    detail = asyncio.run(
        service.apply_action(
            2,
            21,
            OrderConfirmationActionRequest(action="resend"),
        )
    )

    assert detail["status"] == "awaiting_customer"
    assert order_repository.row["confirmation_status"] == "awaiting_customer"
    assert len(chat_repository.messages) == 1
