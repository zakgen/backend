from __future__ import annotations

from datetime import UTC, datetime

from app.schemas.order_confirmation import (
    OrderConfirmationActionRequest,
    OrderSessionInterpretation,
    StoreOrderIngestRequest,
)
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
    def __init__(self) -> None:
        self.detect_calls: list[str] = []

    async def detect_language(self, *, message: str):
        self.detect_calls.append(message)
        lowered = message.lower()
        if any(token in lowered for token in ("bonjour", "merci", "adresse", "livraison", "quantité", "changez")):
            return "french", {"language_detection": {"language": "french"}}
        if any(token in lowered for token in ("bghit", "walakin", "wach", "nبدل", "jawb", "salam")):
            return "darija", {"language_detection": {"language": "darija"}}
        return "english", {"language_detection": {"language": "english"}}

    async def interpret_order_session(
        self,
        *,
        customer_message: str,
        preferred_language: str | None,
        session_status: str,
        order_snapshot: dict,
    ):
        lowered = customer_message.lower()
        if lowered.strip() == "change it":
            return (
                OrderSessionInterpretation(
                    language="english",
                    primary_action="edit_request",
                    confidence=0.35,
                    needs_human=True,
                    reply_summary="Customer asks to change something but does not specify what.",
                ),
                {},
            )
        if "livraison" in lowered or "delivery" in lowered:
            return (
                OrderSessionInterpretation(
                    language="french",
                    primary_action="delivery_question",
                    confidence=0.9,
                    question_type="delivery_status",
                    reply_summary="Customer asks about delivery details.",
                ),
                {},
            )
        if "address" in lowered or "adresse" in lowered:
            return (
                OrderSessionInterpretation(
                    language="french",
                    primary_action="edit_request",
                    secondary_actions=["confirm"],
                    confidence=0.92,
                    edits=[{"field": "delivery_address", "value": "Hay Hassani, Casablanca"}],
                    reply_summary="Customer confirms the order but wants to change the address.",
                ),
                {},
            )
        if "quantity" in lowered:
            return (
                OrderSessionInterpretation(
                    language="english",
                    primary_action="edit_request",
                    secondary_actions=["confirm"],
                    confidence=0.9,
                    edits=[{"field": "quantity", "value": "3"}],
                    reply_summary="Customer requests to change quantity to 3 and confirms the order.",
                ),
                {},
            )
        if "white" in lowered:
            return (
                OrderSessionInterpretation(
                    language="english",
                    primary_action="edit_request",
                    secondary_actions=["confirm"],
                    confidence=0.9,
                    edits=[{"field": "variant", "value": "White"}],
                    reply_summary="Customer requests to change color variant to white, confirms order with edits pending.",
                ),
                {},
            )
        return (
            OrderSessionInterpretation(
                language="darija",
                primary_action="support_request",
                confidence=0.6,
                needs_human=True,
                reply_summary="Fallback support request.",
            ),
            {},
        )


class FakeBusinessRepository:
    def __init__(self, default_language: str | None = "french") -> None:
        self.default_language = default_language

    async def get_by_id(self, business_id: int):
        profile_metadata = {}
        if self.default_language is not None:
            profile_metadata["default_language"] = self.default_language
        return {
            "id": business_id,
            "name": "Atlas Gadget Hub",
            "profile_metadata": profile_metadata,
        }


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

    async def list_messages(
        self,
        business_id: int,
        *,
        phone: str | None = None,
        direction: str | None = None,
        limit: int = 50,
    ):
        rows = [
            message
            for message in self.messages
            if message.get("business_id") == business_id
            and (phone is None or message.get("phone") == phone)
            and (direction is None or message.get("direction") == direction)
        ]
        rows.sort(key=lambda row: row.get("created_at") or datetime.min, reverse=True)
        return rows[:limit]

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

    async def update_order_status(
        self,
        *,
        business_id: int,
        order_id: int,
        status_value: str,
        confirmation_status: str,
        metadata: dict | None = None,
        finalized_order: dict | None = None,
    ):
        assert self.row is not None
        finalized_order = finalized_order or {}
        self.row = {
            **self.row,
            "status": status_value,
            "confirmation_status": confirmation_status,
            "metadata": metadata or {},
            "customer_phone": finalized_order.get("customer_phone", self.row.get("customer_phone")),
            "preferred_language": finalized_order.get("preferred_language", self.row.get("preferred_language")),
            "total_amount": finalized_order.get("total_amount", self.row.get("total_amount")),
            "currency": finalized_order.get("currency", self.row.get("currency")),
            "payment_method": finalized_order.get("payment_method", self.row.get("payment_method")),
            "delivery_city": finalized_order.get("delivery_city", self.row.get("delivery_city")),
            "delivery_address": finalized_order.get("delivery_address", self.row.get("delivery_address")),
            "order_notes": finalized_order.get("order_notes", self.row.get("order_notes")),
            "items": finalized_order.get("items", self.row.get("items")),
            "updated_at": datetime.now(UTC),
        }
        return self.row


class FakeOrderConfirmationRepository:
    def __init__(self) -> None:
        self.session = None
        self.events: list[dict] = []
        self.claim_result = True

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

    async def claim_confirmation_send(self, session_id: int) -> bool:
        return self.claim_result

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


def _build_service(
    *,
    business_default_language: str | None = "french",
) -> tuple[OrderConfirmationService, FakeChatRepository, FakeOrderRepository, FakeOrderConfirmationRepository]:
    session = type("DummyRepoSession", (), {"db": None})()
    service = OrderConfirmationService(
        session=session,
        messaging_provider=FakeProvider(),
        llm_provider=FakeLLMProvider(),
    )
    service.business_repository = FakeBusinessRepository(business_default_language)
    service.integration_repository = FakeIntegrationRepository()
    chat_repository = FakeChatRepository()
    service.chat_repository = chat_repository
    order_repository = FakeOrderRepository()
    service.order_repository = order_repository
    confirmation_repository = FakeOrderConfirmationRepository()
    service.order_confirmation_repository = confirmation_repository
    return service, chat_repository, order_repository, confirmation_repository


def _seed_inbound(chat_repository: FakeChatRepository, business_id: int, phone: str, text: str) -> None:
    import asyncio

    asyncio.run(
        chat_repository.upsert_message(
            business_id=business_id,
            phone=phone,
            customer_name=None,
            text=text,
            direction="inbound",
            intent=None,
            needs_human=False,
            is_read=True,
            provider="twilio",
            provider_message_sid="SM-INBOUND",
            provider_status="received",
            error_code=None,
            raw_payload={},
        )
    )


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
    assert confirmation_repository.session["preferred_language"] == "french"
    assert confirmation_repository.session["structured_snapshot"]["preferred_language"] == "french"


def test_ingest_store_order_falls_back_to_darija_when_business_default_language_missing() -> None:
    service, _, _, confirmation_repository = _build_service(business_default_language=None)

    import asyncio

    result = asyncio.run(
        service.ingest_store_order(
            2,
            StoreOrderIngestRequest(
                source_store="generic",
                external_order_id="WC-1003",
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
    assert confirmation_repository.session["preferred_language"] == "darija"
    assert confirmation_repository.session["structured_snapshot"]["preferred_language"] == "darija"


def test_template_send_stores_rendered_template_preview_in_chat() -> None:
    service, chat_repository, _, _ = _build_service()

    import asyncio

    row = asyncio.run(
        service._send_text(
            business_id=2,
            phone="+212600000001",
            text="fallback",
            connection={
                "config": {
                    "provider": "twilio",
                    "onboarding_status": "connected",
                    "subaccount_sid": "AC123",
                    "sender_sid": "PN123",
                    "whatsapp_number": "+14155238886",
                }
            },
            content_sid="HX0d04a9dd60c8885d847d7f6d5ee7a1b9",
            content_variables={
                "1": "Zakaria Imz",
                "2": "Mercedes Benz Mansoury",
                "3": "Selling Plans Ski Wax x1",
                "4": "Sala Al Jadida, Mly Youssef",
                "5": "Sala Al Jadida",
                "6": "9.95 USD",
            },
        )
    )

    assert row["text"].startswith("السلام عليكم Zakaria Imz")
    assert "🏠 العنوان: Sala Al Jadida, Mly Youssef" in row["text"]
    assert row["raw_payload"]["content_sid"] == "HX0d04a9dd60c8885d847d7f6d5ee7a1b9"


def test_ingest_store_order_skips_duplicate_send_when_claim_fails() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()
    confirmation_repository.claim_result = False

    import asyncio

    result = asyncio.run(
        service.ingest_store_order(
            2,
            StoreOrderIngestRequest(
                source_store="generic",
                external_order_id="WC-1002",
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

    assert result["confirmation_message_sent"] is False
    assert order_repository.row["confirmation_status"] == "pending_send"
    assert confirmation_repository.session["status"] == "pending_send"
    assert chat_repository.messages == []


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

    _seed_inbound(chat_repository, 2, "+212600000001", "1")

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
    assert confirmation_repository.session["preferred_language"] == "french"
    assert chat_repository.analysis_updates == [(55, "autre", False)]
    assert "est confirmée" in chat_repository.messages[1]["text"]
    assert service.llm_provider.detect_calls == []


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

    _seed_inbound(
        chat_repository,
        2,
        "+212600000001",
        "Oui je confirme mais changez mon adresse à Hay Hassani",
    )

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
    assert len(chat_repository.messages) == 2


def test_handle_inbound_custom_edit_reply_uses_ai_interpretation() -> None:
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
        "payment_method": "cash_on_delivery",
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

    _seed_inbound(
        chat_repository,
        2,
        "+212600000001",
        "C'est quoi l'adresse de livraison actuelle ?",
    )

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
            inbound_row={
                "id": 55,
                "phone": "+212600000001",
                "text": "Oui je confirme mais changez mon adresse à Hay Hassani",
            },
        )
    )

    assert handled is True
    assert confirmation_repository.session["status"] == "awaiting_customer"
    assert confirmation_repository.session["last_detected_intent"] == "awaiting_final_confirmation_after_edits"
    assert confirmation_repository.session["needs_human"] is False
    assert confirmation_repository.session["preferred_language"] == "french"
    assert order_repository.row["confirmation_status"] == "awaiting_customer"
    assert order_repository.row["status"] == "pending_confirmation"
    pending_edits = confirmation_repository.session["structured_snapshot"]["pending_edits"]
    assert pending_edits[0]["field"] == "delivery_address"
    assert "Hay Hassani" in pending_edits[0]["value"]
    assert confirmation_repository.session["structured_snapshot"]["delivery_address"] == "Hay Hassani, Casablanca"
    assert confirmation_repository.session["structured_snapshot"]["awaiting_final_confirmation_after_edits"] is True
    assert "Résumé mis à jour de la commande" in chat_repository.messages[1]["text"]
    assert "Répondez 1 pour confirmer la commande mise à jour" in chat_repository.messages[1]["text"]
    assert chat_repository.analysis_updates == [(55, "autre", False)]
    assert confirmation_repository.events[-1]["payload"]["automation_outcome"] == "awaiting_final_confirmation"
    assert confirmation_repository.events[-1]["payload"]["applied_to_snapshot"] is True
    assert service.llm_provider.detect_calls == []


def test_handle_inbound_delivery_question_answers_without_handoff() -> None:
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
        "structured_snapshot": {
            "delivery_city": "Casablanca",
            "delivery_address": "Maarif",
        },
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
        "payment_method": "cash_on_delivery",
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

    _seed_inbound(chat_repository, 2, "+212600000001", "Quantity: 3")

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
            inbound_row={
                "id": 56,
                "phone": "+212600000001",
                "text": "C'est quoi l'adresse de livraison actuelle ?",
            },
        )
    )

    assert handled is True
    assert confirmation_repository.session["status"] == "awaiting_customer"
    assert order_repository.row["confirmation_status"] == "awaiting_customer"
    assert chat_repository.analysis_updates == [(56, "autre", False)]
    assert "📍 Détails de livraison" in chat_repository.messages[1]["text"]


def test_handle_inbound_quantity_edit_stays_automated_until_final_confirmation() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()
    confirmation_repository.session = {
        "id": 21,
        "business_id": 2,
        "order_id": 10,
        "phone": "+212600000001",
        "customer_name": "Lina",
        "preferred_language": "english",
        "status": "awaiting_customer",
        "needs_human": False,
        "last_detected_intent": "order_confirmation_pending",
        "started_at": datetime.now(UTC),
        "last_customer_message_at": None,
        "confirmed_at": None,
        "declined_at": None,
        "updated_at": datetime.now(UTC),
        "structured_snapshot": {
            "items": [{"product_name": "Redmi Note 13", "quantity": 1, "unit_price": 100.0}],
            "currency": "MAD",
            "total_amount": 100.0,
        },
    }
    order_repository.row = {
        "id": 10,
        "business_id": 2,
        "source_store": "generic",
        "external_order_id": "WC-1001",
        "customer_name": "Lina",
        "customer_phone": "+212600000001",
        "preferred_language": "english",
        "total_amount": 100.0,
        "currency": "MAD",
        "payment_method": "cash_on_delivery",
        "delivery_city": "Casablanca",
        "delivery_address": "Maarif",
        "order_notes": None,
        "items": [{"product_name": "Redmi Note 13", "quantity": 1, "unit_price": 100.0}],
        "metadata": {},
        "raw_payload": {},
        "status": "pending_confirmation",
        "confirmation_status": "awaiting_customer",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }

    _seed_inbound(chat_repository, 2, "+212600000001", "1")

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
            inbound_row={"id": 57, "phone": "+212600000001", "text": "Quantity: 3"},
        )
    )

    assert handled is True
    assert confirmation_repository.session["status"] == "awaiting_customer"
    assert confirmation_repository.session["structured_snapshot"]["items"][0]["quantity"] == 3
    assert confirmation_repository.session["structured_snapshot"]["awaiting_final_confirmation_after_edits"] is True
    assert "Updated order summary" in chat_repository.messages[1]["text"]
    assert "Reply 1 to confirm the updated order" in chat_repository.messages[1]["text"]


def test_final_confirmation_after_edits_marks_order_confirmed() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()
    confirmation_repository.session = {
        "id": 21,
        "business_id": 2,
        "order_id": 10,
        "phone": "+212600000001",
        "customer_name": "Lina",
        "preferred_language": "english",
        "status": "awaiting_customer",
        "needs_human": False,
        "last_detected_intent": "awaiting_final_confirmation_after_edits",
        "started_at": datetime.now(UTC),
        "last_customer_message_at": None,
        "confirmed_at": None,
        "declined_at": None,
        "updated_at": datetime.now(UTC),
        "structured_snapshot": {
            "awaiting_final_confirmation_after_edits": True,
            "latest_detected_edits": [{"field": "variant", "value": "White"}],
            "items": [{"product_name": "BMW 120D", "quantity": 1, "variant": "White"}],
        },
    }
    order_repository.row = {
        "id": 10,
        "business_id": 2,
        "source_store": "generic",
        "external_order_id": "WC-1001",
        "customer_name": "Lina",
        "customer_phone": "+212600000001",
        "preferred_language": "english",
        "total_amount": 100000.0,
        "currency": "MAD",
        "payment_method": "cash_on_delivery",
        "delivery_city": "Casablanca",
        "delivery_address": "Maarif",
        "order_notes": None,
        "items": [{"product_name": "BMW 120D", "quantity": 1, "variant": "Black"}],
        "metadata": {},
        "raw_payload": {},
        "status": "pending_confirmation",
        "confirmation_status": "awaiting_customer",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }

    _seed_inbound(chat_repository, 2, "+212600000001", "2")

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
            inbound_row={"id": 58, "phone": "+212600000001", "text": "1"},
        )
    )

    assert handled is True
    assert confirmation_repository.session["status"] == "confirmed"
    assert order_repository.row["confirmation_status"] == "confirmed"
    assert order_repository.row["status"] == "confirmed"
    assert order_repository.row["items"][0]["variant"] == "White"
    assert order_repository.row["preferred_language"] == "english"
    assert order_repository.row["metadata"]["order_confirmation"]["final_snapshot_applied"] is True
    assert confirmation_repository.session["preferred_language"] == "english"
    assert chat_repository.analysis_updates == [(58, "autre", False)]
    assert service.llm_provider.detect_calls == []


def test_numeric_edit_option_keeps_initial_session_language() -> None:
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
        "structured_snapshot": {"preferred_language": "french"},
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
        "payment_method": "cash_on_delivery",
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

    _seed_inbound(
        chat_repository,
        2,
        "+212600000001",
        "Bonjour, je veux changer mon adresse",
    )

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
            inbound_row={"id": 60, "phone": "+212600000001", "text": "2"},
        )
    )

    assert handled is True
    assert confirmation_repository.session["status"] == "edit_requested"
    assert confirmation_repository.session["preferred_language"] == "french"
    assert "Répondez avec les éléments à modifier" in chat_repository.messages[1]["text"]
    assert service.llm_provider.detect_calls == []


def test_custom_text_keeps_business_default_language_for_following_replies() -> None:
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
        "structured_snapshot": {
            "preferred_language": "french",
            "items": [{"product_name": "Redmi Note 13", "quantity": 1, "unit_price": 100.0}],
            "currency": "MAD",
            "total_amount": 100.0,
        },
    }
    order_repository.row = {
        "id": 10,
        "business_id": 2,
        "source_store": "generic",
        "external_order_id": "WC-1001",
        "customer_name": "Lina",
        "customer_phone": "+212600000001",
        "preferred_language": "french",
        "total_amount": 100.0,
        "currency": "MAD",
        "payment_method": "cash_on_delivery",
        "delivery_city": "Casablanca",
        "delivery_address": "Maarif",
        "order_notes": None,
        "items": [{"product_name": "Redmi Note 13", "quantity": 1, "unit_price": 100.0}],
        "metadata": {},
        "raw_payload": {},
        "status": "pending_confirmation",
        "confirmation_status": "awaiting_customer",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }

    _seed_inbound(chat_repository, 2, "+212600000001", "Quantity: 3")

    import asyncio

    first_handled = asyncio.run(
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
            inbound_row={"id": 61, "phone": "+212600000001", "text": "Quantity: 3"},
        )
    )

    assert first_handled is True
    assert confirmation_repository.session["preferred_language"] == "french"
    assert confirmation_repository.session["structured_snapshot"]["preferred_language"] == "french"
    assert "Résumé mis à jour de la commande" in chat_repository.messages[1]["text"]

    _seed_inbound(chat_repository, 2, "+212600000001", "1")

    second_handled = asyncio.run(
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
            inbound_row={"id": 62, "phone": "+212600000001", "text": "1"},
        )
    )

    assert second_handled is True
    assert confirmation_repository.session["status"] == "confirmed"
    assert confirmation_repository.session["preferred_language"] == "french"
    assert "est confirmée" in chat_repository.messages[-1]["text"]
    assert service.llm_provider.detect_calls == []


def test_custom_text_does_not_replace_darija_business_default_language() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()
    confirmation_repository.session = {
        "id": 21,
        "business_id": 2,
        "order_id": 10,
        "phone": "+212600000001",
        "customer_name": "Lina",
        "preferred_language": "darija",
        "status": "awaiting_customer",
        "needs_human": False,
        "last_detected_intent": "order_confirmation_pending",
        "started_at": datetime.now(UTC),
        "last_customer_message_at": None,
        "confirmed_at": None,
        "declined_at": None,
        "updated_at": datetime.now(UTC),
        "structured_snapshot": {
            "preferred_language": "darija",
            "delivery_city": "Casablanca",
            "delivery_address": "Maarif",
        },
    }
    order_repository.row = {
        "id": 10,
        "business_id": 2,
        "source_store": "generic",
        "external_order_id": "WC-1001",
        "customer_name": "Lina",
        "customer_phone": "+212600000001",
        "preferred_language": "darija",
        "total_amount": 3499,
        "currency": "MAD",
        "payment_method": "cash_on_delivery",
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

    _seed_inbound(chat_repository, 2, "+212600000001", "Change it")

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
            inbound_row={
                "id": 63,
                "phone": "+212600000001",
                "text": "Bonjour, je veux changer mon adresse",
            },
        )
    )

    assert handled is True
    assert confirmation_repository.session["preferred_language"] == "darija"
    assert confirmation_repository.session["structured_snapshot"]["preferred_language"] == "darija"
    assert "Hadchi howa l update dyal commande" in chat_repository.messages[1]["text"]
    assert service.llm_provider.detect_calls == []


def test_business_default_english_maps_order_confirmation_language_to_darija() -> None:
    service, _, _, confirmation_repository = _build_service(
        business_default_language="english"
    )

    import asyncio

    result = asyncio.run(
        service.ingest_store_order(
            2,
            StoreOrderIngestRequest(
                source_store="generic",
                external_order_id="WC-1004",
                customer_name="Lina",
                customer_phone="+212600000001",
                preferred_language="english",
                total_amount=3499,
                currency="MAD",
                delivery_city="Casablanca",
                delivery_address="Maarif",
                items=[{"product_name": "Redmi Note 13", "quantity": 1}],
            ),
        )
    )

    assert result["confirmation_message_sent"] is True
    assert confirmation_repository.session["preferred_language"] == "darija"
    assert confirmation_repository.session["structured_snapshot"]["preferred_language"] == "darija"


def test_language_detection_failure_keeps_existing_session_language() -> None:
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
        "structured_snapshot": {"preferred_language": "french"},
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
        "payment_method": "cash_on_delivery",
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

    async def broken_detect_language(*, message: str):
        raise RuntimeError("language detection unavailable")

    service.llm_provider.detect_language = broken_detect_language

    _seed_inbound(
        chat_repository,
        2,
        "+212600000001",
        "I want to know the delivery details",
    )

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
            inbound_row={
                "id": 64,
                "phone": "+212600000001",
                "text": "I want to know the delivery details",
            },
        )
    )

    assert handled is True
    assert confirmation_repository.session["preferred_language"] == "french"
    assert "Détails de livraison" in chat_repository.messages[1]["text"]


def test_admin_confirm_applies_final_snapshot_to_order() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()
    confirmation_repository.session = {
        "id": 21,
        "business_id": 2,
        "order_id": 10,
        "phone": "+212600000001",
        "customer_name": "Lina",
        "preferred_language": "english",
        "status": "awaiting_customer",
        "needs_human": False,
        "last_detected_intent": "awaiting_final_confirmation_after_edits",
        "started_at": datetime.now(UTC),
        "last_customer_message_at": None,
        "confirmed_at": None,
        "declined_at": None,
        "updated_at": datetime.now(UTC),
        "structured_snapshot": {
            "preferred_language": "english",
            "awaiting_final_confirmation_after_edits": True,
            "latest_detected_edits": [{"field": "delivery_city", "value": "Tanger"}],
            "delivery_city": "Tanger",
            "delivery_address": "Centre Ville",
            "items": [{"product_name": "BMW 120D", "quantity": 1, "variant": "White"}],
            "currency": "MAD",
            "total_amount": 100000.0,
        },
    }
    order_repository.row = {
        "id": 10,
        "business_id": 2,
        "source_store": "generic",
        "external_order_id": "WC-1001",
        "customer_name": "Lina",
        "customer_phone": "+212600000001",
        "preferred_language": "french",
        "total_amount": 100000.0,
        "currency": "MAD",
        "payment_method": "cash_on_delivery",
        "delivery_city": "Casablanca",
        "delivery_address": "Maarif",
        "order_notes": None,
        "items": [{"product_name": "BMW 120D", "quantity": 1, "variant": "Black"}],
        "metadata": {},
        "raw_payload": {},
        "status": "pending_confirmation",
        "confirmation_status": "awaiting_customer",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }

    import asyncio

    detail = asyncio.run(
        service.apply_action(
            2,
            21,
            OrderConfirmationActionRequest(action="confirm"),
        )
    )

    assert detail["status"] == "confirmed"
    assert detail["order"]["status"] == "confirmed"
    assert detail["order"]["delivery_city"] == "Tanger"
    assert detail["order"]["items"][0]["variant"] == "White"
    assert detail["order"]["metadata"]["order_confirmation"]["final_snapshot_applied"] is True
    assert confirmation_repository.events[-1]["payload"]["finalized_order"]["delivery_city"] == "Tanger"
    assert chat_repository.messages == []


def test_low_confidence_edit_escalates_to_human() -> None:
    service, chat_repository, order_repository, confirmation_repository = _build_service()
    confirmation_repository.session = {
        "id": 21,
        "business_id": 2,
        "order_id": 10,
        "phone": "+212600000001",
        "customer_name": "Lina",
        "preferred_language": "english",
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
        "preferred_language": "english",
        "total_amount": 100.0,
        "currency": "MAD",
        "payment_method": "cash_on_delivery",
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
            inbound_row={"id": 59, "phone": "+212600000001", "text": "Change it"},
        )
    )

    assert handled is True
    assert confirmation_repository.session["status"] == "human_requested"
    assert order_repository.row["confirmation_status"] == "human_requested"
    assert chat_repository.analysis_updates == [(59, "autre", True)]
