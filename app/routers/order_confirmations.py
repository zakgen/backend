from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.order_confirmation import (
    OrderConfirmationActionRequest,
    OrderConfirmationIngestResponse,
    OrderConfirmationSessionDetail,
    OrderConfirmationSessionListResponse,
    OrderConfirmationSessionSummary,
    OrderRecord,
    StoreOrderIngestRequest,
)
from app.services.auth import AuthenticatedUser, require_business_access
from app.services.database import get_session
from app.services.dashboard_service import to_iso
from app.services.order_confirmation_service import OrderConfirmationService
from app.services.twilio_provider import TwilioMessagingProvider


router = APIRouter(prefix="/business", tags=["order-confirmations"])


def _serialize_order(row: dict) -> OrderRecord:
    return OrderRecord.model_validate(
        {
            "id": str(row["id"]),
            "business_id": int(row["business_id"]),
            "source_store": row["source_store"],
            "external_order_id": row["external_order_id"],
            "customer_name": row.get("customer_name"),
            "customer_phone": row["customer_phone"],
            "preferred_language": row.get("preferred_language"),
            "total_amount": float(row.get("total_amount") or 0),
            "currency": row.get("currency") or "MAD",
            "payment_method": row.get("payment_method"),
            "delivery_city": row.get("delivery_city"),
            "delivery_address": row.get("delivery_address"),
            "order_notes": row.get("order_notes"),
            "status": row.get("status") or "pending_confirmation",
            "confirmation_status": row.get("confirmation_status") or "pending_send",
            "items": row.get("items") or [],
            "metadata": row.get("metadata") or {},
            "created_at": to_iso(row.get("created_at")),
            "updated_at": to_iso(row.get("updated_at")),
        }
    )


def _serialize_session_summary(row: dict) -> OrderConfirmationSessionSummary:
    return OrderConfirmationSessionSummary.model_validate(
        {
            "id": str(row["id"]),
            "order_id": str(row["order_id"]),
            "business_id": int(row["business_id"]),
            "phone": row["phone"],
            "customer_name": row.get("customer_name"),
            "preferred_language": row.get("preferred_language"),
            "status": row["status"],
            "needs_human": bool(row.get("needs_human") or False),
            "last_detected_intent": row.get("last_detected_intent"),
            "started_at": to_iso(row.get("started_at")),
            "last_customer_message_at": to_iso(row.get("last_customer_message_at")),
            "confirmed_at": to_iso(row.get("confirmed_at")),
            "declined_at": to_iso(row.get("declined_at")),
            "updated_at": to_iso(row.get("updated_at")),
        }
    )


def _serialize_session_detail(row: dict) -> OrderConfirmationSessionDetail:
    return OrderConfirmationSessionDetail.model_validate(
        {
            **_serialize_session_summary(row).model_dump(),
            "structured_snapshot": row.get("structured_snapshot") or {},
            "order": _serialize_order(row["order"]).model_dump(),
            "events": [
                {
                    "id": str(event["id"]),
                    "session_id": str(event["session_id"]),
                    "event_type": event["event_type"],
                    "payload": event.get("payload") or {},
                    "created_at": to_iso(event.get("created_at")),
                }
                for event in (row.get("events") or [])
            ],
        }
    )


@router.post(
    "/{business_id}/order-confirmations/orders",
    response_model=OrderConfirmationIngestResponse,
    status_code=status.HTTP_200_OK,
)
async def ingest_store_order(
    business_id: int,
    payload: StoreOrderIngestRequest,
    current_user: AuthenticatedUser = Depends(require_business_access),
    session: AsyncSession = Depends(get_session),
) -> OrderConfirmationIngestResponse:
    service = OrderConfirmationService(
        session=session,
        messaging_provider=TwilioMessagingProvider(),
    )
    result = await service.ingest_store_order(business_id, payload)
    await session.commit()
    detail = await service.get_session_detail(business_id, int(result["session"]["id"]))
    return OrderConfirmationIngestResponse(
        order=_serialize_order(result["order"]),
        session=_serialize_session_detail(detail),
        confirmation_message_sent=result["confirmation_message_sent"],
    )


@router.get(
    "/{business_id}/order-confirmations/sessions",
    response_model=OrderConfirmationSessionListResponse,
    status_code=status.HTTP_200_OK,
)
async def list_order_confirmation_sessions(
    business_id: int,
    status_value: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    current_user: AuthenticatedUser = Depends(require_business_access),
    session: AsyncSession = Depends(get_session),
) -> OrderConfirmationSessionListResponse:
    service = OrderConfirmationService(
        session=session,
        messaging_provider=TwilioMessagingProvider(),
    )
    rows = await service.list_sessions(business_id, status_value=status_value, limit=limit)
    return OrderConfirmationSessionListResponse(
        sessions=[_serialize_session_summary(row) for row in rows],
        total=len(rows),
    )


@router.get(
    "/{business_id}/order-confirmations/sessions/{session_id}",
    response_model=OrderConfirmationSessionDetail,
    status_code=status.HTTP_200_OK,
)
async def get_order_confirmation_session(
    business_id: int,
    session_id: int,
    current_user: AuthenticatedUser = Depends(require_business_access),
    session: AsyncSession = Depends(get_session),
) -> OrderConfirmationSessionDetail:
    service = OrderConfirmationService(
        session=session,
        messaging_provider=TwilioMessagingProvider(),
    )
    detail = await service.get_session_detail(business_id, session_id)
    return _serialize_session_detail(detail)


@router.post(
    "/{business_id}/order-confirmations/sessions/{session_id}/actions",
    response_model=OrderConfirmationSessionDetail,
    status_code=status.HTTP_200_OK,
)
async def apply_order_confirmation_action(
    business_id: int,
    session_id: int,
    payload: OrderConfirmationActionRequest,
    current_user: AuthenticatedUser = Depends(require_business_access),
    session: AsyncSession = Depends(get_session),
) -> OrderConfirmationSessionDetail:
    service = OrderConfirmationService(
        session=session,
        messaging_provider=TwilioMessagingProvider(),
    )
    detail = await service.apply_action(business_id, session_id, payload)
    await session.commit()
    return _serialize_session_detail(detail)
