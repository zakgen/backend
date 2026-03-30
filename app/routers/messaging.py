from __future__ import annotations

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.conversation import ConversationMessage, ConversationReplyRequest
from app.services.database import get_session
from app.services.dashboard_service import chat_row_to_message
from app.services.messaging_service import MessagingService
from app.services.twilio_provider import TwilioMessagingProvider


router = APIRouter(tags=["messaging"])


def _webhook_url(request: Request) -> str:
    base = request.app.state.public_webhook_base_url if hasattr(request.app.state, "public_webhook_base_url") else None
    query = f"?{request.url.query}" if request.url.query else ""
    if base:
        return f"{str(base).rstrip('/')}{request.url.path}{query}"
    return str(request.url)


@router.post(
    "/business/{business_id}/chats/{phone}/reply",
    response_model=ConversationMessage,
    status_code=status.HTTP_200_OK,
)
async def reply_to_chat(
    business_id: int,
    phone: str,
    payload: ConversationReplyRequest,
    session: AsyncSession = Depends(get_session),
) -> ConversationMessage:
    service = MessagingService(session=session, provider=TwilioMessagingProvider())
    message = await service.send_reply(business_id, phone, payload)
    await session.commit()
    return ConversationMessage.model_validate(message)


@router.post("/webhooks/twilio/whatsapp/inbound", status_code=status.HTTP_200_OK)
async def twilio_inbound_webhook(
    request: Request, session: AsyncSession = Depends(get_session)
) -> JSONResponse:
    params = dict(await request.form())
    service = MessagingService(session=session, provider=TwilioMessagingProvider())
    row = await service.handle_inbound_webhook(
        url=_webhook_url(request),
        headers=request.headers,
        params=params,
    )
    await session.commit()
    return JSONResponse({"status": "accepted", "id": str(row["id"])})


@router.post("/webhooks/twilio/whatsapp/status", status_code=status.HTTP_200_OK)
async def twilio_status_webhook(
    request: Request, session: AsyncSession = Depends(get_session)
) -> JSONResponse:
    params = dict(await request.form())
    service = MessagingService(session=session, provider=TwilioMessagingProvider())
    row = await service.handle_status_webhook(
        url=_webhook_url(request),
        headers=request.headers,
        params=params,
    )
    await session.commit()
    if row is None:
        return JSONResponse({"status": "ignored", "reason": "unknown_message_sid"})
    return JSONResponse({"status": "updated", "id": str(row["id"])})
