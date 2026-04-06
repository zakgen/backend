from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.account import MyBusinessCreateRequest, MyBusinessesResponse
from app.schemas.business import BusinessResponse
from app.services.account_service import AccountService
from app.services.auth import AuthenticatedUser, require_authenticated_user
from app.services.dashboard_service import to_iso
from app.services.database import get_session


router = APIRouter(prefix="/me", tags=["account"])


def _serialize_business(row: dict) -> BusinessResponse:
    return BusinessResponse.model_validate(
        {
            "id": int(row["id"]),
            "name": row["name"],
            "description": row.get("description"),
            "city": row.get("city"),
            "shipping_policy": row.get("shipping_policy"),
            "delivery_zones": row.get("delivery_zones") or [],
            "payment_methods": row.get("payment_methods") or [],
            "profile_metadata": row.get("profile_metadata") or {},
            "updated_at": to_iso(row.get("updated_at")),
        }
    )


@router.get("/businesses", response_model=MyBusinessesResponse, status_code=status.HTTP_200_OK)
async def list_my_businesses(
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> MyBusinessesResponse:
    businesses, current_business_id = await AccountService(session=session).list_businesses(
        current_user
    )
    return MyBusinessesResponse(
        businesses=[_serialize_business(row) for row in businesses],
        current_business_id=current_business_id,
    )


@router.get("/business", response_model=BusinessResponse, status_code=status.HTTP_200_OK)
async def get_my_current_business(
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> BusinessResponse:
    business = await AccountService(session=session).get_current_business(current_user)
    if business is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No business is linked to the current user.",
        )
    return _serialize_business(business)


@router.post("/businesses", response_model=BusinessResponse, status_code=status.HTTP_200_OK)
async def create_my_business(
    payload: MyBusinessCreateRequest,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> BusinessResponse:
    business = await AccountService(session=session).create_business(
        current_user=current_user,
        payload=payload,
    )
    await session.commit()
    return _serialize_business(business)
