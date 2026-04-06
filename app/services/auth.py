from __future__ import annotations

from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request, status

from app.services.database import get_session
from app.services.repository_factory import RepositoryFactory


@dataclass(slots=True)
class AuthenticatedUser:
    auth_user_id: str
    email: str | None = None


def _first_header(request: Request, names: tuple[str, ...]) -> str | None:
    for name in names:
        value = str(request.headers.get(name) or "").strip()
        if value:
            return value
    return None


async def require_authenticated_user(request: Request) -> AuthenticatedUser:
    auth_user_id = _first_header(
        request,
        ("x-auth-user-id", "x-user-id"),
    )
    if not auth_user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authenticated user headers are required.",
        )
    return AuthenticatedUser(
        auth_user_id=auth_user_id,
        email=_first_header(request, ("x-auth-user-email", "x-user-email")),
    )


async def ensure_user_can_access_business(
    *,
    session,
    current_user: AuthenticatedUser,
    business_id: int,
) -> None:
    await RepositoryFactory(session).memberships().require_business_access(
        auth_user_id=current_user.auth_user_id,
        business_id=business_id,
    )


async def require_business_access(
    business_id: int,
    session=Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
) -> AuthenticatedUser:
    await ensure_user_can_access_business(
        session=session,
        current_user=current_user,
        business_id=business_id,
    )
    return current_user
