from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.account import MyBusinessCreateRequest
from app.schemas.business import BusinessUpsertRequest
from app.services.auth import AuthenticatedUser
from app.services.embedding_service import EmbeddingService
from app.services.repository_factory import RepositoryFactory
from app.services.sync_service import SyncService


class AccountService:
    def __init__(self, *, session: AsyncSession) -> None:
        self.session = session
        factory = RepositoryFactory(session)
        self.business_repository = factory.business()
        self.membership_repository = factory.memberships()

    async def list_businesses(self, current_user: AuthenticatedUser) -> tuple[list[dict], int | None]:
        businesses = await self.membership_repository.list_businesses_for_user(
            current_user.auth_user_id
        )
        current_business = await self.membership_repository.get_current_business_for_user(
            current_user.auth_user_id
        )
        return businesses, None if current_business is None else int(current_business["id"])

    async def get_current_business(self, current_user: AuthenticatedUser) -> dict | None:
        return await self.membership_repository.get_current_business_for_user(
            current_user.auth_user_id
        )

    async def create_business(
        self,
        *,
        current_user: AuthenticatedUser,
        payload: MyBusinessCreateRequest,
    ) -> dict:
        existing_count = await self.membership_repository.count_businesses_for_user(
            current_user.auth_user_id
        )
        business_row = await self.business_repository.upsert(
            BusinessUpsertRequest(
                name=payload.name,
                description=payload.description,
                city=payload.city,
                shipping_policy=payload.shipping_policy,
                delivery_zones=payload.delivery_zones,
                payment_methods=payload.payment_methods,
                profile_metadata=payload.profile_metadata,
            )
        )
        await self.membership_repository.upsert_membership(
            auth_user_id=current_user.auth_user_id,
            email=current_user.email,
            business_id=int(business_row["id"]),
            role="owner",
            is_default=existing_count == 0,
        )
        sync_service = SyncService(
            session=self.session,
            embedding_service=EmbeddingService(),
        )
        await sync_service.sync_business_profile(int(business_row["id"]))
        await sync_service.update_status_snapshot(
            int(business_row["id"]),
            last_result="Business profile synced successfully.",
        )
        return await self.business_repository.get_by_id(int(business_row["id"]))
