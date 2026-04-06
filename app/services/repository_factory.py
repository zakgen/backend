from __future__ import annotations

from fastapi import HTTPException, status
from typing import Any

from app.config import Settings, get_settings
from app.services.mongo_repositories import (
    MongoAIRunRepository,
    MongoBusinessRepository,
    MongoBusinessMembershipRepository,
    MongoChatRepository,
    MongoFAQRepository,
    MongoIntegrationRepository,
    MongoProductRepository,
    MongoSyncStatusRepository,
)
from app.services.mongo_order_repositories import (
    MongoOrderConfirmationRepository,
    MongoOrderRepository,
)
from app.services.order_repositories import (
    OrderConfirmationRepository,
    OrderRepository,
)
from app.services.repositories import (
    AIRunRepository,
    BusinessRepository,
    BusinessMembershipRepository,
    ChatRepository,
    FAQRepository,
    IntegrationRepository,
    ProductRepository,
    SyncStatusRepository,
)


class RepositoryFactory:
    def __init__(self, session: Any, settings: Settings | None = None) -> None:
        self.session = session
        self.settings = settings or get_settings()

    def _is_mongo(self) -> bool:
        if self.settings.database_backend == "mongo":
            return True
        if self.settings.database_backend == "postgres":
            return False
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Unsupported DATABASE_BACKEND: {self.settings.database_backend}",
        )

    def business(self) -> BusinessRepository | MongoBusinessRepository:
        if self._is_mongo():
            return MongoBusinessRepository(self.session)
        return BusinessRepository(self.session)

    def memberships(self) -> BusinessMembershipRepository | MongoBusinessMembershipRepository:
        if self._is_mongo():
            return MongoBusinessMembershipRepository(self.session)
        return BusinessMembershipRepository(self.session)

    def products(self) -> ProductRepository | MongoProductRepository:
        if self._is_mongo():
            return MongoProductRepository(self.session)
        return ProductRepository(self.session)

    def faqs(self) -> FAQRepository | MongoFAQRepository:
        if self._is_mongo():
            return MongoFAQRepository(self.session)
        return FAQRepository(self.session)

    def chats(self) -> ChatRepository | MongoChatRepository:
        if self._is_mongo():
            return MongoChatRepository(self.session)
        return ChatRepository(self.session)

    def integrations(self) -> IntegrationRepository | MongoIntegrationRepository:
        if self._is_mongo():
            return MongoIntegrationRepository(self.session)
        return IntegrationRepository(self.session)

    def sync_status(self) -> SyncStatusRepository | MongoSyncStatusRepository:
        if self._is_mongo():
            return MongoSyncStatusRepository(self.session)
        return SyncStatusRepository(self.session)

    def ai_runs(self) -> AIRunRepository | MongoAIRunRepository:
        if self._is_mongo():
            return MongoAIRunRepository(self.session)
        return AIRunRepository(self.session)

    def orders(self) -> OrderRepository | MongoOrderRepository:
        if self._is_mongo():
            return MongoOrderRepository(self.session)
        return OrderRepository(self.session)

    def order_confirmations(
        self,
    ) -> OrderConfirmationRepository | MongoOrderConfirmationRepository:
        if self._is_mongo():
            return MongoOrderConfirmationRepository(self.session)
        return OrderConfirmationRepository(self.session)
