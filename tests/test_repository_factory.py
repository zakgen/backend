from app.config import Settings
from app.services.mongo_repositories import MongoBusinessRepository, MongoProductRepository
from app.services.repositories import BusinessRepository, ProductRepository
from app.services.repository_factory import RepositoryFactory


def test_repository_factory_returns_postgres_repositories_by_default() -> None:
    settings = Settings(
        db_url="postgresql+asyncpg://postgres:postgres@localhost:5432/zakbot",
        database_backend="postgres",
    )
    factory = RepositoryFactory(session=object(), settings=settings)  # type: ignore[arg-type]

    assert isinstance(factory.business(), BusinessRepository)
    assert isinstance(factory.products(), ProductRepository)


def test_repository_factory_returns_mongo_repositories_when_enabled() -> None:
    class DummySession:
        db = object()

    settings = Settings(
        database_backend="mongo",
        mongo_url="mongodb+srv://example",
    )
    factory = RepositoryFactory(session=DummySession(), settings=settings)

    assert isinstance(factory.business(), MongoBusinessRepository)
    assert isinstance(factory.products(), MongoProductRepository)
