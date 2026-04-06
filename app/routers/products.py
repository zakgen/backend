from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.product import (
    DashboardProductBulkRequest,
    DashboardProductCreateRequest,
    DashboardProductUpdateRequest,
    Product,
    ProductListResult,
)
from app.schemas.product import (
    BulkProductUpsertRequest,
    BulkProductUpsertResponse,
    ProductResponse,
    ProductUpsertRequest,
)
from app.services.auth import AuthenticatedUser, ensure_user_can_access_business, require_authenticated_user, require_business_access
from app.services.database import get_session
from app.services.dashboard_service import build_product_storage_payload, product_row_to_dashboard
from app.services.embedding_service import EmbeddingService
from app.services.repository_factory import RepositoryFactory
from app.services.sync_service import SyncService


router = APIRouter(tags=["products"])


@router.get(
    "/business/{business_id}/products",
    response_model=ProductListResult,
    status_code=status.HTTP_200_OK,
)
async def list_products(
    business_id: int,
    search: str | None = None,
    category: str | None = None,
    current_user: AuthenticatedUser = Depends(require_business_access),
    session: AsyncSession = Depends(get_session),
) -> ProductListResult:
    rows, total, categories = await RepositoryFactory(session).products().list_dashboard(
        business_id, search=search, category=category
    )
    return ProductListResult(
        products=[product_row_to_dashboard(row) for row in rows],
        total=total,
        categories=categories,
    )


@router.post("/products", response_model=Product, status_code=status.HTTP_200_OK)
async def create_product(
    payload: DashboardProductCreateRequest,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> Product:
    await ensure_user_can_access_business(
        session=session,
        current_user=current_user,
        business_id=payload.business_id,
    )
    repository = RepositoryFactory(session).products()
    product_row = await repository.create_dashboard_product(
        build_product_storage_payload(
            business_id=payload.business_id,
            external_id=payload.external_id,
            name=payload.name,
            description=payload.description,
            category=payload.category,
            price=payload.price,
            currency=payload.currency,
            stock_status=payload.stock_status,
            variants=[variant.model_dump() for variant in payload.variants],
        )
    )
    sync_service = SyncService(session=session, embedding_service=EmbeddingService())
    await sync_service.sync_products(payload.business_id, product_ids=[product_row["id"]])
    await sync_service.update_status_snapshot(
        payload.business_id, last_result="Product created from dashboard."
    )
    await session.commit()
    return product_row_to_dashboard(
        await repository.get_by_id(payload.business_id, product_row["id"])
    )


@router.put("/products/{product_id}", response_model=Product, status_code=status.HTTP_200_OK)
async def update_product(
    product_id: int,
    payload: DashboardProductUpdateRequest,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> Product:
    repository = RepositoryFactory(session).products()
    existing = await repository.get_by_product_id(product_id)
    await ensure_user_can_access_business(
        session=session,
        current_user=current_user,
        business_id=int(existing["business_id"]),
    )
    updated_row = await repository.update_dashboard_product(
        product_id,
        build_product_storage_payload(
            business_id=existing["business_id"],
            external_id=payload.external_id
            if payload.external_id is not None
            else existing.get("external_id"),
            name=payload.name if payload.name is not None else existing["name"],
            description=payload.description
            if payload.description is not None
            else existing.get("description") or "",
            category=payload.category
            if payload.category is not None
            else existing.get("category") or "",
            price=payload.price if payload.price is not None else existing.get("price"),
            currency=payload.currency
            if payload.currency is not None
            else existing.get("currency") or "MAD",
            stock_status=payload.stock_status
            if payload.stock_status is not None
            else existing.get("availability") or "in_stock",
            variants=[variant.model_dump() for variant in payload.variants]
            if payload.variants is not None
            else existing.get("variants") or [],
            metadata=existing.get("metadata") or {},
        ),
    )
    sync_service = SyncService(session=session, embedding_service=EmbeddingService())
    await sync_service.sync_products(existing["business_id"], product_ids=[product_id])
    await sync_service.update_status_snapshot(
        existing["business_id"], last_result="Product updated from dashboard."
    )
    await session.commit()
    return product_row_to_dashboard(updated_row)


@router.delete("/products/{product_id}", status_code=status.HTTP_200_OK)
async def delete_product(
    product_id: int,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> dict[str, bool | str]:
    repository = RepositoryFactory(session).products()
    existing = await repository.get_by_product_id(product_id)
    await ensure_user_can_access_business(
        session=session,
        current_user=current_user,
        business_id=int(existing["business_id"]),
    )
    deleted = await repository.delete(product_id)
    await SyncService(session=session, embedding_service=EmbeddingService()).update_status_snapshot(
        deleted["business_id"],
        last_result="Product deleted from dashboard.",
    )
    await session.commit()
    return {"deleted": True, "id": str(deleted["id"])}


@router.post("/products/bulk", response_model=ProductListResult, status_code=status.HTTP_200_OK)
async def bulk_create_products(
    payload: DashboardProductBulkRequest,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> ProductListResult:
    await ensure_user_can_access_business(
        session=session,
        current_user=current_user,
        business_id=payload.business_id,
    )
    repository = RepositoryFactory(session).products()
    created_rows = []
    for item in payload.products:
        created_rows.append(
            await repository.create_dashboard_product(
                build_product_storage_payload(
                    business_id=payload.business_id,
                    external_id=item.external_id,
                    name=item.name,
                    description=item.description,
                    category=item.category,
                    price=item.price,
                    currency=item.currency,
                    stock_status=item.stock_status,
                    variants=[variant.model_dump() for variant in item.variants],
                )
            )
        )

    sync_service = SyncService(session=session, embedding_service=EmbeddingService())
    await sync_service.sync_products(
        payload.business_id, product_ids=[row["id"] for row in created_rows]
    )
    await sync_service.update_status_snapshot(
        payload.business_id, last_result="Bulk product import completed from dashboard."
    )
    await session.commit()

    rows, total, categories = await repository.list_dashboard(payload.business_id)
    return ProductListResult(
        products=[product_row_to_dashboard(row) for row in rows],
        total=total,
        categories=categories,
    )


@router.post("/products/upsert", response_model=ProductResponse, status_code=status.HTTP_200_OK)
async def upsert_product(
    payload: ProductUpsertRequest,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> ProductResponse:
    await ensure_user_can_access_business(
        session=session,
        current_user=current_user,
        business_id=payload.business_id,
    )
    repository = RepositoryFactory(session).products()
    product = await repository.upsert(payload)

    sync_service = SyncService(session=session, embedding_service=EmbeddingService())
    await sync_service.sync_products(payload.business_id, product_ids=[product["id"]])
    await sync_service.update_status_snapshot(
        payload.business_id, last_result="Product synced successfully."
    )
    await session.commit()

    refreshed = await repository.get_by_id(payload.business_id, product["id"])
    return ProductResponse.model_validate(refreshed)


@router.post(
    "/products/bulk-upsert",
    response_model=BulkProductUpsertResponse,
    status_code=status.HTTP_200_OK,
)
async def bulk_upsert_products(
    payload: BulkProductUpsertRequest,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
    session: AsyncSession = Depends(get_session),
) -> BulkProductUpsertResponse:
    await ensure_user_can_access_business(
        session=session,
        current_user=current_user,
        business_id=payload.business_id,
    )
    repository = RepositoryFactory(session).products()
    products = await repository.bulk_upsert(payload)

    sync_service = SyncService(session=session, embedding_service=EmbeddingService())
    await sync_service.sync_products(
        payload.business_id, product_ids=[product["id"] for product in products]
    )
    await sync_service.update_status_snapshot(
        payload.business_id, last_result="Bulk product sync completed successfully."
    )
    await session.commit()

    return BulkProductUpsertResponse(
        business_id=payload.business_id,
        count=len(products),
        products=[ProductResponse.model_validate(product) for product in products],
    )
