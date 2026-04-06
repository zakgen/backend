from __future__ import annotations

from datetime import UTC, datetime
import json
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import text, text as sql_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.business import BusinessUpsertRequest
from app.schemas.faq import FAQUpsertRequest
from app.schemas.product import BulkProductUpsertRequest, ProductBulkItem, ProductUpsertRequest
from app.utils.phones import normalize_phone_number
from app.utils.vector import to_vector_literal


def _json_dumps(value: Any) -> str:
    return json.dumps(value)


def _build_where_clause(
    *, business_id: int, search: str | None = None, category: str | None = None
) -> tuple[str, dict[str, Any]]:
    conditions = ["business_id = :business_id"]
    params: dict[str, Any] = {"business_id": business_id}

    if search:
        conditions.append("(name ILIKE :search OR COALESCE(description, '') ILIKE :search)")
        params["search"] = f"%{search.strip()}%"

    if category:
        conditions.append("category = :category")
        params["category"] = category.strip()

    return " AND ".join(conditions), params


class BusinessRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_id(self, business_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                SELECT id, name, description, city, shipping_policy,
                       delivery_zones, payment_methods, profile_metadata,
                       created_at, updated_at
                FROM business
                WHERE id = :business_id
                """
            ),
            {"business_id": business_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Business {business_id} was not found.",
            )
        return dict(row)

    async def upsert(self, payload: BusinessUpsertRequest) -> dict[str, Any]:
        params = {
            "id": payload.id,
            "name": payload.name,
            "description": payload.description,
            "city": payload.city,
            "shipping_policy": payload.shipping_policy,
            "delivery_zones": _json_dumps(payload.delivery_zones),
            "payment_methods": _json_dumps(payload.payment_methods),
            "profile_metadata": _json_dumps(payload.profile_metadata),
        }

        if payload.id is None:
            query = text(
                """
                INSERT INTO business (
                    name, description, city, shipping_policy,
                    delivery_zones, payment_methods, profile_metadata
                )
                VALUES (
                    :name, :description, :city, :shipping_policy,
                    CAST(:delivery_zones AS jsonb),
                    CAST(:payment_methods AS jsonb),
                    CAST(:profile_metadata AS jsonb)
                )
                RETURNING id, name, description, city, shipping_policy,
                          delivery_zones, payment_methods, profile_metadata,
                          created_at, updated_at
                """
            )
        else:
            query = text(
                """
                UPDATE business
                SET name = :name,
                    description = :description,
                    city = :city,
                    shipping_policy = :shipping_policy,
                    delivery_zones = CAST(:delivery_zones AS jsonb),
                    payment_methods = CAST(:payment_methods AS jsonb),
                    profile_metadata = CAST(:profile_metadata AS jsonb),
                    updated_at = timezone('utc', now())
                WHERE id = :id
                RETURNING id, name, description, city, shipping_policy,
                          delivery_zones, payment_methods, profile_metadata,
                          created_at, updated_at
                """
            )

        result = await self.session.execute(query, params)
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Business {payload.id} was not found.",
            )
        return dict(row)

    async def update_dashboard_profile(
        self, business_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                UPDATE business
                SET name = :name,
                    description = :description,
                    city = :city,
                    shipping_policy = :shipping_policy,
                    delivery_zones = CAST(:delivery_zones AS jsonb),
                    payment_methods = CAST(:payment_methods AS jsonb),
                    profile_metadata = CAST(:profile_metadata AS jsonb),
                    updated_at = timezone('utc', now())
                WHERE id = :business_id
                RETURNING id, name, description, city, shipping_policy,
                          delivery_zones, payment_methods, profile_metadata,
                          created_at, updated_at
                """
            ),
            {
                "business_id": business_id,
                "name": payload["name"],
                "description": payload["description"],
                "city": payload["city"],
                "shipping_policy": payload["shipping_policy"],
                "delivery_zones": _json_dumps(payload["delivery_zones"]),
                "payment_methods": _json_dumps(payload["payment_methods"]),
                "profile_metadata": _json_dumps(payload["profile_metadata"]),
            },
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Business {business_id} was not found.",
            )
        return dict(row)

    async def upsert_profile_knowledge(
        self,
        business_id: int,
        title: str,
        content: str,
        metadata: dict[str, Any],
        embedding: list[float],
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                INSERT INTO business_knowledge (
                    business_id, source_type, source_id, title, content, metadata, embedding
                )
                VALUES (
                    :business_id, 'profile', :business_id, :title, :content,
                    CAST(:metadata AS jsonb), CAST(:embedding AS vector)
                )
                ON CONFLICT (business_id, source_type, source_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    embedding = EXCLUDED.embedding,
                    updated_at = timezone('utc', now())
                RETURNING id, business_id, source_type, source_id, title, content, metadata,
                          created_at, updated_at
                """
            ),
            {
                "business_id": business_id,
                "title": title,
                "content": content,
                "metadata": _json_dumps(metadata),
                "embedding": to_vector_literal(embedding),
            },
        )
        return dict(result.mappings().one())

    async def search_knowledge(
        self, business_id: int, embedding: list[float], limit: int
    ) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, title, content, metadata, source_type,
                       1 - (embedding <=> CAST(:embedding AS vector)) AS score
                FROM business_knowledge
                WHERE business_id = :business_id
                  AND embedding IS NOT NULL
                ORDER BY embedding <=> CAST(:embedding AS vector)
                LIMIT :limit
                """
            ),
            {
                "business_id": business_id,
                "embedding": to_vector_literal(embedding),
                "limit": limit,
            },
        )
        return [dict(row) for row in result.mappings().all()]


class BusinessMembershipRepository:
    _BUSINESS_COLUMNS = """
        b.id, b.name, b.description, b.city, b.shipping_policy,
        b.delivery_zones, b.payment_methods, b.profile_metadata,
        b.created_at, b.updated_at
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def count_businesses_for_user(self, auth_user_id: str) -> int:
        result = await self.session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM business_memberships
                WHERE auth_user_id = :auth_user_id
                """
            ),
            {"auth_user_id": auth_user_id},
        )
        return int(result.scalar_one())

    async def upsert_membership(
        self,
        *,
        auth_user_id: str,
        email: str | None,
        business_id: int,
        role: str,
        is_default: bool,
    ) -> dict[str, Any]:
        if is_default:
            await self.session.execute(
                text(
                    """
                    UPDATE business_memberships
                    SET is_default = FALSE,
                        updated_at = timezone('utc', now())
                    WHERE auth_user_id = :auth_user_id
                    """
                ),
                {"auth_user_id": auth_user_id},
            )
        result = await self.session.execute(
            text(
                """
                INSERT INTO business_memberships (
                    auth_user_id, email, business_id, role, is_default
                )
                VALUES (
                    :auth_user_id, :email, :business_id, :role, :is_default
                )
                ON CONFLICT (auth_user_id, business_id)
                DO UPDATE SET
                    email = COALESCE(EXCLUDED.email, business_memberships.email),
                    role = EXCLUDED.role,
                    is_default = EXCLUDED.is_default,
                    updated_at = timezone('utc', now())
                RETURNING id, auth_user_id, email, business_id, role, is_default, created_at, updated_at
                """
            ),
            {
                "auth_user_id": auth_user_id,
                "email": email,
                "business_id": business_id,
                "role": role,
                "is_default": is_default,
            },
        )
        return dict(result.mappings().one())

    async def require_business_access(self, *, auth_user_id: str, business_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                SELECT id, auth_user_id, email, business_id, role, is_default, created_at, updated_at
                FROM business_memberships
                WHERE auth_user_id = :auth_user_id
                  AND business_id = :business_id
                """
            ),
            {
                "auth_user_id": auth_user_id,
                "business_id": business_id,
            },
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You do not have access to this business.",
            )
        return dict(row)

    async def list_businesses_for_user(self, auth_user_id: str) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                f"""
                SELECT {self._BUSINESS_COLUMNS},
                       m.role,
                       m.is_default
                FROM business_memberships m
                JOIN business b ON b.id = m.business_id
                WHERE m.auth_user_id = :auth_user_id
                ORDER BY m.is_default DESC, m.created_at ASC, b.id ASC
                """
            ),
            {"auth_user_id": auth_user_id},
        )
        return [dict(row) for row in result.mappings().all()]

    async def get_current_business_for_user(self, auth_user_id: str) -> dict[str, Any] | None:
        result = await self.session.execute(
            text(
                f"""
                SELECT {self._BUSINESS_COLUMNS},
                       m.role,
                       m.is_default
                FROM business_memberships m
                JOIN business b ON b.id = m.business_id
                WHERE m.auth_user_id = :auth_user_id
                ORDER BY m.is_default DESC, m.created_at ASC, b.id ASC
                LIMIT 1
                """
            ),
            {"auth_user_id": auth_user_id},
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None


class ProductRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_id(self, business_id: int, product_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, external_id, name, description, price, currency,
                       category, availability, variants, tags, metadata,
                       created_at, updated_at
                FROM products
                WHERE business_id = :business_id AND id = :product_id
                """
            ),
            {"business_id": business_id, "product_id": product_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} was not found for business {business_id}.",
            )
        return dict(row)

    async def get_by_product_id(self, product_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, external_id, name, description, price, currency,
                       category, availability, variants, tags, metadata,
                       created_at, updated_at
                FROM products
                WHERE id = :product_id
                """
            ),
            {"product_id": product_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} was not found.",
            )
        return dict(row)

    async def list_by_business(self, business_id: int) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, external_id, name, description, price, currency,
                       category, availability, variants, tags, metadata,
                       created_at, updated_at
                FROM products
                WHERE business_id = :business_id
                ORDER BY updated_at DESC, id DESC
                """
            ),
            {"business_id": business_id},
        )
        return [dict(row) for row in result.mappings().all()]

    async def list_dashboard(
        self, business_id: int, search: str | None = None, category: str | None = None
    ) -> tuple[list[dict[str, Any]], int, list[str]]:
        where_clause, params = _build_where_clause(
            business_id=business_id, search=search, category=category
        )

        rows_result = await self.session.execute(
            text(
                f"""
                SELECT id, business_id, external_id, name, description, price, currency,
                       category, availability, variants, tags, metadata,
                       created_at, updated_at
                FROM products
                WHERE {where_clause}
                ORDER BY updated_at DESC, id DESC
                """
            ),
            params,
        )
        count_result = await self.session.execute(
            text(f"SELECT COUNT(*) FROM products WHERE {where_clause}"),
            params,
        )
        categories_result = await self.session.execute(
            text(
                """
                SELECT DISTINCT category
                FROM products
                WHERE business_id = :business_id
                  AND category IS NOT NULL
                  AND category <> ''
                ORDER BY category
                """
            ),
            {"business_id": business_id},
        )

        return (
            [dict(row) for row in rows_result.mappings().all()],
            int(count_result.scalar() or 0),
            [row[0] for row in categories_result.all()],
        )

    async def count_by_business(self, business_id: int) -> int:
        result = await self.session.execute(
            text("SELECT COUNT(*) FROM products WHERE business_id = :business_id"),
            {"business_id": business_id},
        )
        return int(result.scalar() or 0)

    async def count_active_by_business(self, business_id: int) -> int:
        result = await self.session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM products
                WHERE business_id = :business_id
                  AND COALESCE(availability, 'in_stock') <> 'out_of_stock'
                """
            ),
            {"business_id": business_id},
        )
        return int(result.scalar() or 0)

    async def recent_by_business(self, business_id: int, limit: int) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, external_id, name, description, price, currency,
                       category, availability, variants, tags, metadata,
                       created_at, updated_at
                FROM products
                WHERE business_id = :business_id
                ORDER BY updated_at DESC, id DESC
                LIMIT :limit
                """
            ),
            {"business_id": business_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]

    async def upsert(self, payload: ProductUpsertRequest) -> dict[str, Any]:
        item = ProductBulkItem.model_validate(payload.model_dump(exclude={"business_id"}))
        return await self._upsert_item(payload.business_id, item)

    async def bulk_upsert(
        self, payload: BulkProductUpsertRequest
    ) -> list[dict[str, Any]]:
        products: list[dict[str, Any]] = []
        for product in payload.products:
            products.append(await self._upsert_item(payload.business_id, product))
        return products

    async def create_dashboard_product(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                INSERT INTO products (
                    business_id, external_id, name, description, price, currency,
                    category, availability, variants, tags, metadata
                )
                VALUES (
                    :business_id, :external_id, :name, :description, :price, :currency,
                    :category, :availability, CAST(:variants AS jsonb),
                    CAST(:tags AS jsonb), CAST(:metadata AS jsonb)
                )
                RETURNING id, business_id, external_id, name, description, price, currency,
                          category, availability, variants, tags, metadata,
                          created_at, updated_at
                """
            ),
            {
                **payload,
                "variants": _json_dumps(payload["variants"]),
                "tags": _json_dumps(payload.get("tags", [])),
                "metadata": _json_dumps(payload.get("metadata", {})),
            },
        )
        return dict(result.mappings().one())

    async def update_dashboard_product(
        self, product_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                UPDATE products
                SET external_id = :external_id,
                    name = :name,
                    description = :description,
                    price = :price,
                    currency = :currency,
                    category = :category,
                    availability = :availability,
                    variants = CAST(:variants AS jsonb),
                    tags = CAST(:tags AS jsonb),
                    metadata = CAST(:metadata AS jsonb),
                    updated_at = timezone('utc', now())
                WHERE id = :product_id
                RETURNING id, business_id, external_id, name, description, price, currency,
                          category, availability, variants, tags, metadata,
                          created_at, updated_at
                """
            ),
            {
                "product_id": product_id,
                **payload,
                "variants": _json_dumps(payload["variants"]),
                "tags": _json_dumps(payload.get("tags", [])),
                "metadata": _json_dumps(payload.get("metadata", {})),
            },
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} was not found.",
            )
        return dict(row)

    async def delete(self, product_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                DELETE FROM products
                WHERE id = :product_id
                RETURNING id, business_id
                """
            ),
            {"product_id": product_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} was not found.",
            )
        return dict(row)

    async def _upsert_item(
        self, business_id: int, product: ProductBulkItem
    ) -> dict[str, Any]:
        params = {
            "id": product.id,
            "business_id": business_id,
            "external_id": product.external_id,
            "name": product.name,
            "description": product.description,
            "price": product.price,
            "currency": product.currency,
            "category": product.category,
            "availability": product.availability,
            "variants": _json_dumps(product.variants),
            "tags": _json_dumps(product.tags),
            "metadata": _json_dumps(product.metadata),
        }

        if product.id is not None:
            query = text(
                """
                UPDATE products
                SET external_id = :external_id,
                    name = :name,
                    description = :description,
                    price = :price,
                    currency = :currency,
                    category = :category,
                    availability = :availability,
                    variants = CAST(:variants AS jsonb),
                    tags = CAST(:tags AS jsonb),
                    metadata = CAST(:metadata AS jsonb),
                    updated_at = timezone('utc', now())
                WHERE business_id = :business_id AND id = :id
                RETURNING id, business_id, external_id, name, description, price, currency,
                          category, availability, variants, tags, metadata,
                          created_at, updated_at
                """
            )
        elif product.external_id:
            query = text(
                """
                INSERT INTO products (
                    business_id, external_id, name, description, price, currency,
                    category, availability, variants, tags, metadata
                )
                VALUES (
                    :business_id, :external_id, :name, :description, :price, :currency,
                    :category, :availability, CAST(:variants AS jsonb),
                    CAST(:tags AS jsonb), CAST(:metadata AS jsonb)
                )
                ON CONFLICT (business_id, external_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    price = EXCLUDED.price,
                    currency = EXCLUDED.currency,
                    category = EXCLUDED.category,
                    availability = EXCLUDED.availability,
                    variants = EXCLUDED.variants,
                    tags = EXCLUDED.tags,
                    metadata = EXCLUDED.metadata,
                    updated_at = timezone('utc', now())
                RETURNING id, business_id, external_id, name, description, price, currency,
                          category, availability, variants, tags, metadata,
                          created_at, updated_at
                """
            )
        else:
            query = text(
                """
                INSERT INTO products (
                    business_id, name, description, price, currency,
                    category, availability, variants, tags, metadata
                )
                VALUES (
                    :business_id, :name, :description, :price, :currency,
                    :category, :availability, CAST(:variants AS jsonb),
                    CAST(:tags AS jsonb), CAST(:metadata AS jsonb)
                )
                RETURNING id, business_id, external_id, name, description, price, currency,
                          category, availability, variants, tags, metadata,
                          created_at, updated_at
                """
            )

        result = await self.session.execute(query, params)
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product.id} was not found for business {business_id}.",
            )
        return dict(row)

    async def update_embedding(self, product_id: int, embedding: list[float]) -> None:
        await self.session.execute(
            text(
                """
                UPDATE products
                SET embedding = CAST(:embedding AS vector),
                    updated_at = timezone('utc', now())
                WHERE id = :product_id
                """
            ),
            {
                "product_id": product_id,
                "embedding": to_vector_literal(embedding),
            },
        )

    async def search(
        self, business_id: int, embedding: list[float], limit: int
    ) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, name, description, price, currency, category, availability,
                       metadata,
                       1 - (embedding <=> CAST(:embedding AS vector)) AS score
                FROM products
                WHERE business_id = :business_id
                  AND embedding IS NOT NULL
                ORDER BY embedding <=> CAST(:embedding AS vector)
                LIMIT :limit
                """
            ),
            {
                "business_id": business_id,
                "embedding": to_vector_literal(embedding),
                "limit": limit,
            },
        )
        return [dict(row) for row in result.mappings().all()]


class FAQRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_id(self, business_id: int, faq_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, external_id, question, answer, metadata,
                       created_at, updated_at
                FROM faqs
                WHERE business_id = :business_id AND id = :faq_id
                """
            ),
            {"business_id": business_id, "faq_id": faq_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"FAQ {faq_id} was not found for business {business_id}.",
            )
        return dict(row)

    async def list_by_business(self, business_id: int) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, external_id, question, answer, metadata,
                       created_at, updated_at
                FROM faqs
                WHERE business_id = :business_id
                ORDER BY updated_at DESC, id DESC
                """
            ),
            {"business_id": business_id},
        )
        return [dict(row) for row in result.mappings().all()]

    async def upsert(self, payload: FAQUpsertRequest) -> dict[str, Any]:
        params = {
            "id": payload.id,
            "business_id": payload.business_id,
            "external_id": payload.external_id,
            "question": payload.question,
            "answer": payload.answer,
            "metadata": _json_dumps(payload.metadata),
        }

        if payload.id is not None:
            query = text(
                """
                UPDATE faqs
                SET external_id = :external_id,
                    question = :question,
                    answer = :answer,
                    metadata = CAST(:metadata AS jsonb),
                    updated_at = timezone('utc', now())
                WHERE business_id = :business_id AND id = :id
                RETURNING id, business_id, external_id, question, answer, metadata,
                          created_at, updated_at
                """
            )
        elif payload.external_id:
            query = text(
                """
                INSERT INTO faqs (business_id, external_id, question, answer, metadata)
                VALUES (
                    :business_id, :external_id, :question, :answer,
                    CAST(:metadata AS jsonb)
                )
                ON CONFLICT (business_id, external_id)
                DO UPDATE SET
                    question = EXCLUDED.question,
                    answer = EXCLUDED.answer,
                    metadata = EXCLUDED.metadata,
                    updated_at = timezone('utc', now())
                RETURNING id, business_id, external_id, question, answer, metadata,
                          created_at, updated_at
                """
            )
        else:
            query = text(
                """
                INSERT INTO faqs (business_id, question, answer, metadata)
                VALUES (
                    :business_id, :question, :answer,
                    CAST(:metadata AS jsonb)
                )
                RETURNING id, business_id, external_id, question, answer, metadata,
                          created_at, updated_at
                """
            )

        result = await self.session.execute(query, params)
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"FAQ {payload.id} was not found for business {payload.business_id}.",
            )
        return dict(row)

    async def replace_for_business(
        self, business_id: int, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        keep_ids: list[int] = []

        for item in items:
            raw_id = item.get("id")
            faq_id = int(raw_id) if raw_id and str(raw_id).isdigit() else None
            if faq_id is not None:
                result = await self.session.execute(
                    text(
                        """
                        UPDATE faqs
                        SET question = :question,
                            answer = :answer,
                            updated_at = timezone('utc', now())
                        WHERE business_id = :business_id AND id = :faq_id
                        RETURNING id
                        """
                    ),
                    {
                        "business_id": business_id,
                        "faq_id": faq_id,
                        "question": item["question"],
                        "answer": item["answer"],
                    },
                )
                row = result.mappings().first()
                if row is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"FAQ {faq_id} was not found for business {business_id}.",
                    )
                keep_ids.append(int(row["id"]))
                continue

            result = await self.session.execute(
                text(
                    """
                    INSERT INTO faqs (business_id, question, answer, metadata)
                    VALUES (:business_id, :question, :answer, '{}'::jsonb)
                    RETURNING id
                    """
                ),
                {
                    "business_id": business_id,
                    "question": item["question"],
                    "answer": item["answer"],
                },
            )
            keep_ids.append(int(result.mappings().one()["id"]))

        if keep_ids:
            await self.session.execute(
                text(
                    """
                    DELETE FROM faqs
                    WHERE business_id = :business_id
                      AND id != ALL(:keep_ids)
                    """
                ),
                {"business_id": business_id, "keep_ids": keep_ids},
            )
        else:
            await self.session.execute(
                text("DELETE FROM faqs WHERE business_id = :business_id"),
                {"business_id": business_id},
            )

        return await self.list_by_business(business_id)

    async def update_embedding(self, faq_id: int, embedding: list[float]) -> None:
        await self.session.execute(
            text(
                """
                UPDATE faqs
                SET embedding = CAST(:embedding AS vector),
                    updated_at = timezone('utc', now())
                WHERE id = :faq_id
                """
            ),
            {
                "faq_id": faq_id,
                "embedding": to_vector_literal(embedding),
            },
        )

    async def search(
        self, business_id: int, embedding: list[float], limit: int
    ) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, question, answer, metadata,
                       1 - (embedding <=> CAST(:embedding AS vector)) AS score
                FROM faqs
                WHERE business_id = :business_id
                  AND embedding IS NOT NULL
                ORDER BY embedding <=> CAST(:embedding AS vector)
                LIMIT :limit
                """
            ),
            {
                "business_id": business_id,
                "embedding": to_vector_literal(embedding),
                "limit": limit,
            },
        )
        return [dict(row) for row in result.mappings().all()]


class ChatRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_message(self, message_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, phone, customer_name, text, direction, intent,
                       needs_human, is_read, provider, provider_message_sid,
                       provider_status, error_code, raw_payload, created_at, updated_at
                FROM chat_messages
                WHERE id = :message_id
                """
            ),
            {"message_id": message_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat message {message_id} was not found.",
            )
        return dict(row)

    async def list_messages(
        self,
        business_id: int,
        *,
        phone: str | None = None,
        intent: str | None = None,
        direction: str | None = None,
        needs_human: bool | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        conditions = ["business_id = :business_id"]
        params: dict[str, Any] = {"business_id": business_id}

        if phone:
            conditions.append("phone ILIKE :phone")
            params["phone"] = f"%{phone.strip()}%"
        if intent:
            conditions.append("intent = :intent")
            params["intent"] = intent
        if direction:
            conditions.append("direction = :direction")
            params["direction"] = direction
        if needs_human is not None:
            conditions.append("needs_human = :needs_human")
            params["needs_human"] = needs_human

        limit_clause = ""
        if limit is not None:
            params["limit"] = limit
            limit_clause = "LIMIT :limit"

        result = await self.session.execute(
            text(
                f"""
                SELECT id, business_id, phone, customer_name, text, direction, intent,
                       needs_human, is_read, provider, provider_message_sid,
                       provider_status, error_code, raw_payload, created_at, updated_at
                FROM chat_messages
                WHERE {' AND '.join(conditions)}
                ORDER BY created_at DESC, id DESC
                {limit_clause}
                """
            ),
            params,
        )
        return [dict(row) for row in result.mappings().all()]

    async def get_thread(self, business_id: int, phone: str) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, phone, customer_name, text, direction, intent,
                       needs_human, is_read, provider, provider_message_sid,
                       provider_status, error_code, raw_payload, created_at, updated_at
                FROM chat_messages
                WHERE business_id = :business_id
                  AND phone = :phone
                ORDER BY created_at ASC, id ASC
                """
            ),
            {"business_id": business_id, "phone": phone},
        )
        return [dict(row) for row in result.mappings().all()]

    async def count_messages(self, business_id: int) -> int:
        result = await self.session.execute(
            text("SELECT COUNT(*) FROM chat_messages WHERE business_id = :business_id"),
            {"business_id": business_id},
        )
        return int(result.scalar() or 0)

    async def count_conversations(self, business_id: int) -> int:
        result = await self.session.execute(
            text("SELECT COUNT(DISTINCT phone) FROM chat_messages WHERE business_id = :business_id"),
            {"business_id": business_id},
        )
        return int(result.scalar() or 0)

    async def upsert_message(
        self,
        *,
        business_id: int,
        phone: str,
        customer_name: str | None,
        text: str,
        direction: str,
        intent: str | None,
        needs_human: bool,
        is_read: bool,
        provider: str | None,
        provider_message_sid: str | None,
        provider_status: str | None,
        error_code: str | None,
        raw_payload: dict[str, Any],
    ) -> dict[str, Any]:
        result = await self.session.execute(
            sql_text(
                """
                INSERT INTO chat_messages (
                    business_id, phone, customer_name, text, direction, intent,
                    needs_human, is_read, provider, provider_message_sid,
                    provider_status, error_code, raw_payload
                )
                VALUES (
                    :business_id, :phone, :customer_name, :text, :direction, :intent,
                    :needs_human, :is_read, :provider, :provider_message_sid,
                    :provider_status, :error_code, CAST(:raw_payload AS jsonb)
                )
                ON CONFLICT (provider_message_sid)
                DO UPDATE SET
                    customer_name = COALESCE(EXCLUDED.customer_name, chat_messages.customer_name),
                    text = EXCLUDED.text,
                    provider_status = COALESCE(EXCLUDED.provider_status, chat_messages.provider_status),
                    error_code = EXCLUDED.error_code,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = timezone('utc', now())
                RETURNING id, business_id, phone, customer_name, text, direction, intent,
                          needs_human, is_read, provider, provider_message_sid,
                          provider_status, error_code, raw_payload, created_at, updated_at
                """
            ),
            {
                "business_id": business_id,
                "phone": normalize_phone_number(phone),
                "customer_name": customer_name,
                "text": text,
                "direction": direction,
                "intent": intent,
                "needs_human": needs_human,
                "is_read": is_read,
                "provider": provider,
                "provider_message_sid": provider_message_sid,
                "provider_status": provider_status,
                "error_code": error_code,
                "raw_payload": _json_dumps(raw_payload),
            },
        )
        return dict(result.mappings().one())

    async def update_provider_status(
        self,
        *,
        provider_message_sid: str,
        provider_status: str | None,
        error_code: str | None,
        raw_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        result = await self.session.execute(
            text(
                """
                UPDATE chat_messages
                SET provider_status = :provider_status,
                    error_code = :error_code,
                    raw_payload = CAST(:raw_payload AS jsonb),
                    updated_at = timezone('utc', now())
                WHERE provider_message_sid = :provider_message_sid
                RETURNING id, business_id, phone, customer_name, text, direction, intent,
                          needs_human, is_read, provider, provider_message_sid,
                          provider_status, error_code, raw_payload, created_at, updated_at
                """
            ),
            {
                "provider_message_sid": provider_message_sid,
                "provider_status": provider_status,
                "error_code": error_code,
                "raw_payload": _json_dumps(raw_payload),
            },
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def update_message_analysis(
        self,
        message_id: int,
        *,
        intent: str | None,
        needs_human: bool,
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                UPDATE chat_messages
                SET intent = :intent,
                    needs_human = :needs_human,
                    updated_at = timezone('utc', now())
                WHERE id = :message_id
                RETURNING id, business_id, phone, customer_name, text, direction, intent,
                          needs_human, is_read, provider, provider_message_sid,
                          provider_status, error_code, raw_payload, created_at, updated_at
                """
            ),
            {
                "message_id": message_id,
                "intent": intent,
                "needs_human": needs_human,
            },
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chat message {message_id} was not found.",
            )
        return dict(row)


class AIRunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_run(
        self,
        *,
        business_id: int,
        phone: str | None,
        inbound_chat_message_id: int | None,
        outbound_chat_message_id: int | None,
        provider: str,
        model: str,
        status_value: str,
        customer_message: str,
        language: str | None,
        intent: str | None,
        needs_human: bool,
        confidence: float,
        reply_text: str | None,
        fallback_reason: str | None,
        retrieval_summary: dict[str, Any],
        prompt_version: str,
        request_payload: dict[str, Any],
        response_payload: dict[str, Any],
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                INSERT INTO ai_message_runs (
                    business_id, phone, inbound_chat_message_id, outbound_chat_message_id,
                    provider, model, status, customer_message, language, intent,
                    needs_human, confidence, reply_text, fallback_reason,
                    retrieval_summary, prompt_version, request_payload, response_payload
                )
                VALUES (
                    :business_id, :phone, :inbound_chat_message_id, :outbound_chat_message_id,
                    :provider, :model, :status, :customer_message, :language, :intent,
                    :needs_human, :confidence, :reply_text, :fallback_reason,
                    CAST(:retrieval_summary AS jsonb), :prompt_version,
                    CAST(:request_payload AS jsonb), CAST(:response_payload AS jsonb)
                )
                RETURNING id, business_id, phone, inbound_chat_message_id, outbound_chat_message_id,
                          provider, model, status, customer_message, language, intent,
                          needs_human, confidence, reply_text, fallback_reason,
                          retrieval_summary, prompt_version, request_payload, response_payload,
                          created_at, updated_at
                """
            ),
            {
                "business_id": business_id,
                "phone": normalize_phone_number(phone) if phone else None,
                "inbound_chat_message_id": inbound_chat_message_id,
                "outbound_chat_message_id": outbound_chat_message_id,
                "provider": provider,
                "model": model,
                "status": status_value,
                "customer_message": customer_message,
                "language": language,
                "intent": intent,
                "needs_human": needs_human,
                "confidence": confidence,
                "reply_text": reply_text,
                "fallback_reason": fallback_reason,
                "retrieval_summary": _json_dumps(retrieval_summary),
                "prompt_version": prompt_version,
                "request_payload": _json_dumps(request_payload),
                "response_payload": _json_dumps(response_payload),
            },
        )
        return dict(result.mappings().one())

    async def update_run(
        self,
        run_id: int,
        *,
        status_value: str,
        outbound_chat_message_id: int | None = None,
        fallback_reason: str | None = None,
        response_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                UPDATE ai_message_runs
                SET status = :status,
                    outbound_chat_message_id = COALESCE(CAST(:outbound_chat_message_id AS bigint), outbound_chat_message_id),
                    fallback_reason = COALESCE(CAST(:fallback_reason AS text), fallback_reason),
                    response_payload = COALESCE(CAST(:response_payload AS jsonb), response_payload),
                    updated_at = timezone('utc', now())
                WHERE id = :run_id
                RETURNING id, business_id, phone, inbound_chat_message_id, outbound_chat_message_id,
                          provider, model, status, customer_message, language, intent,
                          needs_human, confidence, reply_text, fallback_reason,
                          retrieval_summary, prompt_version, request_payload, response_payload,
                          created_at, updated_at
                """
            ),
            {
                "run_id": run_id,
                "status": status_value,
                "outbound_chat_message_id": outbound_chat_message_id,
                "fallback_reason": fallback_reason,
                "response_payload": _json_dumps(response_payload) if response_payload is not None else None,
            },
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"AI run {run_id} was not found.",
            )
        return dict(row)

    async def list_runs(self, business_id: int, limit: int = 50) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, phone, inbound_chat_message_id, outbound_chat_message_id,
                       provider, model, status, customer_message, language, intent,
                       needs_human, confidence, reply_text, fallback_reason,
                       retrieval_summary, prompt_version, request_payload, response_payload,
                       created_at, updated_at
                FROM ai_message_runs
                WHERE business_id = :business_id
                ORDER BY created_at DESC, id DESC
                LIMIT :limit
                """
            ),
            {"business_id": business_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]

    async def get_run(self, business_id: int, run_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, phone, inbound_chat_message_id, outbound_chat_message_id,
                       provider, model, status, customer_message, language, intent,
                       needs_human, confidence, reply_text, fallback_reason,
                       retrieval_summary, prompt_version, request_payload, response_payload,
                       created_at, updated_at
                FROM ai_message_runs
                WHERE business_id = :business_id
                  AND id = :run_id
                """
            ),
            {"business_id": business_id, "run_id": run_id},
        )
        row = result.mappings().first()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"AI run {run_id} was not found for business {business_id}.",
            )
        return dict(row)


class IntegrationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_connections(self, business_id: int) -> list[dict[str, Any]]:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, integration_type, status, health, config, metrics,
                       last_activity_at, last_synced_at, created_at, updated_at
                FROM integration_connections
                WHERE business_id = :business_id
                """
            ),
            {"business_id": business_id},
        )
        return [dict(row) for row in result.mappings().all()]

    async def get_connection(
        self, business_id: int, integration_type: str
    ) -> dict[str, Any] | None:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, integration_type, status, health, config, metrics,
                       last_activity_at, last_synced_at, created_at, updated_at
                FROM integration_connections
                WHERE business_id = :business_id
                  AND integration_type = :integration_type
                """
            ),
            {"business_id": business_id, "integration_type": integration_type},
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def upsert_connection(
        self,
        *,
        business_id: int,
        integration_type: str,
        status_value: str,
        health: str,
        config: dict[str, Any],
        metrics: dict[str, Any],
        last_activity_at: Any = None,
        last_synced_at: Any = None,
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                INSERT INTO integration_connections (
                    business_id, integration_type, status, health, config, metrics,
                    last_activity_at, last_synced_at
                )
                VALUES (
                    :business_id, :integration_type, :status, :health,
                    CAST(:config AS jsonb), CAST(:metrics AS jsonb),
                    :last_activity_at, :last_synced_at
                )
                ON CONFLICT (business_id, integration_type)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    health = EXCLUDED.health,
                    config = EXCLUDED.config,
                    metrics = EXCLUDED.metrics,
                    last_activity_at = EXCLUDED.last_activity_at,
                    last_synced_at = EXCLUDED.last_synced_at,
                    updated_at = timezone('utc', now())
                RETURNING id, business_id, integration_type, status, health, config, metrics,
                          last_activity_at, last_synced_at, created_at, updated_at
                """
            ),
            {
                "business_id": business_id,
                "integration_type": integration_type,
                "status": status_value,
                "health": health,
                "config": _json_dumps(config),
                "metrics": _json_dumps(metrics),
                "last_activity_at": last_activity_at,
                "last_synced_at": last_synced_at,
            },
        )
        return dict(result.mappings().one())

    async def find_whatsapp_connection(
        self, *, sender_phone: str, subaccount_sid: str | None = None
    ) -> dict[str, Any] | None:
        normalized_sender = normalize_phone_number(sender_phone)
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, integration_type, status, health, config, metrics,
                       last_activity_at, last_synced_at, created_at, updated_at
                FROM integration_connections
                WHERE integration_type = 'whatsapp'
                  AND (
                    config->>'whatsapp_number' = :sender_phone
                    OR config->>'phone_number' = :sender_phone
                    OR (:subaccount_sid <> '' AND config->>'subaccount_sid' = :subaccount_sid)
                  )
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ),
            {
                "sender_phone": normalized_sender,
                "subaccount_sid": subaccount_sid or "",
            },
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def find_shopify_connection(self, *, shop_domain: str) -> dict[str, Any] | None:
        result = await self.session.execute(
            text(
                """
                SELECT id, business_id, integration_type, status, health, config, metrics,
                       last_activity_at, last_synced_at, created_at, updated_at
                FROM integration_connections
                WHERE integration_type = 'shopify'
                  AND config->>'shop_domain' = :shop_domain
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ),
            {"shop_domain": shop_domain.strip().lower()},
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def increment_whatsapp_metrics(
        self,
        business_id: int,
        *,
        received_delta: int = 0,
        sent_delta: int = 0,
        failed_delta: int = 0,
        touch_last_activity: bool = False,
    ) -> dict[str, Any] | None:
        connection = await self.get_connection(business_id, "whatsapp")
        if connection is None:
            return None

        metrics = {
            "received_messages_last_30_days": 0,
            "sent_messages_last_30_days": 0,
            "failed_messages_last_30_days": 0,
        }
        metrics.update(dict(connection.get("metrics") or {}))
        metrics["received_messages_last_30_days"] = (
            int(metrics.get("received_messages_last_30_days") or 0) + received_delta
        )
        metrics["sent_messages_last_30_days"] = (
            int(metrics.get("sent_messages_last_30_days") or 0) + sent_delta
        )
        metrics["failed_messages_last_30_days"] = (
            int(metrics.get("failed_messages_last_30_days") or 0) + failed_delta
        )

        return await self.upsert_connection(
            business_id=business_id,
            integration_type="whatsapp",
            status_value=connection["status"],
            health=connection["health"],
            config=dict(connection.get("config") or {}),
            metrics=metrics,
            last_activity_at=connection.get("last_activity_at")
            if not touch_last_activity
            else datetime.now(UTC),
            last_synced_at=connection.get("last_synced_at"),
        )


class SyncStatusRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_status(self, business_id: int) -> dict[str, Any] | None:
        result = await self.session.execute(
            text(
                """
                SELECT business_id, status, last_synced_at, last_result,
                       synced_products, synced_business_knowledge, synced_faqs,
                       embedding_model, created_at, updated_at
                FROM embedding_sync_status
                WHERE business_id = :business_id
                """
            ),
            {"business_id": business_id},
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def mark_running(self, business_id: int, embedding_model: str) -> dict[str, Any]:
        return await self.upsert_status(
            business_id=business_id,
            status_value="running",
            last_synced_at=None,
            last_result="Embedding sync is running.",
            synced_products=0,
            synced_business_knowledge=0,
            synced_faqs=0,
            embedding_model=embedding_model,
        )

    async def mark_error(
        self, business_id: int, message: str, embedding_model: str
    ) -> dict[str, Any]:
        counts = await self.get_embedding_counts(business_id)
        return await self.upsert_status(
            business_id=business_id,
            status_value="error",
            last_synced_at=counts.get("last_embedded_at"),
            last_result=message,
            synced_products=int(counts.get("synced_products") or 0),
            synced_business_knowledge=int(counts.get("synced_business_knowledge") or 0),
            synced_faqs=int(counts.get("synced_faqs") or 0),
            embedding_model=embedding_model,
        )

    async def upsert_status(
        self,
        *,
        business_id: int,
        status_value: str,
        last_synced_at: Any,
        last_result: str | None,
        synced_products: int,
        synced_business_knowledge: int,
        synced_faqs: int,
        embedding_model: str,
    ) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                INSERT INTO embedding_sync_status (
                    business_id, status, last_synced_at, last_result,
                    synced_products, synced_business_knowledge, synced_faqs,
                    embedding_model
                )
                VALUES (
                    :business_id, :status, :last_synced_at, :last_result,
                    :synced_products, :synced_business_knowledge, :synced_faqs,
                    :embedding_model
                )
                ON CONFLICT (business_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    last_synced_at = EXCLUDED.last_synced_at,
                    last_result = EXCLUDED.last_result,
                    synced_products = EXCLUDED.synced_products,
                    synced_business_knowledge = EXCLUDED.synced_business_knowledge,
                    synced_faqs = EXCLUDED.synced_faqs,
                    embedding_model = EXCLUDED.embedding_model,
                    updated_at = timezone('utc', now())
                RETURNING business_id, status, last_synced_at, last_result,
                          synced_products, synced_business_knowledge, synced_faqs,
                          embedding_model, created_at, updated_at
                """
            ),
            {
                "business_id": business_id,
                "status": status_value,
                "last_synced_at": last_synced_at,
                "last_result": last_result,
                "synced_products": synced_products,
                "synced_business_knowledge": synced_business_knowledge,
                "synced_faqs": synced_faqs,
                "embedding_model": embedding_model,
            },
        )
        return dict(result.mappings().one())

    async def get_embedding_counts(self, business_id: int) -> dict[str, Any]:
        result = await self.session.execute(
            text(
                """
                WITH embedded_updates AS (
                    SELECT updated_at FROM products
                    WHERE business_id = :business_id AND embedding IS NOT NULL
                    UNION ALL
                    SELECT updated_at FROM faqs
                    WHERE business_id = :business_id AND embedding IS NOT NULL
                    UNION ALL
                    SELECT updated_at FROM business_knowledge
                    WHERE business_id = :business_id AND embedding IS NOT NULL
                )
                SELECT
                    (SELECT COUNT(*) FROM products
                     WHERE business_id = :business_id AND embedding IS NOT NULL) AS synced_products,
                    (SELECT COUNT(*) FROM business_knowledge
                     WHERE business_id = :business_id AND embedding IS NOT NULL) AS synced_business_knowledge,
                    (SELECT COUNT(*) FROM faqs
                     WHERE business_id = :business_id AND embedding IS NOT NULL) AS synced_faqs,
                    (SELECT MAX(updated_at) FROM embedded_updates) AS last_embedded_at
                """
            ),
            {"business_id": business_id},
        )
        row = result.mappings().one()
        return dict(row)
