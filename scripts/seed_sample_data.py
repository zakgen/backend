from __future__ import annotations

import asyncio

from app.schemas.business import BusinessUpsertRequest
from app.schemas.faq import FAQUpsertRequest
from app.schemas.product import BulkProductUpsertRequest, ProductBulkItem
from app.services.database import get_session_factory
from app.services.embedding_service import EmbeddingService
from app.services.repositories import BusinessRepository, FAQRepository, ProductRepository
from app.services.sync_service import SyncService


async def main() -> None:
    session_factory = get_session_factory()

    async with session_factory() as session:
        business_repository = BusinessRepository(session)
        product_repository = ProductRepository(session)
        faq_repository = FAQRepository(session)

        business = await business_repository.upsert(
            BusinessUpsertRequest(
                name="Boutique Lina",
                description="Boutique marocaine de mode feminine et accessoires.",
                city="Rabat",
                shipping_policy="Livraison partout au Maroc sous 24 a 72h.",
                delivery_zones=["Rabat", "Sale", "Casablanca", "Marrakech"],
                payment_methods=["cash_on_delivery"],
                profile_metadata={"language": "fr-darija", "store_type": "fashion"},
            )
        )

        await product_repository.bulk_upsert(
            BulkProductUpsertRequest(
                business_id=business["id"],
                products=[
                    ProductBulkItem(
                        external_id="robe-satin-noire",
                        name="Robe satin noire",
                        description="Robe elegante pour sorties et occasions.",
                        price=299,
                        currency="MAD",
                        category="fashion",
                        availability="in_stock",
                        variants=["S", "M", "L"],
                        tags=["robe", "satin", "soir"],
                        metadata={"color": "black"},
                    ),
                    ProductBulkItem(
                        external_id="sac-cuir-beige",
                        name="Sac cuir beige",
                        description="Sac pratique en cuir synthetique beige.",
                        price=189,
                        currency="MAD",
                        category="accessories",
                        availability="in_stock",
                        tags=["sac", "beige"],
                        metadata={"material": "synthetic_leather"},
                    ),
                ],
            )
        )

        await faq_repository.upsert(
            FAQUpsertRequest(
                business_id=business["id"],
                external_id="shipping-rabat",
                question="Kayn livraison l Rabat?",
                answer="Oui, livraison disponible a Rabat et Sale avec paiement a la livraison.",
                metadata={"topic": "shipping"},
            )
        )

        sync_service = SyncService(session=session, embedding_service=EmbeddingService())
        await sync_service.sync_business_embeddings(business["id"])
        await session.commit()

        print(f"Seeded business_id={business['id']}")


if __name__ == "__main__":
    asyncio.run(main())
