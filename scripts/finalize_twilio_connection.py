from __future__ import annotations

import argparse
import asyncio

from app.services.database import get_session_factory
from app.services.repository_factory import RepositoryFactory
from app.utils.phones import normalize_phone_number


async def main() -> None:
    parser = argparse.ArgumentParser(description="Finalize a Twilio WhatsApp connection.")
    parser.add_argument("--business-id", type=int, required=True)
    parser.add_argument("--subaccount-sid", required=True)
    parser.add_argument("--sender-sid", required=True)
    parser.add_argument("--whatsapp-number", required=True)
    args = parser.parse_args()

    session_factory = get_session_factory()
    async with session_factory() as session:
        factory = RepositoryFactory(session)
        business = await factory.business().get_by_id(args.business_id)
        repository = factory.integrations()
        existing = await repository.get_connection(args.business_id, "whatsapp")
        if existing is None:
            raise SystemExit(f"Business {args.business_id} has no pending WhatsApp integration.")

        config = dict(existing.get("config") or {})
        config.update(
            {
                "provider": "twilio",
                "subaccount_sid": args.subaccount_sid,
                "sender_sid": args.sender_sid,
                "phone_number": normalize_phone_number(args.whatsapp_number),
                "whatsapp_number": normalize_phone_number(args.whatsapp_number),
                "business_name": config.get("business_name") or business["name"],
                "onboarding_status": "connected",
            }
        )
        metrics = {
            "received_messages_last_30_days": 0,
            "sent_messages_last_30_days": 0,
            "failed_messages_last_30_days": 0,
        }
        metrics.update(dict(existing.get("metrics") or {}))

        await repository.upsert_connection(
            business_id=args.business_id,
            integration_type="whatsapp",
            status_value="connected",
            health="healthy",
            config=config,
            metrics=metrics,
            last_activity_at=existing.get("last_activity_at"),
            last_synced_at=existing.get("last_synced_at"),
        )
        await session.commit()
        print(f"Twilio connection finalized for business_id={args.business_id}")


if __name__ == "__main__":
    asyncio.run(main())
