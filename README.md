# ZakBot RAG Backend MVP

ZakBot is a production-minded FastAPI backend that gives an n8n workflow business-scoped context for incoming customer messages. It is designed for Moroccan e-commerce stores using a WhatsApp AI sales assistant.

## Stack

- Python 3.11
- FastAPI
- Supabase Postgres
- pgvector
- OpenAI embeddings API
- SQLAlchemy async
- Pydantic v2
- Docker

## What the API does

- Upserts business profile data
- Upserts products and optional FAQs
- Generates and stores embeddings for products, FAQs, and business profile knowledge
- Performs business-scoped semantic search only
- Exposes dashboard-oriented business, product, chat, integration, and sync-status routes
- Supports provider-agnostic messaging with a Twilio WhatsApp implementation
- Returns stable JSON for n8n consumption

## Repository layout

```text
app/
  main.py
  config.py
  routers/
  schemas/
  services/
  utils/
migrations/
scripts/
tests/
Dockerfile
.env.example
README.md
```

## Environment variables

Copy `.env.example` to `.env` and fill in:

- `OPENAI_API_KEY`: OpenAI API key used for embeddings
- `SUPABASE_URL`: Supabase project URL, documented for future admin integrations
- `SUPABASE_SERVICE_ROLE_KEY`: Supabase service role key, documented for future admin integrations
- `DB_URL`: Async SQLAlchemy connection string used when the API runs on your host machine
- `DOCKER_DB_URL`: Async SQLAlchemy connection string used by the API container in Docker Compose
- `LOCAL_DB_NAME`: Local Docker Postgres database name
- `LOCAL_DB_USER`: Local Docker Postgres username
- `LOCAL_DB_PASSWORD`: Local Docker Postgres password
- `LOCAL_DB_PORT`: Local Docker Postgres port exposed on your machine
- `EMBEDDING_MODEL`: Default `text-embedding-3-small`
- `PORT`: API port, default `8000`
- `LOG_LEVEL`: Logging verbosity, default `INFO`
- `CORS_ALLOW_ORIGINS`: Comma-separated browser origins allowed to call the API
- `SEARCH_MIN_SCORE`: Minimum semantic score returned in `matches`
- `TWILIO_ACCOUNT_SID`: Master Twilio account SID used to create/manage subaccounts
- `TWILIO_AUTH_TOKEN`: Master Twilio auth token
- `PUBLIC_WEBHOOK_BASE_URL`: Public base URL used to build Twilio status callback URLs

Default local `DB_URL` for host-machine development:

```text
postgresql+asyncpg://postgres:postgres@localhost:5432/zakbot
```

Default local `DOCKER_DB_URL` for Docker Compose:

```text
postgresql+asyncpg://postgres:postgres@db:5432/zakbot
```

Supabase example `DB_URL`:

```text
postgresql+asyncpg://postgres:password@db.<project-ref>.supabase.co:5432/postgres?sslmode=require
```

If you use the Supabase pooler on port `6543`, use a URL like:

```text
postgresql+asyncpg://postgres.<project-ref>:password@aws-0-<region>.pooler.supabase.com:6543/postgres?sslmode=require
```

The app auto-adjusts two things for Supabase:

- adds `sslmode=require` if it is missing
- disables asyncpg prepared statement caching and uses `NullPool` when the URL points at the Supabase pooler

## Local setup

### Option 1: uv

```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Option 2: pip

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Option 3: requirements.txt

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Local database setup

This repo now includes a local `pgvector` Postgres instance in [`docker-compose.yml`](/Users/zakariaimzilen/Pro/Biz/ZakBot/rag/docker-compose.yml).

What you get:

- `pgvector` enabled
- migrations auto-applied on the first database boot
- a named Docker volume, so data survives container stop/start
- the volume is only deleted if you run `docker compose down -v`

Start the local database:

```bash
docker compose up -d db
```

If the database volume already exists or you add new migrations later, apply them with:

```bash
python scripts/apply_migrations.py
```

You can then run the API either on your host machine or in Docker Compose.

### Host-machine API + local Docker DB

Use the default `DB_URL` from `.env.example`:

```text
postgresql+asyncpg://postgres:postgres@localhost:5432/zakbot
```

Then run:

```bash
python -m uvicorn app.main:app --reload --port 8000
```

### Full Docker Compose

The API container uses `DOCKER_DB_URL`, which defaults to:

```text
postgresql+asyncpg://postgres:postgres@db:5432/zakbot
```

Run:

```bash
docker compose up --build
```

## Supabase setup

If you want to use Supabase instead of the local database:

1. Create or reuse a Supabase project.
2. Open the Supabase SQL editor.
3. Run `migrations/001_init.sql`.
4. Run `migrations/002_dashboard_support.sql`.
5. Run `migrations/003_twilio_messaging.sql`.
6. Confirm `pgvector` is enabled and the tables were created.

## Run the API

```bash
uvicorn app.main:app --reload --port 8000
```

Or with Docker:

```bash
docker compose up --build
```

## Seed sample data

```bash
python scripts/seed_sample_data.py
```

This inserts:

- `Boutique Lina`
- sample products
- one shipping FAQ
- embeddings for the seeded business

## API endpoints

- `GET /health`
- `POST /business/upsert`
- `GET /business/{id}`
- `PUT /business/{id}`
- `GET /business/{id}/overview`
- `GET /business/{id}/chats`
- `GET /business/{id}/chats/{phone}`
- `POST /business/{id}/integrations/whatsapp/connect`
- `POST /business/{id}/integrations/whatsapp/disconnect`
- `POST /business/{id}/integrations/whatsapp/test`
- `GET /business/{id}/integrations`
- `POST /products/upsert`
- `POST /products/bulk-upsert`
- `GET /business/{id}/products`
- `POST /products`
- `PUT /products/{id}`
- `DELETE /products/{id}`
- `POST /products/bulk`
- `POST /faqs/upsert`
- `GET /embeddings/sync/business/{business_id}/status`
- `POST /embeddings/sync/business/{business_id}`
- `POST /business/{id}/chats/{phone}/reply`
- `POST /webhooks/twilio/whatsapp/inbound`
- `POST /webhooks/twilio/whatsapp/status`
- `POST /search`

## Twilio WhatsApp flow

ZakBot now has a provider-agnostic messaging layer and a Twilio implementation.

Production onboarding is admin-assisted:

1. Merchant calls `POST /business/{id}/integrations/whatsapp/connect`.
2. Backend creates or reuses a Twilio subaccount and stores a pending WhatsApp integration.
3. Admin completes Twilio sender onboarding outside the product.
4. Admin finalizes the backend linkage with:

```bash
python scripts/finalize_twilio_connection.py \
  --business-id 1 \
  --subaccount-sid ACSUBACCOUNT123 \
  --sender-sid XE123456789 \
  --whatsapp-number +212600000001
```

5. The business can then receive Twilio webhooks and send dashboard replies.

Twilio webhook endpoints:

- `POST /webhooks/twilio/whatsapp/inbound`
- `POST /webhooks/twilio/whatsapp/status`

Dashboard reply endpoint:

```bash
curl -X POST http://localhost:8000/business/1/chats/+212600000001/reply \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Salam, oui la livraison a Rabat est disponible.",
    "intent": "livraison"
  }'
```

## Example requests

### Health

```bash
curl http://localhost:8000/health
```

### Upsert a business

```bash
curl -X POST http://localhost:8000/business/upsert \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Boutique Lina",
    "description": "Boutique marocaine de mode feminine.",
    "city": "Rabat",
    "shipping_policy": "Livraison partout au Maroc",
    "delivery_zones": ["Rabat", "Sale", "Casablanca"],
    "payment_methods": ["cash_on_delivery"],
    "profile_metadata": {
      "store_type": "fashion",
      "language": "fr-darija"
    }
  }'
```

### Upsert a product

```bash
curl -X POST http://localhost:8000/products/upsert \
  -H "Content-Type: application/json" \
  -d '{
    "business_id": 1,
    "external_id": "robe-satin-noire",
    "name": "Robe satin noire",
    "description": "Robe elegante",
    "price": 299,
    "currency": "MAD",
    "category": "fashion",
    "availability": "in_stock",
    "variants": ["S", "M", "L"],
    "tags": ["robe", "satin"],
    "metadata": {
      "color": "black"
    }
  }'
```

### Bulk upsert products

```bash
curl -X POST http://localhost:8000/products/bulk-upsert \
  -H "Content-Type: application/json" \
  -d '{
    "business_id": 1,
    "products": [
      {
        "external_id": "robe-satin-noire",
        "name": "Robe satin noire",
        "description": "Robe elegante",
        "price": 299,
        "currency": "MAD",
        "category": "fashion",
        "availability": "in_stock"
      },
      {
        "external_id": "sac-cuir-beige",
        "name": "Sac cuir beige",
        "description": "Sac pratique",
        "price": 189,
        "currency": "MAD",
        "category": "accessories",
        "availability": "in_stock"
      }
    ]
  }'
```

### Upsert an FAQ

```bash
curl -X POST http://localhost:8000/faqs/upsert \
  -H "Content-Type: application/json" \
  -d '{
    "business_id": 1,
    "external_id": "shipping-rabat",
    "question": "Kayn livraison l Rabat?",
    "answer": "Oui, livraison disponible a Rabat avec paiement a la livraison.",
    "metadata": {
      "topic": "shipping"
    }
  }'
```

### Re-sync embeddings for one business

```bash
curl -X POST http://localhost:8000/embeddings/sync/business/1
```

### Search from n8n

Request:

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{
    "business_id": 2,
    "query": "Salam, wach kayn livraison l Rabat?",
    "top_k": 5
  }'
```

Response shape:

```json
{
  "business_id": 2,
  "query": "Salam, wach kayn livraison l Rabat?",
  "matches": [
    {
      "type": "faq",
      "id": 4,
      "name": "Kayn livraison l Rabat?",
      "description": "Oui, livraison disponible a Rabat et Sale avec paiement a la livraison.",
      "price": null,
      "currency": null,
      "score": 0.91,
      "metadata": {
        "topic": "shipping",
        "confidence_label": "high"
      }
    },
    {
      "type": "business_knowledge",
      "id": 1,
      "name": "Boutique Lina",
      "description": "Business: Boutique Lina. City: Rabat. Shipping policy: Livraison partout au Maroc.",
      "price": null,
      "currency": null,
      "score": 0.82,
      "metadata": {
        "source_type": "profile",
        "confidence_label": "medium"
      }
    }
  ],
  "business_context": {
    "id": 2,
    "name": "Boutique Lina",
    "description": "Boutique marocaine de mode feminine et accessoires.",
    "city": "Rabat",
    "shipping_policy": "Livraison partout au Maroc sous 24 a 72h.",
    "delivery_zones": ["Rabat", "Sale", "Casablanca", "Marrakech"],
    "payment_methods": ["cash_on_delivery"],
    "profile_metadata": {
      "language": "fr-darija",
      "store_type": "fashion"
    }
  }
}
```

## Search behavior

- Every search query is filtered by `business_id`.
- Products, FAQs, and business knowledge are stored separately but searched under the same business scope.
- If product similarity is weak, `business_context` still returns core store information for the workflow.
- `confidence_label` helps n8n branch logic without parsing raw scores only.

## Suggested n8n usage

1. Receive WhatsApp message.
2. Extract the correct `business_id` from the store/workspace context.
3. Call `POST /search`.
4. Pass `matches` and `business_context` into the LLM prompt.
5. Use `confidence_label` or low result count to decide when to ask clarifying questions.

## Notes

- This MVP uses direct Postgres access instead of the Supabase Python client because vector search is simpler and easier to maintain through SQL.
- `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are documented in case you later add signed admin operations or file storage flows.
- The embedding dimension in SQL is fixed to `1536`, which matches `text-embedding-3-small`.
