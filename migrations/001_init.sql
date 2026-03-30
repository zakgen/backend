CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS business (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    city TEXT,
    shipping_policy TEXT,
    delivery_zones JSONB NOT NULL DEFAULT '[]'::jsonb,
    payment_methods JSONB NOT NULL DEFAULT '[]'::jsonb,
    profile_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS products (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    external_id TEXT,
    name TEXT NOT NULL,
    description TEXT,
    price NUMERIC(12, 2),
    currency TEXT NOT NULL DEFAULT 'MAD',
    category TEXT,
    availability TEXT,
    variants JSONB NOT NULL DEFAULT '[]'::jsonb,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    embedding VECTOR(1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    CONSTRAINT products_business_external_id_unique UNIQUE NULLS NOT DISTINCT (business_id, external_id)
);

CREATE TABLE IF NOT EXISTS faqs (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    external_id TEXT,
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    embedding VECTOR(1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    CONSTRAINT faqs_business_external_id_unique UNIQUE NULLS NOT DISTINCT (business_id, external_id)
);

CREATE TABLE IF NOT EXISTS business_knowledge (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    source_type TEXT NOT NULL,
    source_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    embedding VECTOR(1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    CONSTRAINT business_knowledge_source_unique UNIQUE (business_id, source_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_products_business_id ON products (business_id);
CREATE INDEX IF NOT EXISTS idx_faqs_business_id ON faqs (business_id);
CREATE INDEX IF NOT EXISTS idx_business_knowledge_business_id ON business_knowledge (business_id);

CREATE INDEX IF NOT EXISTS idx_products_embedding
ON products USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_faqs_embedding
ON faqs USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_business_knowledge_embedding
ON business_knowledge USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

