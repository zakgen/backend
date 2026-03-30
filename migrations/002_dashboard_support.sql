CREATE TABLE IF NOT EXISTS chat_messages (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    phone TEXT NOT NULL,
    customer_name TEXT,
    text TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    intent TEXT CHECK (
        intent IN ('livraison', 'prix', 'disponibilite', 'retour', 'paiement', 'infos_produit', 'autre')
    ),
    needs_human BOOLEAN NOT NULL DEFAULT FALSE,
    is_read BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_business_phone
ON chat_messages (business_id, phone, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_chat_messages_business_filters
ON chat_messages (business_id, direction, intent, needs_human);

CREATE TABLE IF NOT EXISTS integration_connections (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    integration_type TEXT NOT NULL CHECK (
        integration_type IN ('whatsapp', 'youcan', 'shopify', 'woocommerce', 'zid')
    ),
    status TEXT NOT NULL CHECK (status IN ('connected', 'disconnected')),
    health TEXT NOT NULL CHECK (health IN ('healthy', 'attention')),
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_activity_at TIMESTAMPTZ,
    last_synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    CONSTRAINT integration_connections_unique UNIQUE (business_id, integration_type)
);

CREATE INDEX IF NOT EXISTS idx_integration_connections_business
ON integration_connections (business_id, integration_type);

CREATE TABLE IF NOT EXISTS embedding_sync_status (
    business_id BIGINT PRIMARY KEY REFERENCES business(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('up_to_date', 'recommended', 'running', 'error')),
    last_synced_at TIMESTAMPTZ,
    last_result TEXT,
    synced_products INTEGER NOT NULL DEFAULT 0,
    synced_business_knowledge INTEGER NOT NULL DEFAULT 0,
    synced_faqs INTEGER NOT NULL DEFAULT 0,
    embedding_model TEXT NOT NULL DEFAULT 'text-embedding-3-small',
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);
