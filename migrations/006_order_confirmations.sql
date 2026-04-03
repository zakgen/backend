CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    source_store TEXT NOT NULL,
    external_order_id TEXT NOT NULL,
    customer_name TEXT,
    customer_phone TEXT NOT NULL,
    preferred_language TEXT,
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'MAD',
    payment_method TEXT,
    delivery_city TEXT,
    delivery_address TEXT,
    order_notes TEXT,
    items JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending_confirmation',
    confirmation_status TEXT NOT NULL DEFAULT 'pending_send',
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    CONSTRAINT orders_business_source_external_unique UNIQUE (business_id, source_store, external_order_id)
);

CREATE TABLE IF NOT EXISTS order_confirmation_sessions (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    phone TEXT NOT NULL,
    customer_name TEXT,
    preferred_language TEXT,
    status TEXT NOT NULL DEFAULT 'pending_send',
    needs_human BOOLEAN NOT NULL DEFAULT false,
    last_detected_intent TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    last_customer_message_at TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    declined_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    last_outbound_message_sid TEXT,
    structured_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS order_confirmation_events (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    session_id BIGINT NOT NULL REFERENCES order_confirmation_sessions(id) ON DELETE CASCADE,
    order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE INDEX IF NOT EXISTS idx_orders_business_id ON orders (business_id);
CREATE INDEX IF NOT EXISTS idx_orders_phone ON orders (customer_phone);
CREATE INDEX IF NOT EXISTS idx_order_confirmation_sessions_business_phone
ON order_confirmation_sessions (business_id, phone);
CREATE INDEX IF NOT EXISTS idx_order_confirmation_sessions_status
ON order_confirmation_sessions (business_id, status);
CREATE INDEX IF NOT EXISTS idx_order_confirmation_events_session_id
ON order_confirmation_events (session_id);

