CREATE TABLE IF NOT EXISTS ai_message_runs (
    id BIGSERIAL PRIMARY KEY,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    phone TEXT,
    inbound_chat_message_id BIGINT REFERENCES chat_messages(id) ON DELETE SET NULL,
    outbound_chat_message_id BIGINT REFERENCES chat_messages(id) ON DELETE SET NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('generated', 'sent', 'escalated', 'failed')),
    customer_message TEXT NOT NULL,
    language TEXT,
    intent TEXT,
    needs_human BOOLEAN NOT NULL DEFAULT FALSE,
    confidence NUMERIC(4, 3) NOT NULL DEFAULT 0,
    reply_text TEXT,
    fallback_reason TEXT,
    retrieval_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    prompt_version TEXT NOT NULL,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE INDEX IF NOT EXISTS idx_ai_message_runs_business_id
ON ai_message_runs (business_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ai_message_runs_inbound_message
ON ai_message_runs (inbound_chat_message_id);
