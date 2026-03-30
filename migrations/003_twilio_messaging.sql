ALTER TABLE chat_messages
ADD COLUMN IF NOT EXISTS provider TEXT,
ADD COLUMN IF NOT EXISTS provider_message_sid TEXT,
ADD COLUMN IF NOT EXISTS provider_status TEXT,
ADD COLUMN IF NOT EXISTS error_code TEXT,
ADD COLUMN IF NOT EXISTS raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now());

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chat_messages_provider_message_sid_unique'
    ) THEN
        ALTER TABLE chat_messages
        ADD CONSTRAINT chat_messages_provider_message_sid_unique UNIQUE (provider_message_sid);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_chat_messages_provider_message_sid
ON chat_messages (provider_message_sid);
