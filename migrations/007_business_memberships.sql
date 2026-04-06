CREATE TABLE IF NOT EXISTS business_memberships (
    id BIGSERIAL PRIMARY KEY,
    auth_user_id TEXT NOT NULL,
    email TEXT,
    business_id BIGINT NOT NULL REFERENCES business(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'owner',
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),
    CONSTRAINT business_memberships_user_business_unique UNIQUE (auth_user_id, business_id)
);

CREATE INDEX IF NOT EXISTS idx_business_memberships_auth_user_id
ON business_memberships (auth_user_id, is_default DESC, created_at ASC);
