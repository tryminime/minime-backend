-- Integration OAuth Tokens Table
-- Stores OAuth access tokens and refresh tokens for third-party integrations

CREATE TABLE IF NOT EXISTS integrations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    provider VARCHAR(50) NOT NULL,  -- 'github', 'google', 'notion'
    access_token TEXT NOT NULL,
    refresh_token TEXT,
    token_expires_at TIMESTAMP,
    username VARCHAR(255),
    email VARCHAR(255),
    external_id VARCHAR(255),
    provider_metadata JSONB DEFAULT '{}',
    connected BOOLEAN DEFAULT TRUE,
    last_synced_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_user_provider UNIQUE(user_id, provider)
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_integrations_user_provider ON integrations(user_id, provider);
CREATE INDEX IF NOT EXISTS idx_integrations_user_id ON integrations(user_id);
CREATE INDEX IF NOT EXISTS idx_integrations_provider ON integrations(provider);
CREATE INDEX IF NOT EXISTS idx_integrations_connected ON integrations(connected);

-- Comments
COMMENT ON TABLE integrations IS 'Stores OAuth integration credentials for GitHub, Google Calendar, and Notion';
COMMENT ON COLUMN integrations.provider IS 'Integration provider: github, google, notion';
COMMENT ON COLUMN integrations.access_token IS 'OAuth access token (encrypted at rest in production)';
COMMENT ON COLUMN integrations.refresh_token IS 'OAuth refresh token for token renewal';
COMMENT ON COLUMN integrations.metadata IS 'Provider-specific metadata (scopes, permissions, etc.)';
