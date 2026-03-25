-- =====================================================
-- MINIME DATABASE SCHEMA (PostgreSQL 15)
-- Event Store & Application Data
-- =====================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =====================================================
-- USERS TABLE
-- =====================================================
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    display_name VARCHAR(255),
    avatar_url TEXT,
    bio TEXT,
    timezone VARCHAR(100) DEFAULT 'UTC',
    tier VARCHAR(50) DEFAULT 'free' CHECK (tier IN ('free', 'premium', 'enterprise')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_login TIMESTAMP,
    email_verified BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    deleted_at TIMESTAMP NULL
);

CREATE INDEX idx_users_email ON users(email) WHERE deleted_at IS NULL;
CREATE INDEX idx_users_tier ON users(tier);
CREATE INDEX idx_users_active ON users(is_active) WHERE is_active = true;

-- =====================================================
-- ACTIVITY EVENTS TABLE (Partitioned by month)
-- =====================================================
CREATE TABLE activity_events (
    id UUID DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL CHECK (event_type IN ('window', 'browser', 'email', 'calendar', 'document', 'meeting', 'mobile', 'wearable')),
    source VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    duration_seconds INTEGER,
    application VARCHAR(255),
    title VARCHAR(500),
    url TEXT,
    domain VARCHAR(255),
    category VARCHAR(100),
    raw_data JSONB,
    enriched BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Create partitions for 2026 (expand as needed)
CREATE TABLE activity_events_2026_01 PARTITION OF activity_events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE activity_events_2026_02 PARTITION OF activity_events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE activity_events_2026_03 PARTITION OF activity_events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE activity_events_2026_04 PARTITION OF activity_events
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE activity_events_2026_05 PARTITION OF activity_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE activity_events_2026_06 PARTITION OF activity_events
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE INDEX idx_activity_user_time ON activity_events(user_id, timestamp DESC);
CREATE INDEX idx_activity_type ON activity_events(event_type);
CREATE INDEX idx_activity_domain ON activity_events(domain);
CREATE INDEX idx_activity_enriched ON activity_events(enriched) WHERE enriched = false;
CREATE INDEX idx_activity_raw_data ON activity_events USING GIN(raw_data);

-- =====================================================
-- ENTITIES TABLE
-- =====================================================
CREATE TABLE entities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    entity_type VARCHAR(50) NOT NULL CHECK (entity_type IN ('person', 'project', 'skill', 'concept', 'organization', 'artifact', 'event', 'interaction')),
    name VARCHAR(500) NOT NULL,
    canonical_id UUID,
    confidence FLOAT DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    first_seen TIMESTAMP DEFAULT NOW(),
    last_seen TIMESTAMP DEFAULT NOW(),
    occurrence_count INTEGER DEFAULT 1,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_entities_user_type ON entities(user_id, entity_type);
CREATE INDEX idx_entities_canonical ON entities(canonical_id);
CREATE INDEX idx_entities_name ON entities USING GIN(to_tsvector('english', name));
CREATE INDEX idx_entities_metadata ON entities USING GIN(metadata);

-- =====================================================
-- ACTIVITY ENTITY LINKS (Many-to-Many)
-- =====================================================
CREATE TABLE activity_entity_links (
    activity_id UUID NOT NULL,
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    relevance_score FLOAT DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (activity_id, entity_id)
);

CREATE INDEX idx_ael_entity ON activity_entity_links(entity_id);
CREATE INDEX idx_ael_activity ON activity_entity_links(activity_id);

-- =====================================================
-- SESSIONS TABLE (for authentication)
-- =====================================================
CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    refresh_token VARCHAR(500) UNIQUE NOT NULL,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    revoked BOOLEAN DEFAULT false
);

CREATE INDEX idx_sessions_user ON sessions(user_id);
CREATE INDEX idx_sessions_token ON sessions(refresh_token) WHERE revoked = false;
CREATE INDEX idx_sessions_expires ON sessions(expires_at) WHERE revoked = false;

-- =====================================================
-- AUDIT LOGS TABLE (for compliance)
-- =====================================================
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(100),
    resource_id UUID,
    ip_address INET,
    user_agent TEXT,
    timestamp TIMESTAMP DEFAULT NOW(),
    metadata JSONB,
    status VARCHAR(20) DEFAULT 'success' CHECK (status IN ('success', 'failure', 'error'))
);

CREATE INDEX idx_audit_user_time ON audit_logs(user_id, timestamp DESC);
CREATE INDEX idx_audit_action ON audit_logs(action);
CREATE INDEX idx_audit_resource ON audit_logs(resource_type, resource_id);
CREATE INDEX idx_audit_timestamp ON audit_logs(timestamp DESC);

-- =====================================================
-- USER SETTINGS TABLE
-- =====================================================
CREATE TABLE user_settings (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    privacy_settings JSONB DEFAULT '{}',
    notification_preferences JSONB DEFAULT '{}',
    tracking_filters JSONB DEFAULT '{}',
    ui_preferences JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- =====================================================
-- INTEGRATIONS TABLE
-- =====================================================
CREATE TABLE integrations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    integration_type VARCHAR(50) NOT NULL CHECK (integration_type IN ('google', 'microsoft', 'github', 'slack', 'jira', 'notion', 'calendar', 'email')),
    credentials_encrypted TEXT,
    is_active BOOLEAN DEFAULT true,
    last_sync TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, integration_type)
);

CREATE INDEX idx_integrations_user ON integrations(user_id);
CREATE INDEX idx_integrations_type ON integrations(integration_type);

-- =====================================================
-- ANALYTICS CACHE TABLE (pre-computed metrics)
-- =====================================================
CREATE TABLE analytics_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    metric_type VARCHAR(100) NOT NULL,
    time_range VARCHAR(50) NOT NULL,
    computed_at TIMESTAMP NOT NULL,
    data JSONB NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    UNIQUE(user_id, metric_type, time_range)
);

CREATE INDEX idx_analytics_user_type ON analytics_cache(user_id, metric_type);
CREATE INDEX idx_analytics_expires ON analytics_cache(expires_at);

-- =====================================================
-- FUNCTIONS & TRIGGERS
-- =====================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_entities_updated_at BEFORE UPDATE ON entities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_settings_updated_at BEFORE UPDATE ON user_settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_integrations_updated_at BEFORE UPDATE ON integrations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- INITIAL DATA
-- =====================================================

-- Insert system user for background processes
INSERT INTO users (id, email, password_hash, full_name, tier, email_verified, is_active)
VALUES ('00000000-0000-0000-0000-000000000000', 'system@minime.ai', 'SYSTEM', 'System User', 'enterprise', true, true);

-- Grant necessary permissions
-- (Add role-based access control as needed)
