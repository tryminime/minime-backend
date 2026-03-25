-- Database Migration: Add Settings and AI Chat Tables
-- Generated: 2026-01-31
-- Description: Creates tables for user settings, 2FA, backups, conversations, and AI interactions

-- ============================================================================
-- USER SETTINGS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_settings (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    
    -- Tracking settings
    tracking_enabled BOOLEAN DEFAULT true,
    track_projects BOOLEAN DEFAULT true,
    track_files BOOLEAN DEFAULT true,
    track_commits BOOLEAN DEFAULT true,
    track_documents BOOLEAN DEFAULT true,
    track_ide BOOLEAN DEFAULT true,
    track_browser BOOLEAN DEFAULT true,
    track_writing BOOLEAN DEFAULT true,
    track_communication BOOLEAN DEFAULT true,
    track_video_calls BOOLEAN DEFAULT true,
    idle_threshold_minutes INTEGER DEFAULT 5,
    pause_on_lock BOOLEAN DEFAULT true,
    
    -- Focus settings
    focus_enabled BOOLEAN DEFAULT true,
    auto_detect_deep_work BOOLEAN DEFAULT true,
    min_duration_minutes INTEGER DEFAULT 30,
    default_duration_minutes INTEGER DEFAULT 90,
    auto_break_minutes INTEGER DEFAULT 15,
    
    -- Privacy settings
    https_only BOOLEAN DEFAULT true,
    filter_credit_cards BOOLEAN DEFAULT true,
    filter_ssn BOOLEAN DEFAULT true,
    filter_api_keys BOOLEAN DEFAULT true,
    filter_emails BOOLEAN DEFAULT true,
    local_encryption BOOLEAN DEFAULT true,
    e2e_encryption BOOLEAN DEFAULT true,
    retention_days INTEGER DEFAULT 365,
    auto_delete BOOLEAN DEFAULT true,
    
    -- Notification settings
    in_app_enabled BOOLEAN DEFAULT true,
    email_enabled BOOLEAN DEFAULT true,
    browser_enabled BOOLEAN DEFAULT true,
    daily_summary BOOLEAN DEFAULT true,
    deadline_reminders BOOLEAN DEFAULT true,
    focus_reminders BOOLEAN DEFAULT true,
    break_suggestions BOOLEAN DEFAULT true,
    wellness_summary BOOLEAN DEFAULT true,
    ai_insights BOOLEAN DEFAULT true,
    sync_errors BOOLEAN DEFAULT true,
    dnd_enabled BOOLEAN DEFAULT true,
    dnd_from VARCHAR(5) DEFAULT '18:00',
    dnd_to VARCHAR(5) DEFAULT '09:00',
    
    -- Appearance & profile
    theme VARCHAR(20) DEFAULT 'system',
    profile JSONB DEFAULT '{}',
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_user_settings_user_id ON user_settings(user_id);

-- ============================================================================
-- TWO-FACTOR AUTHENTICATION TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS two_factor_auth (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    
    -- TOTP configuration (encrypted in production)
    secret VARCHAR(255) NOT NULL,
    backup_codes JSONB DEFAULT '[]',
    
    -- Status
    enabled BOOLEAN DEFAULT false,
    verified BOOLEAN DEFAULT false,
    
    -- Timestamps
    enabled_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_2fa_user_id ON two_factor_auth(user_id);

-- ============================================================================
-- BACKUPS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS backups (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Backup metadata
    filename VARCHAR(255) NOT NULL,
    size_bytes BIGINT DEFAULT 0,
    format VARCHAR(10) DEFAULT 'zip',
    
    -- Storage info
    storage_type VARCHAR(50) DEFAULT 'local',
    storage_path VARCHAR(500) NOT NULL,
    
    -- Status
    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX idx_backups_user_id ON backups(user_id);
CREATE INDEX idx_backups_created_at ON backups(created_at);

-- ============================================================================
-- CONVERSATIONS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS conversations (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Conversation metadata
    title VARCHAR(255),
    summary TEXT,
    
    -- Settings
    context_enabled BOOLEAN DEFAULT true,
    
    -- Status
    archived BOOLEAN DEFAULT false,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_message_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_conversations_user_id ON conversations(user_id);
CREATE INDEX idx_conversations_last_message ON conversations(last_message_at DESC);
CREATE INDEX idx_conversations_archived ON conversations(archived);

-- ============================================================================
-- CHAT MESSAGES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS chat_messages (
    id VARCHAR(36) PRIMARY KEY,
    conversation_id VARCHAR(36) NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    
    -- Message content
    role VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    
    -- AI metadata
    model VARCHAR(100),
    tokens INTEGER,
    context_used JSONB,
    
    -- Timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_chat_messages_conversation_id ON chat_messages(conversation_id);
CREATE INDEX idx_chat_messages_created_at ON chat_messages(created_at);

-- ============================================================================
-- AI INTERACTIONS TABLE (for analytics and billing)
-- ============================================================================

CREATE TABLE IF NOT EXISTS ai_interactions (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Interaction details
    interaction_type VARCHAR(50) NOT NULL,
    model VARCHAR(100),
    
    -- Usage metrics
    tokens_input INTEGER DEFAULT 0,
    tokens_output INTEGER DEFAULT 0,
    tokens_total INTEGER DEFAULT 0,
    
    -- Cost tracking (in cents)
    cost_usd INTEGER DEFAULT 0,
    
    -- Performance
    latency_ms INTEGER,
    
    -- Status
    success BOOLEAN DEFAULT true,
    error_message TEXT,
    
    -- Timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ai_interactions_user_id ON ai_interactions(user_id);
CREATE INDEX idx_ai_interactions_created_at ON ai_interactions(created_at);
CREATE INDEX idx_ai_interactions_type ON ai_interactions(interaction_type);

-- ============================================================================
-- SEED DEFAULT SETTINGS FOR EXISTING USERS
-- ============================================================================

-- Create default settings for all existing users who don't have them
INSERT INTO user_settings (id, user_id)
SELECT gen_random_uuid()::text, u.id
FROM users u
WHERE NOT EXISTS (
    SELECT 1 FROM user_settings us WHERE us.user_id = u.id
);

-- ============================================================================
-- TRIGGERS FOR UPDATED_AT
-- ============================================================================

-- Create function for updating timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add triggers
CREATE TRIGGER update_user_settings_updated_at
    BEFORE UPDATE ON user_settings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_2fa_updated_at
    BEFORE UPDATE ON two_factor_auth
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_conversations_updated_at
    BEFORE UPDATE ON conversations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Migration complete!
