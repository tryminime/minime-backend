-- Phase 3b: Add synced_at columns and sync_history table
-- Run against local PostgreSQL: psql -h localhost -U minime -d minime -f 003_cloud_sync.sql

-- synced_at watermarks for incremental sync
ALTER TABLE entities ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;
ALTER TABLE activity_entity_links ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;
ALTER TABLE user_goals ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;
-- activities already has synced_at

-- Sync history table
CREATE TABLE IF NOT EXISTS sync_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    trigger VARCHAR(20) NOT NULL DEFAULT 'manual',
    results JSONB DEFAULT '{}',
    error TEXT,
    records_synced INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_history_user_id ON sync_history(user_id);
CREATE INDEX IF NOT EXISTS idx_sync_history_started_at ON sync_history(started_at);
