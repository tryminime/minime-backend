-- Migration 001: Extend Activity Schema for Robust Ingestion
-- Description: Add fields for multi-source activity ingestion with deduplication
-- Date: 2026-02-03
-- Author: MiniMe Development Team

-- =====================================================
-- PART 1: ADD NEW COLUMNS
-- =====================================================

-- Add source version tracking
ALTER TABLE activities
ADD COLUMN IF NOT EXISTS source_version VARCHAR(50);

-- Add client-generated ID for deduplication
ALTER TABLE activities
ADD COLUMN IF NOT EXISTS client_generated_id VARCHAR(255);

-- Add occurred_at (when activity actually happened, distinct from created_at)
ALTER TABLE activities
ADD COLUMN IF NOT EXISTS occurred_at TIMESTAMPTZ;

-- Add received_at (when server received the activity)
ALTER TABLE activities
ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ DEFAULT NOW();

-- Add flexible context storage (replacing individual fields)
-- NOTE: We'll keep existing fields for backward compatibility
ALTER TABLE activities
ADD COLUMN IF NOT EXISTS context JSONB DEFAULT '{}'::jsonb;

-- Add ingestion metadata
ALTER TABLE activities
ADD COLUMN IF NOT EXISTS ingestion_metadata JSONB DEFAULT '{}'::jsonb;

-- =====================================================
-- PART 2: BACKFILL DATA
-- =====================================================

-- Migrate existing data to new columns
UPDATE activities
SET occurred_at = created_at
WHERE occurred_at IS NULL;

-- Backfill context from existing fields
UPDATE activities
SET context = jsonb_build_object(
    'app', app,
    'title', title,
    'domain', domain,
    'url', url
)
WHERE context = '{}'::jsonb AND (app IS NOT NULL OR title IS NOT NULL OR domain IS NOT NULL OR url IS NOT NULL);

-- =====================================================
-- PART 3: CREATE INDEXES
-- =====================================================

-- Primary deduplication index: user + source + client_id
CREATE INDEX IF NOT EXISTS idx_activities_user_source_client
ON activities (user_id, source, client_generated_id)
WHERE client_generated_id IS NOT NULL;

-- Time-based query index
CREATE INDEX IF NOT EXISTS idx_activities_user_occurred
ON activities (user_id, occurred_at DESC);

-- Type + time composite index for filtered queries
CREATE INDEX IF NOT EXISTS idx_activities_user_type_occurred
ON activities (user_id, type, occurred_at DESC);

-- Context domain index for heuristic dedup
CREATE INDEX IF NOT EXISTS idx_activities_context_domain
ON activities ((context->>'domain'))
WHERE context->>'domain' IS NOT NULL;

-- JSONB GIN index for flexible context queries
CREATE INDEX IF NOT EXISTS idx_activities_context_gin
ON activities USING GIN (context);

-- =====================================================
-- PART 4: ADD CONSTRAINTS
-- =====================================================

-- Ensure occurred_at is set for all new activities
ALTER TABLE activities
ADD CONSTRAINT check_occurred_at_not_null
CHECK (occurred_at IS NOT NULL);

-- =====================================================
-- PART 5: COMMENTS
-- =====================================================

COMMENT ON COLUMN activities.source_version IS 'Version of the client that sent this activity (e.g., "ext-0.1.3", "desktop-1.2.0")';
COMMENT ON COLUMN activities.client_generated_id IS 'Client-generated unique ID for idempotent ingestion and deduplication';
COMMENT ON COLUMN activities.occurred_at IS 'Timestamp when the activity actually occurred (client time, normalized to UTC)';
COMMENT ON COLUMN activities.received_at IS 'Timestamp when the server received this activity';
COMMENT ON COLUMN activities.context IS 'Flexible JSONB storage for activity context (url, domain, title, app, file_path, etc.)';
COMMENT ON COLUMN activities.ingestion_metadata IS 'Metadata about the ingestion process (schema_version, ip_hash, user_agent, timezone)';

-- =====================================================
-- ROLLBACK SCRIPT (if needed)
-- =====================================================

/*
-- To rollback this migration:

DROP INDEX IF EXISTS idx_activities_context_gin;
DROP INDEX IF EXISTS idx_activities_context_domain;
DROP INDEX IF EXISTS idx_activities_user_type_occurred;
DROP INDEX IF EXISTS idx_activities_user_occurred;
DROP INDEX IF EXISTS idx_activities_user_source_client;

ALTER TABLE activities DROP CONSTRAINT IF EXISTS check_occurred_at_not_null;

ALTER TABLE activities DROP COLUMN IF EXISTS ingestion_metadata;
ALTER TABLE activities DROP COLUMN IF EXISTS context;
ALTER TABLE activities DROP COLUMN IF EXISTS received_at;
ALTER TABLE activities DROP COLUMN IF EXISTS occurred_at;
ALTER TABLE activities DROP COLUMN IF EXISTS client_generated_id;
ALTER TABLE activities DROP COLUMN IF EXISTS source_version;
*/
