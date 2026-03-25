-- Migration: 002_entity_extensions.sql
-- Week 7: NER Pipeline & Entity Storage
-- Extends entities table and creates entity_occurrences table

-- ============================================
-- PART 1: Extend entities table
-- ============================================

ALTER TABLE entities
ADD COLUMN IF NOT EXISTS embedding FLOAT[] DEFAULT NULL,
ADD COLUMN IF NOT EXISTS external_ids JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS sources TEXT[] DEFAULT '{}',
ADD COLUMN IF NOT EXISTS merged_into_id UUID REFERENCES entities(id);

-- Create indexes for new columns
CREATE INDEX IF NOT EXISTS idx_entities_external_ids_gin ON entities USING GIN (external_ids);
CREATE INDEX IF NOT EXISTS idx_entities_merged_into ON entities (merged_into_id);
CREATE INDEX IF NOT EXISTS idx_entities_embedding ON entities USING GIN (embedding);

-- Add comments
COMMENT ON COLUMN entities.embedding IS '384-dimensional sentence embedding for similarity search (vector length depends on model)';
COMMENT ON COLUMN entities.external_ids IS 'External identifiers: {"orcid": "0000-...", "doi": "10.1234/...", "github": "username"}';
COMMENT ON COLUMN entities.sources IS 'Sources where entity was discovered: ["github", "arxiv", "manual", "browser"]';
COMMENT ON COLUMN entities.merged_into_id IS 'If entity was merged, points to the canonical entity';

-- ============================================
-- PART 2: Create entity_occurrences table
-- ============================================

CREATE TABLE IF NOT EXISTS entity_occurrences (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Foreign keys
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    activity_id UUID NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Occurrence context
    source_type VARCHAR(50) NOT NULL,  -- 'title', 'url', 'snippet', 'content', 'window_title'
    start_offset INTEGER,
    end_offset INTEGER,
    confidence FLOAT DEFAULT 0.8,
    
    -- Extracted metadata
    extracted_text TEXT,  -- The actual text that was recognized
    ner_label VARCHAR(50),  -- Original spaCy NER label (PERSON, ORG, GPE, etc.)
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT valid_offsets CHECK (start_offset IS NULL OR end_offset >= start_offset),
    CONSTRAINT valid_confidence CHECK (confidence BETWEEN 0 AND 1),
    CONSTRAINT valid_source_type CHECK (source_type IN ('title', 'url', 'snippet', 'content', 'window_title', 'app_name'))
);

-- ============================================
-- PART 3: Create indexes for performance
-- ============================================

-- Primary query patterns: "Show me all occurrences of entity X"
CREATE INDEX idx_occurrences_entity_time ON entity_occurrences (entity_id, created_at DESC);

-- Query pattern: "What entities appear in activity Y"
CREATE INDEX idx_occurrences_activity ON entity_occurrences (activity_id);

-- Query pattern: "All entity occurrences for user Z"
CREATE INDEX idx_occurrences_user_time ON entity_occurrences (user_id, created_at DESC);

-- Query pattern: "Filter by source type"
CREATE INDEX idx_occurrences_source_type ON entity_occurrences (source_type);

-- Query pattern: "Entity-Activity relationships"
CREATE INDEX idx_occurrences_entity_activity ON entity_occurrences (entity_id, activity_id);

-- Query pattern: "Find high-confidence extractions"
CREATE INDEX idx_occurrences_confidence ON entity_occurrences (confidence DESC) WHERE confidence >= 0.8;

-- ============================================
-- PART 4: Add table comments
-- ============================================

COMMENT ON TABLE entity_occurrences IS 'Tracks every appearance of an entity in user activities for context and validation';
COMMENT ON COLUMN entity_occurrences.source_type IS 'Where in the activity the entity was found';
COMMENT ON COLUMN entity_occurrences.start_offset IS 'Character offset where entity starts in source text';
COMMENT ON COLUMN entity_occurrences.end_offset IS 'Character offset where entity ends in source text';
COMMENT ON COLUMN entity_occurrences.confidence IS 'NER confidence score (0-1), based on model output or heuristics';
COMMENT ON COLUMN entity_occurrences.extracted_text IS 'The exact text that was recognized as an entity';
COMMENT ON COLUMN entity_occurrences.ner_label IS 'Original NER label from spaCy (PERSON, ORG, GPE, PRODUCT, etc.)';

-- ============================================
-- PART 5: Create view for entity timeline
-- ============================================

CREATE OR REPLACE VIEW entity_timeline AS
SELECT 
    eo.id AS occurrence_id,
    e.id AS entity_id,
    e.canonical_name,
    e.type AS entity_type,
    a.id AS activity_id,
    a.type AS activity_type,
    a.occurred_at,
    eo.source_type,
    eo.extracted_text,
    eo.confidence,
    eo.created_at
FROM entity_occurrences eo
JOIN entities e ON eo.entity_id = e.id
JOIN activities a ON eo.activity_id = a.id
ORDER BY a.occurred_at DESC;

COMMENT ON VIEW entity_timeline IS 'Chronological view of all entity occurrences with activity context';

-- ============================================
-- ROLLBACK (if needed)
-- ============================================

-- To rollback this migration, run:
/*
DROP VIEW IF EXISTS entity_timeline;
DROP TABLE IF EXISTS entity_occurrences CASCADE;

ALTER TABLE entities
DROP COLUMN IF EXISTS embedding,
DROP COLUMN IF EXISTS external_ids,
DROP COLUMN IF EXISTS sources,
DROP COLUMN IF EXISTS merged_into_id;

DROP INDEX IF EXISTS idx_entities_external_ids_gin;
DROP INDEX IF EXISTS idx_entities_merged_into;
DROP INDEX IF EXISTS idx_entities_embedding;
*/

-- ============================================
-- Migration complete
-- ============================================

SELECT 'Migration 002_entity_extensions.sql completed successfully' AS status;
