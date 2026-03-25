-- Neo4j Graph Schema for MiniMe Knowledge Graph
-- Month 5: Week 1 - Schema Design
-- Version: 1.0
-- Created: 2026-02-04

-- =====================================================
-- SECTION 1: UNIQUE CONSTRAINTS (Prevent Duplicates)
-- =====================================================

-- PERSON nodes
CREATE CONSTRAINT person_id IF NOT EXISTS
FOR (p:PERSON) REQUIRE p.id IS UNIQUE;

-- PAPER nodes
CREATE CONSTRAINT paper_id IF NOT EXISTS
FOR (p:PAPER) REQUIRE p.id IS UNIQUE;

-- TOPIC nodes
CREATE CONSTRAINT topic_id IF NOT EXISTS
FOR (t:TOPIC) REQUIRE t.id IS UNIQUE;

-- PROJECT nodes
CREATE CONSTRAINT project_id IF NOT EXISTS
FOR (p:PROJECT) REQUIRE p.id IS UNIQUE;

-- DATASET nodes
CREATE CONSTRAINT dataset_id IF NOT EXISTS
FOR (d:DATASET) REQUIRE d.id IS UNIQUE;

-- INSTITUTION nodes
CREATE CONSTRAINT institution_id IF NOT EXISTS
FOR (i:INSTITUTION) REQUIRE i.id IS UNIQUE;

-- TOOL nodes
CREATE CONSTRAINT tool_id IF NOT EXISTS
FOR (t:TOOL) REQUIRE t.id IS UNIQUE;

-- VENUE nodes
CREATE CONSTRAINT venue_id IF NOT EXISTS
FOR (v:VENUE) REQUIRE v.id IS UNIQUE;

-- =====================================================
-- SECTION 2: PERFORMANCE INDEXES (Multi-tenant & Queries)
-- =====================================================

-- PERSON indexes
CREATE INDEX person_user_id IF NOT EXISTS
FOR (p:PERSON) ON (p.user_id);

CREATE INDEX person_canonical_name IF NOT EXISTS
FOR (p:PERSON) ON (p.canonical_name);

CREATE INDEX person_email IF NOT EXISTS
FOR (p:PERSON) ON (p.email);

CREATE INDEX person_affiliation IF NOT EXISTS
FOR (p:PERSON) ON (p.affiliation);

-- PAPER indexes
CREATE INDEX paper_user_id IF NOT EXISTS
FOR (p:PAPER) ON (p.user_id);

CREATE INDEX paper_doi IF NOT EXISTS
FOR (p:PAPER) ON (p.doi);

CREATE INDEX paper_arxiv_id IF NOT EXISTS
FOR (p:PAPER) ON (p.arxiv_id);

CREATE INDEX paper_year IF NOT EXISTS
FOR (p:PAPER) ON (p.year);

CREATE INDEX paper_venue IF NOT EXISTS
FOR (p:PAPER) ON (p.venue);

-- TOPIC indexes
CREATE INDEX topic_user_id IF NOT EXISTS
FOR (t:TOPIC) ON (t.user_id);

CREATE INDEX topic_canonical_name IF NOT EXISTS
FOR (t:TOPIC) ON (t.canonical_name);

CREATE INDEX topic_parent IF NOT EXISTS
FOR (t:TOPIC) ON (t.parent_topic_id);

-- PROJECT indexes
CREATE INDEX project_user_id IF NOT EXISTS
FOR (p:PROJECT) ON (p.user_id);

CREATE INDEX project_status IF NOT EXISTS
FOR (p:PROJECT) ON (p.status);

CREATE INDEX project_name IF NOT EXISTS
FOR (p:PROJECT) ON (p.name);

-- DATASET indexes
CREATE INDEX dataset_user_id IF NOT EXISTS
FOR (d:DATASET) ON (d.user_id);

CREATE INDEX dataset_name IF NOT EXISTS
FOR (d:DATASET) ON (d.name);

-- INSTITUTION indexes
CREATE INDEX institution_user_id IF NOT EXISTS
FOR (i:INSTITUTION) ON (i.user_id);

CREATE INDEX institution_name IF NOT EXISTS
FOR (i:INSTITUTION) ON (i.name);

CREATE INDEX institution_country IF NOT EXISTS
FOR (i:INSTITUTION) ON (i.country);

-- TOOL indexes
CREATE INDEX tool_user_id IF NOT EXISTS
FOR (t:TOOL) ON (t.user_id);

CREATE INDEX tool_name IF NOT EXISTS
FOR (t:TOOL) ON (t.name);

CREATE INDEX tool_type IF NOT EXISTS
FOR (t:TOOL) ON (t.type);

-- VENUE indexes
CREATE INDEX venue_user_id IF NOT EXISTS
FOR (v:VENUE) ON (v.user_id);

CREATE INDEX venue_name IF NOT EXISTS
FOR (v:VENUE) ON (v.name);

CREATE INDEX venue_year IF NOT EXISTS
FOR (v:VENUE) ON (v.year);

CREATE INDEX venue_type IF NOT EXISTS
FOR (v:VENUE) ON (v.type);

-- =====================================================
-- SECTION 3: FULL-TEXT SEARCH INDEXES (Neo4j 5.x)
-- =====================================================

-- PERSON full-text search
CREATE FULLTEXT INDEX person_search IF NOT EXISTS
FOR (p:PERSON)
ON EACH [p.canonical_name, p.research_interests];

-- PAPER full-text search
CREATE FULLTEXT INDEX paper_search IF NOT EXISTS
FOR (p:PAPER)
ON EACH [p.title, p.abstract, p.keywords];

-- TOPIC full-text search
CREATE FULLTEXT INDEX topic_search IF NOT EXISTS
FOR (t:TOPIC)
ON EACH [t.canonical_name, t.description, t.aliases];

-- PROJECT full-text search
CREATE FULLTEXT INDEX project_search IF NOT EXISTS
FOR (p:PROJECT)
ON EACH [p.name, p.description];

-- DATASET full-text search
CREATE FULLTEXT INDEX dataset_search IF NOT EXISTS
FOR (d:DATASET)
ON EACH [d.name, d.description];

-- INSTITUTION full-text search
CREATE FULLTEXT INDEX institution_search IF NOT EXISTS
FOR (i:INSTITUTION)
ON EACH [i.name, i.city];

-- TOOL full-text search
CREATE FULLTEXT INDEX tool_search IF NOT EXISTS
FOR (t:TOOL)
ON EACH [t.name];

-- VENUE full-text search
CREATE FULLTEXT INDEX venue_search IF NOT EXISTS
FOR (v:VENUE)
ON EACH [v.name, v.acronym, v.location];

-- =====================================================
-- SECTION 4: COMPOSITE INDEXES (Advanced Queries)
-- =====================================================

-- User + Type combination (for filtering)
CREATE INDEX person_user_type IF NOT EXISTS
FOR (p:PERSON) ON (p.user_id, p.affiliation);

CREATE INDEX paper_user_year IF NOT EXISTS
FOR (p:PAPER) ON (p.user_id, p.year);

-- =====================================================
-- SECTION 5: RELATIONSHIP PROPERTY INDEXES (Future)
-- =====================================================

-- Note: Relationship property indexes available in Neo4j 5.x+
-- These would be added later for relationship-based queries

-- Example (commented for now):
-- CREATE INDEX authored_position FOR ()-[r:AUTHORED]->() ON (r.position);
-- CREATE INDEX cites_weight FOR ()-[r:CITES]->() ON (r.weight);

-- =====================================================
-- SECTION 6: VERIFICATION QUERIES
-- =====================================================

-- Show all constraints
-- SHOW CONSTRAINTS;

-- Show all indexes
-- SHOW INDEXES;

-- Check constraint status
-- SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties, state;

-- =====================================================
-- END OF SCHEMA
-- =====================================================
