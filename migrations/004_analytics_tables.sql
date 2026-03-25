-- Migration: Analytics Tables for Month 6 Personal Analytics
-- Created: 2026-02-09
-- Description: Creates tables for daily metrics, summaries, weekly reports, and email tracking

-- =====================================================
-- TABLE: daily_metrics
-- Purpose: Store 6 core productivity metrics per user per day
-- =====================================================
CREATE TABLE IF NOT EXISTS daily_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    
    -- 6 Core Productivity Metrics
    focus_score NUMERIC(4,1) CHECK (focus_score >= 0 AND focus_score <= 10),
    deep_work_hours NUMERIC(5,2) CHECK (deep_work_hours >= 0),
    context_switches INTEGER CHECK (context_switches >= 0),
    meeting_load_pct NUMERIC(5,2) CHECK (meeting_load_pct >= 0 AND meeting_load_pct <= 100),
    distraction_index NUMERIC(5,2) CHECK (distraction_index >= 0 AND distraction_index <= 100),
    break_quality NUMERIC(4,1) CHECK (break_quality >= 0 AND break_quality <= 10),
    
    -- Full breakdown and metadata
    raw_metrics JSONB,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraint: one row per user per day
    UNIQUE(user_id, date)
);

-- Indexes for daily_metrics
CREATE INDEX idx_daily_metrics_user_date ON daily_metrics(user_id, date DESC);
CREATE INDEX idx_daily_metrics_date ON daily_metrics(date DESC);

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_daily_metrics_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_daily_metrics_updated_at
    BEFORE UPDATE ON daily_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_daily_metrics_timestamp();

-- =====================================================
-- TABLE: daily_summaries
-- Purpose: Store LLM-generated daily summaries
-- =====================================================
CREATE TABLE IF NOT EXISTS daily_summaries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    
    -- Summary content in multiple formats
    summary_markdown TEXT NOT NULL,
    summary_html TEXT NOT NULL,
    
    -- Denormalized key metrics for quick access
    focus_score NUMERIC(4,1),
    deep_work_hours NUMERIC(5,2),
    
    -- Additional metadata
    metadata JSONB, -- accomplishments, recommendations, etc.
    generated_at TIMESTAMP WITH TIME ZONE,
    llm_model VARCHAR(50), -- e.g., 'claude-3-sonnet-20240229'
    generation_duration_ms INTEGER, -- how long it took to generate
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraint: one summary per user per day
    UNIQUE(user_id, date)
);

-- Indexes for daily_summaries
CREATE INDEX idx_daily_summaries_user_date ON daily_summaries(user_id, date DESC);
CREATE INDEX idx_daily_summaries_date ON daily_summaries(date DESC);
CREATE INDEX idx_daily_summaries_generated_at ON daily_summaries(generated_at DESC);

-- Trigger for updated_at
CREATE TRIGGER trigger_daily_summaries_updated_at
    BEFORE UPDATE ON daily_summaries
    FOR EACH ROW
    EXECUTE FUNCTION update_daily_metrics_timestamp();

-- =====================================================
-- TABLE: weekly_reports
-- Purpose: Store comprehensive weekly analytics reports
-- =====================================================
CREATE TABLE IF NOT EXISTS weekly_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    week_start_date DATE NOT NULL, -- Monday
    week_end_date DATE NOT NULL,   -- Sunday
    
    -- 9 Report Sections (stored as JSONB for flexibility)
    overview JSONB,                     -- LLM-generated overview
    time_analytics JSONB,               -- hours breakdown
    productivity_metrics JSONB,         -- aggregated metrics
    projects_section JSONB,             -- top projects
    papers_section JSONB,               -- research progress
    collaboration_section JSONB,        -- collaborators & network
    skills_section JSONB,               -- skills worked on
    trends_section JSONB,               -- week-over-week changes
    recommendations_section JSONB,      -- LLM recommendations
    
    -- Rendered output
    report_markdown TEXT,
    report_html TEXT,
    
    -- Generation metadata
    generated_at TIMESTAMP WITH TIME ZONE,
    llm_model VARCHAR(50),
    generation_duration_ms INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(user_id, week_start_date),
    CHECK (week_end_date >= week_start_date),
    CHECK (week_end_date - week_start_date = 6) -- exactly 7 days (Monday-Sunday)
);

-- Indexes for weekly_reports
CREATE INDEX idx_weekly_reports_user_week ON weekly_reports(user_id, week_start_date DESC);
CREATE INDEX idx_weekly_reports_week_start ON weekly_reports(week_start_date DESC);
CREATE INDEX idx_weekly_reports_generated_at ON weekly_reports(generated_at DESC);

-- Trigger for updated_at
CREATE TRIGGER trigger_weekly_reports_updated_at
    BEFORE UPDATE ON weekly_reports
    FOR EACH ROW
    EXECUTE FUNCTION update_daily_metrics_timestamp();

-- =====================================================
-- TABLE: analytics_emails
-- Purpose: Track email delivery for analytics (daily & weekly)
-- =====================================================
CREATE TABLE IF NOT EXISTS analytics_emails (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type VARCHAR(16) NOT NULL CHECK (type IN ('daily', 'weekly')),
    
    -- Reference to what was sent
    reference_date DATE,        -- for daily summaries
    week_start_date DATE,       -- for weekly reports
    
    -- Delivery tracking
    sent_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed', 'bounced')),
    provider_message_id TEXT,   -- from SendGrid/SES
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Engagement tracking (optional)
    opened_at TIMESTAMP WITH TIME ZONE,
    clicked_at TIMESTAMP WITH TIME ZONE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints based on type
    CHECK (
        (type = 'daily' AND reference_date IS NOT NULL AND week_start_date IS NULL) OR
        (type = 'weekly' AND week_start_date IS NOT NULL AND reference_date IS NULL)
    )
);

-- Indexes for analytics_emails
CREATE INDEX idx_analytics_emails_user_type ON analytics_emails(user_id, type, created_at DESC);
CREATE INDEX idx_analytics_emails_status ON analytics_emails(status, created_at DESC);
CREATE INDEX idx_analytics_emails_daily_ref ON analytics_emails(reference_date) WHERE type = 'daily';
CREATE INDEX idx_analytics_emails_weekly_ref ON analytics_emails(week_start_date) WHERE type = 'weekly';

-- =====================================================
-- COMMENTS
-- =====================================================
COMMENT ON TABLE daily_metrics IS 'Stores 6 core productivity metrics computed daily per user';
COMMENT ON TABLE daily_summaries IS 'Stores LLM-generated daily summaries with markdown/HTML output';
COMMENT ON TABLE weekly_reports IS 'Stores comprehensive 9-section weekly analytics reports';
COMMENT ON TABLE analytics_emails IS 'Tracks delivery status of daily and weekly analytics emails';

COMMENT ON COLUMN daily_metrics.focus_score IS 'Composite score 0-10 from deep work, context switches, meetings, breaks';
COMMENT ON COLUMN daily_metrics.deep_work_hours IS 'Hours of continuous focused sessions ≥30min on productive apps';
COMMENT ON COLUMN daily_metrics.context_switches IS 'Number of distinct app/window switches per day';
COMMENT ON COLUMN daily_metrics.meeting_load_pct IS 'Percentage of time spent in meetings vs total tracked time';
COMMENT ON COLUMN daily_metrics.distraction_index IS 'Percentage of focus time on non-productive apps (0-100, higher = worse)';
COMMENT ON COLUMN daily_metrics.break_quality IS 'Score 0-10 based on break distribution and frequency';

COMMENT ON COLUMN weekly_reports.week_start_date IS 'Monday of the week (ISO 8601)';
COMMENT ON COLUMN weekly_reports.week_end_date IS 'Sunday of the week (ISO 8601)';

-- =====================================================
-- GRANT PERMISSIONS (adjust as needed for your setup)
-- =====================================================
-- GRANT SELECT, INSERT, UPDATE, DELETE ON daily_metrics TO minime_backend;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON daily_summaries TO minime_backend;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON weekly_reports TO minime_backend;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON analytics_emails TO minime_backend;
