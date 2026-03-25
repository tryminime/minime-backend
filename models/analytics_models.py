"""
Analytics Models for Month 6 Personal Analytics.

Defines SQLAlchemy ORM models for:
- DailyMetrics: 6 core productivity metrics per user per day
- DailySummary: LLM-generated daily summaries
- WeeklyReport: Comprehensive 9-section weekly reports
- AnalyticsEmail: Email delivery tracking
"""

from sqlalchemy import (
    Column, String, Integer, Numeric, Date, DateTime,
    ForeignKey, CheckConstraint, UniqueConstraint, Text, Boolean
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import date, datetime
from typing import Optional, Dict, Any
import uuid

from database.postgres import Base


class DailyMetrics(Base):
    """Store 6 core productivity metrics per user per day."""
    
    __tablename__ = "daily_metrics"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    
    # 6 Core Productivity Metrics
    focus_score = Column(Numeric(4, 1))  # 0-10
    deep_work_hours = Column(Numeric(5, 2))  # hours
    context_switches = Column(Integer)  # count
    meeting_load_pct = Column(Numeric(5, 2))  # 0-100
    distraction_index = Column(Numeric(5, 2))  # 0-100
    break_quality = Column(Numeric(4, 1))  # 0-10
    
    # Metadata
    raw_metrics = Column(JSONB)  # full breakdown
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('user_id', 'date', name='uq_daily_metrics_user_date'),
        CheckConstraint('focus_score >= 0 AND focus_score <= 10', name='ck_focus_score_range'),
        CheckConstraint('deep_work_hours >= 0', name='ck_deep_work_positive'),
        CheckConstraint('context_switches >= 0', name='ck_context_switches_positive'),
        CheckConstraint('meeting_load_pct >= 0 AND meeting_load_pct <= 100', name='ck_meeting_load_range'),
        CheckConstraint('distraction_index >= 0 AND distraction_index <= 100', name='ck_distraction_range'),
        CheckConstraint('break_quality >= 0 AND break_quality <= 10', name='ck_break_quality_range'),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "date": self.date.isoformat() if self.date else None,
            "focus_score": float(self.focus_score) if self.focus_score else None,
            "deep_work_hours": float(self.deep_work_hours) if self.deep_work_hours else None,
            "context_switches": self.context_switches,
            "meeting_load_pct": float(self.meeting_load_pct) if self.meeting_load_pct else None,
            "distraction_index": float(self.distraction_index) if self.distraction_index else None,
            "break_quality": float(self.break_quality) if self.break_quality else None,
            "raw_metrics": self.raw_metrics,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class DailySummary(Base):
    """Store LLM-generated daily summaries."""
    
    __tablename__ = "daily_summaries"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    
    # Summary content
    summary_markdown = Column(Text, nullable=False)
    summary_html = Column(Text, nullable=False)
    
    # Denormalized key metrics
    focus_score = Column(Numeric(4, 1))
    deep_work_hours = Column(Numeric(5, 2))
    
    # Metadata
    summary_metadata = Column(JSONB)  # accomplishments, recommendations
    generated_at = Column(DateTime(timezone=True))
    llm_model = Column(String(50))  # e.g., 'claude-3-sonnet-20240229'
    generation_duration_ms = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('user_id', 'date', name='uq_daily_summaries_user_date'),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "date": self.date.isoformat() if self.date else None,
            "summary_markdown": self.summary_markdown,
            "summary_html": self.summary_html,
            "focus_score": float(self.focus_score) if self.focus_score else None,
            "deep_work_hours": float(self.deep_work_hours) if self.deep_work_hours else None,
            "summary_metadata": self.summary_metadata,
            "generated_at": self.generated_at.isoformat() if self.generated_at else None,
            "llm_model": self.llm_model,
            "generation_duration_ms": self.generation_duration_ms,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class WeeklyReport(Base):
    """Store comprehensive weekly analytics reports."""
    
    __tablename__ = "weekly_reports"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    week_start_date = Column(Date, nullable=False)  # Monday
    week_end_date = Column(Date, nullable=False)    # Sunday
    
    # 9 Report Sections (JSONB for flexibility)
    overview = Column(JSONB)                    # LLM-generated overview
    time_analytics = Column(JSONB)              # hours breakdown
    productivity_metrics = Column(JSONB)        # aggregated metrics
    projects_section = Column(JSONB)            # top projects
    papers_section = Column(JSONB)              # research progress
    collaboration_section = Column(JSONB)       # collaborators
    skills_section = Column(JSONB)              # skills worked on
    trends_section = Column(JSONB)              # week-over-week
    recommendations_section = Column(JSONB)     # LLM recommendations
    
    # Rendered output
    report_markdown = Column(Text)
    report_html = Column(Text)
    
    # Generation metadata
    generated_at = Column(DateTime(timezone=True))
    llm_model = Column(String(50))
    generation_duration_ms = Column(Integer)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('user_id', 'week_start_date', name='uq_weekly_reports_user_week'),
        CheckConstraint('week_end_date >= week_start_date', name='ck_week_dates_order'),
        CheckConstraint('week_end_date - week_start_date = 6', name='ck_week_exactly_7_days'),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "week_start_date": self.week_start_date.isoformat() if self.week_start_date else None,
            "week_end_date": self.week_end_date.isoformat() if self.week_end_date else None,
            "overview": self.overview,
            "time_analytics": self.time_analytics,
            "productivity_metrics": self.productivity_metrics,
            "projects_section": self.projects_section,
            "papers_section": self.papers_section,
            "collaboration_section": self.collaboration_section,
            "skills_section": self.skills_section,
            "trends_section": self.trends_section,
            "recommendations_section": self.recommendations_section,
            "report_markdown": self.report_markdown,
            "report_html": self.report_html,
            "generated_at": self.generated_at.isoformat() if self.generated_at else None,
            "llm_model": self.llm_model,
            "generation_duration_ms": self.generation_duration_ms,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class AnalyticsEmail(Base):
    """Track email delivery for analytics (daily & weekly)."""
    
    __tablename__ = "analytics_emails"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    type = Column(String(16), nullable=False)  # 'daily' or 'weekly'
    
    # Reference to what was sent
    reference_date = Column(Date)     # for daily summaries
    week_start_date = Column(Date)    # for weekly reports
    
    # Delivery tracking
    sent_at = Column(DateTime(timezone=True))
    status = Column(String(16), nullable=False, default='pending')  # 'pending', 'sent', 'failed', 'bounced'
    provider_message_id = Column(Text)  # from SendGrid/SES
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    
    # Engagement tracking (optional)
    opened_at = Column(DateTime(timezone=True))
    clicked_at = Column(DateTime(timezone=True))
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Constraints
    __table_args__ = (
        CheckConstraint("type IN ('daily', 'weekly')", name='ck_email_type'),
        CheckConstraint("status IN ('pending', 'sent', 'failed', 'bounced')", name='ck_email_status'),
        CheckConstraint(
            "(type = 'daily' AND reference_date IS NOT NULL AND week_start_date IS NULL) OR "
            "(type = 'weekly' AND week_start_date IS NOT NULL AND reference_date IS NULL)",
            name='ck_email_reference_consistency'
        ),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "type": self.type,
            "reference_date": self.reference_date.isoformat() if self.reference_date else None,
            "week_start_date": self.week_start_date.isoformat() if self.week_start_date else None,
            "sent_at": self.sent_at.isoformat() if self.sent_at else None,
            "status": self.status,
            "provider_message_id": self.provider_message_id,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "clicked_at": self.clicked_at.isoformat() if self.clicked_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class UserGoal(Base):
    """User goals — persisted to PostgreSQL so they survive server restarts."""

    __tablename__ = "user_goals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    title = Column(String(255), nullable=False)
    category = Column(String(50), nullable=False, default="custom")
    target_value = Column(Numeric(10, 2), nullable=False, default=1.0)
    current_value = Column(Numeric(10, 2), nullable=False, default=0.0)
    unit = Column(String(50), nullable=False, default="sessions")
    deadline = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(50), nullable=False, default="active")
    streak_count = Column(Integer, nullable=False, default=0)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("id", name="uq_user_goals_id"),
        {"extend_existing": True},
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "title": self.title,
            "category": self.category,
            "target_value": float(self.target_value),
            "current_value": float(self.current_value),
            "unit": self.unit,
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "status": self.status,
            "streak_count": self.streak_count,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
