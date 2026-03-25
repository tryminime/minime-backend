"""
Pydantic Schemas for Analytics API.

Defines request/response models for all analytics endpoints.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any, List, Generic, TypeVar
from datetime import date, datetime
from decimal import Decimal


# ============================================================================
# PAGINATION & FILTERING SCHEMAS
# ============================================================================

class PaginationParams(BaseModel):
    """Pagination parameters for list endpoints."""
    
    offset: int = Field(0, ge=0, description="Number of items to skip")
    limit: int = Field(20, ge=1, le=100, description="Maximum items to return")
    
    class Config:
        json_schema_extra = {
            "example": {
                "offset": 0,
                "limit": 20
            }
        }


class PaginatedResponse(BaseModel):
    """Paginated response wrapper."""
    
    total: int = Field(description="Total number of items")
    offset: int = Field(description="Current offset")
    limit: int = Field(description="Items per page")
    items: List[Dict[str, Any]] = Field(description="List of items")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total": 100,
                "offset": 0,
                "limit": 20,
                "items": []
            }
        }


class DateRangeFilter(BaseModel):
    """Date range filter for queries."""
    
    start_date: date = Field(description="Start date (inclusive)")
    end_date: date = Field(description="End date (inclusive)")
    
    @field_validator('end_date')
    @classmethod
    def validate_range(cls, v, info):
        """Ensure end_date >= start_date."""
        if 'start_date' in info.data and v < info.data['start_date']:
            raise ValueError('end_date must be >= start_date')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "start_date": "2026-02-01",
                "end_date": "2026-02-09"
            }
        }


# ============================================================================
# DAILY METRICS SCHEMAS
# ============================================================================

class DailyMetricsResponse(BaseModel):
    """Response model for daily metrics."""
    
    id: str
    user_id: str
    date: date
    
    focus_score: Optional[float] = Field(None, ge=0, le=10, description="Focus score 0-10")
    deep_work_hours: Optional[float] = Field(None, ge=0, description="Hours of deep work")
    context_switches: Optional[int] = Field(None, ge=0, description="Number of context switches")
    meeting_load_pct: Optional[float] = Field(None, ge=0, le=100, description="Meeting load percentage")
    distraction_index: Optional[float] = Field(None, ge=0, le=100, description="Distraction index 0-100")
    break_quality: Optional[float] = Field(None, ge=0, le=10, description="Break quality score 0-10")
    
    raw_metrics: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "user_id": "123e4567-e89b-12d3-a456-426614174001",
                "date": "2026-02-09",
                "focus_score": 7.8,
                "deep_work_hours": 4.2,
                "context_switches": 25,
                "meeting_load_pct": 35.0,
                "distraction_index": 15.5,
                "break_quality": 8.0,
                "raw_metrics": {"total_activities": 150},
                "created_at": "2026-02-09T10:00:00Z",
                "updated_at": "2026-02-09T10:00:00Z"
            }
        }


class WeeklyMetricsResponse(BaseModel):
    """Response model for weekly aggregated metrics."""
    
    week_start: date
    week_end: date
    days_tracked: int
    
    averages: Dict[str, float] = Field(description="Average metrics across the week")
    totals: Dict[str, float] = Field(description="Total metrics across the week")
    best_day: Dict[str, Optional[date]] = Field(description="Best days by metric")
    worst_day: Dict[str, Optional[date]] = Field(description="Worst days by metric")
    
    daily_breakdown: List[DailyMetricsResponse]
    
    class Config:
        json_schema_extra = {
            "example": {
                "week_start": "2026-02-03",
                "week_end": "2026-02-09",
                "days_tracked": 7,
                "averages": {
                    "focus_score": 7.5,
                    "deep_work_hours": 4.0,
                    "meeting_load_pct": 30.0
                },
                "totals": {
                    "deep_work_hours": 28.0,
                    "context_switches": 175
                },
                "best_day": {
                    "focus_score": "2026-02-05",
                    "deep_work": "2026-02-06"
                },
                "worst_day": {
                    "focus_score": "2026-02-07"
                },
                "daily_breakdown": []
            }
        }


# ============================================================================
# DAILY SUMMARY SCHEMAS
# ============================================================================

class DailySummaryResponse(BaseModel):
    """Response model for daily summary."""
    
    id: str
    user_id: str
    date: date
    
    summary_markdown: str
    summary_html: str
    
    focus_score: Optional[float]
    deep_work_hours: Optional[float]
    
    summary_metadata: Optional[Dict[str, Any]] = Field(None, description="Accomplishments, recommendations")
    generated_at: Optional[datetime]
    llm_model: Optional[str]
    generation_duration_ms: Optional[int]
    
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174002",
                "user_id": "123e4567-e89b-12d3-a456-426614174001",
                "date": "2026-02-09",
                "summary_markdown": "Today you achieved a focus score of 7.8/10...",
                "summary_html": "<p>Today you achieved a focus score of 7.8/10...</p>",
                "focus_score": 7.8,
                "deep_work_hours": 4.2,
                "metadata": {
                    "accomplishments": ["Completed feature X", "Fixed bug Y"],
                    "recommendations": ["Take more breaks"]
                },
                "generated_at": "2026-02-09T18:00:00Z",
                "llm_model": "claude-3-sonnet-20240229",
                "generation_duration_ms": 2500,
                "created_at": "2026-02-09T18:00:00Z",
                "updated_at": "2026-02-09T18:00:00Z"
            }
        }


class GenerateSummaryRequest(BaseModel):
    """Request to generate a daily summary."""
    
    date: date = Field(description="Date to generate summary for")
    force_regenerate: bool = Field(default=False, description="Force regeneration even if exists")
    
    class Config:
        json_schema_extra = {
            "example": {
                "date": "2026-02-09",
                "force_regenerate": False
            }
        }


# ============================================================================
# WEEKLY REPORT SCHEMAS
# ============================================================================

class WeeklyReportResponse(BaseModel):
    """Response model for weekly report."""
    
    id: str
    user_id: str
    week_start_date: date
    week_end_date: date
    
    # 9 sections
    overview: Optional[Dict[str, Any]]
    time_analytics: Optional[Dict[str, Any]]
    productivity_metrics: Optional[Dict[str, Any]]
    projects_section: Optional[Dict[str, Any]]
    papers_section: Optional[Dict[str, Any]]
    collaboration_section: Optional[Dict[str, Any]]
    skills_section: Optional[Dict[str, Any]]
    trends_section: Optional[Dict[str, Any]]
    recommendations_section: Optional[Dict[str, Any]]
    
    report_markdown: Optional[str]
    report_html: Optional[str]
    
    generated_at: Optional[datetime]
    llm_model: Optional[str]
    generation_duration_ms: Optional[int]
    
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================================
# COLLABORATION SCHEMAS
# ============================================================================

class Collaborator(BaseModel):
    """Individual collaborator information."""
    
    id: str
    name: str
    affiliation: Optional[str]
    shared_projects: int
    shared_papers: int
    edge_weight: float
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "collab-123",
                "name": "Dr. Jane Smith",
                "affiliation": "MIT",
                "shared_projects": 3,
                "shared_papers": 5,
                "edge_weight": 0.85
            }
        }


class CollaborationSummaryResponse(BaseModel):
    """Response for collaboration summary."""
    
    collaboration_score: float = Field(ge=0, le=100, description="Overall collaboration score 0-100")
    total_collaborators: int
    active_collaborators_this_month: int
    network_diversity_score: float = Field(ge=0, le=100)
    
    frequency_score: float
    strength_score: float
    diversity_breakdown: Dict[str, Any]
    
    class Config:
        json_schema_extra = {
            "example": {
                "collaboration_score": 75.5,
                "total_collaborators": 12,
                "active_collaborators_this_month": 8,
                "network_diversity_score": 82.0,
                "frequency_score": 70.0,
                "strength_score": 85.0,
                "diversity_breakdown": {
                    "institutions": 5,
                    "topics": 8
                }
            }
        }


class CollaboratorsResponse(BaseModel):
    """Response for top collaborators list."""
    
    collaborators: List[Collaborator]
    total_count: int
    
    class Config:
        json_schema_extra = {
            "example": {
                "collaborators": [],
                "total_count": 12
            }
        }


# ============================================================================
# SKILLS SCHEMAS
# ============================================================================

class Skill(BaseModel):
    """Individual skill information."""
    
    topic_id: str
    topic_name: str
    mastery_level: float = Field(ge=0, le=100, description="Mastery score 0-100")
    papers_written: int
    hours_spent: float
    growth_trajectory: str = Field(description="e.g., 'increasing', 'stable', 'decreasing'")
    recency_score: float
    
    class Config:
        json_schema_extra = {
            "example": {
                "topic_id": "topic-456",
                "topic_name": "Machine Learning",
                "mastery_level": 75.0,
                "papers_written": 3,
                "hours_spent": 120.5,
                "growth_trajectory": "increasing",
                "recency_score": 0.9
            }
        }


class SkillsSummaryResponse(BaseModel):
    """Response for skills summary."""
    
    skills: List[Skill]
    total_skills: int
    top_skill: Optional[Skill]
    fastest_growing: Optional[Skill]
    
    class Config:
        json_schema_extra = {
            "example": {
                "skills": [],
                "total_skills": 8,
                "top_skill": None,
                "fastest_growing": None
            }
        }


class SkillDetailResponse(BaseModel):
    """Detailed response for a single skill."""
    
    skill: Skill
    related_projects: List[str]
    related_papers: List[str]
    collaborators_with_skill: List[str]
    weekly_hours_trend: List[float]
    
    class Config:
        json_schema_extra = {
            "example": {
                "skill": {},
                "related_projects": ["Project A", "Project B"],
                "related_papers": ["Paper 1", "Paper 2"],
                "collaborators_with_skill": ["Dr. Smith", "Prof. Johnson"],
                "weekly_hours_trend": [5.0, 6.5, 7.2, 8.0]
            }
        }


class SkillRecommendation(BaseModel):
    """Skill recommendation."""
    
    topic_id: str
    topic_name: str
    reason: str
    relevance_score: float = Field(ge=0, le=1)
    source: str = Field(description="e.g., 'collaborators', 'trending', 'complementary'")
    
    class Config:
        json_schema_extra = {
            "example": {
                "topic_id": "topic-789",
                "topic_name": "Deep Learning",
                "reason": "5 of your collaborators have expertise in this area",
                "relevance_score": 0.85,
                "source": "collaborators"
            }
        }


class SkillRecommendationsResponse(BaseModel):
    """Response for skill recommendations."""
    
    recommendations: List[SkillRecommendation]
    
    class Config:
        json_schema_extra = {
            "example": {
                "recommendations": []
            }
        }


# ============================================================================
# EMAIL SCHEMAS
# ============================================================================

class SendEmailRequest(BaseModel):
    """Request to send analytics email."""
    
    type: str = Field(pattern="^(daily|weekly)$", description="Email type")
    reference_date: Optional[date] = Field(None, description="For daily emails")
    week_start_date: Optional[date] = Field(None, description="For weekly emails")
    recipient_email: Optional[str] = Field(None, description="Override recipient (default: user's email)")
    
    @field_validator('reference_date', 'week_start_date')
    @classmethod
    def validate_dates(cls, v, info):
        """Ensure correct date is provided based on type."""
        if info.data.get('type') == 'daily' and 'reference_date' in info.field_name and v is None:
            raise ValueError("reference_date required for daily emails")
        if info.data.get('type') == 'weekly' and 'week_start_date' in info.field_name and v is None:
            raise ValueError("week_start_date required for weekly emails")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "type": "daily",
                "reference_date": "2026-02-09",
                "recipient_email": None
            }
        }


class EmailResponse(BaseModel):
    """Response for email send request."""
    
    email_id: str
    status: str = Field(pattern="^(pending|sent|failed)$")
    message: str
    provider_message_id: Optional[str] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "email_id": "email-123",
                "status": "sent",
                "message": "Daily summary email sent successfully",
                "provider_message_id": "sendgrid-msg-456"
            }
        }


# ============================================================================
# ERROR SCHEMAS
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response."""
    
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "NotFound",
                "message": "Daily metrics not found for the specified date",
                "details": {"date": "2026-02-09"}
            }
        }
