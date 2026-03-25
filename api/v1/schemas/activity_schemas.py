"""
Pydantic schemas for Activity Ingestion API.
Validates batch activity submissions from clients.
"""

from pydantic import BaseModel, Field, validator
from typing import Dict, Any, List, Optional, Literal
from datetime import datetime
from uuid import UUID
from enum import Enum


class ActivityType(str, Enum):
    """Canonical activity types."""
    PAGE_VIEW = "page_view"
    APP_FOCUS = "app_focus"
    FILE_EDIT = "file_edit"
    COMMIT = "commit"
    MEETING = "meeting"
    CUSTOM = "custom"
    WINDOW_FOCUS = "window_focus"
    WEB_VISIT = "web_visit"
    SOCIAL_MEDIA = "social_media"
    VIDEO_WATCH = "video_watch"
    SEARCH_QUERY = "search_query"
    READING_ANALYTICS = "reading_analytics"


class ActivitySource(str, Enum):
    """Activity source types."""
    BROWSER = "browser"
    DESKTOP = "desktop"
    MOBILE = "mobile"
    INTEGRATION = "integration"


class ActivityIngestItem(BaseModel):
    """Single activity item for batch ingestion."""
    
    client_generated_id: Optional[str] = Field(
        None,
        description="Client-generated unique ID for idempotency (strongly recommended)",
        max_length=255,
        example="browser:12345:1706906400:a1b2c3"
    )
    
    occurred_at: datetime = Field(
        ...,
        description="When the activity occurred (ISO 8601, UTC)",
        example="2026-02-03T18:22:15Z"
    )
    
    type: ActivityType = Field(
        ...,
        description="Activity type",
        example="page_view"
    )
    
    context: Dict[str, Any] = Field(
        default_factory=dict,
        description="Flexible context data (url, domain, title, app_name, etc.)",
        example={
            "url": "https://github.com/tryminime/backend",
            "domain": "github.com",
            "title": "MiniMe Backend Repository"
        }
    )
    
    duration_seconds: Optional[int] = Field(
        None,
        ge=0,
        le=86400,  # Max 24 hours
        description="Duration in seconds (null if unknown)"
    )
    
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional arbitrary metadata",
        example={"tab_id": 123, "window_id": 1}
    )
    
    class Config:
        use_enum_values = True
    
    @validator('context')
    def validate_context(cls, v):
        """Ensure context doesn't exceed reasonable size."""
        import json
        if len(json.dumps(v)) > 10000:  # 10KB limit
            raise ValueError("context field too large (max 10KB)")
        return v
    
    @validator('metadata')
    def validate_metadata(cls, v):
        """Ensure metadata doesn't exceed reasonable size."""
        import json
        if len(json.dumps(v)) > 5000:  # 5KB limit
            raise ValueError("metadata field too large (max 5KB)")
        return v


class ActivityBatchRequest(BaseModel):
    """Request schema for batch activity ingestion."""
    
    source: ActivitySource = Field(
        ...,
        description="Activity source identifier",
        example="browser"
    )
    
    source_version: str = Field(
        ...,
        description="Client version string",
        max_length=50,
        example="ext-0.1.3"
    )
    
    activities: List[ActivityIngestItem] = Field(
        ...,
        description="List of activities to ingest (max 1000)",
        min_items=1,
        max_items=1000
    )
    
    class Config:
        use_enum_values = True
    
    @validator('activities')
    def validate_batch_size(cls, v):
        """Enforce max batch size."""
        if len(v) > 1000:
            raise ValueError("Batch size exceeds maximum of 1000 activities")
        return v


class ActivityBatchResponseItem(BaseModel):
    """Result for a single activity in the batch."""
    
    client_generated_id: Optional[str]
    status: Literal["ingested", "duplicate", "failed"]
    activity_id: Optional[UUID] = None
    error: Optional[str] = None


class ActivityBatchResponse(BaseModel):
    """Response schema for batch activity ingestion."""
    
    ingested_count: int = Field(
        ...,
        description="Number of activities successfully ingested",
        example=98
    )
    
    duplicate_count: int = Field(
        ...,
        description="Number of activities skipped as duplicates",
        example=2
    )
    
    failed_count: int = Field(
        0,
        description="Number of activities that failed to ingest",
        example=0
    )
    
    results: List[ActivityBatchResponseItem] = Field(
        default_factory=list,
        description="Per-item results (optional, for debugging)"
    )
    
    processing_time_ms: float = Field(
        ...,
        description="Total processing time in milliseconds",
        example=156.3
    )


class IngestionMetadata(BaseModel):
    """Ingestion metadata schema."""
    
    schema_version: int = Field(1, description="Activity schema version")
    ip_hash: Optional[str] = Field(None, description="Hashed IP address")
    user_agent: Optional[str] = Field(None, max_length=500)
    client_timezone: Optional[str] = Field(None, max_length=50)
    received_at: datetime = Field(default_factory=datetime.utcnow)
