"""
SQLAlchemy ORM models for PostgreSQL database.
"""

from sqlalchemy import Column, String, DateTime, Boolean, JSON, Integer, Text, Index, ForeignKey, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from datetime import datetime
import uuid

from database.postgres import Base


class User(Base):
    """User account model."""
    __tablename__ = "users"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=True)
    
    # Subscription and tier
    tier = Column(String(50), default="free", nullable=False)  # free, premium, enterprise
    subscription_status = Column(String(50), default="active", nullable=False)
    
    # Admin role
    is_superadmin = Column(Boolean, default=False, nullable=False)
    
    # Profile
    avatar_url = Column(Text, nullable=True)
    bio = Column(Text, nullable=True)
    
    # Preferences and privacy
    preferences = Column(JSON, default=dict, nullable=False)
    privacy_settings = Column(JSON, default=dict, nullable=False)
    
    # Email verification
    email_verified = Column(Boolean, default=False, nullable=False)
    email_verification_token = Column(String(255), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_users_tier', 'tier'),
    )
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            "id": str(self.id),
            "email": self.email,
            "full_name": self.full_name,
            "tier": self.tier,
            "subscription_status": self.subscription_status,
            "is_superadmin": self.is_superadmin,
            "avatar_url": self.avatar_url,
            "bio": self.bio,
            "preferences": self.preferences,
            "privacy_settings": self.privacy_settings,
            "email_verified": self.email_verified,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class Session(Base):
    """User session model for refresh token management."""
    __tablename__ = "sessions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    refresh_token = Column(String(512), nullable=False, unique=True, index=True)
    device_info = Column(JSON, default=dict, nullable=True)
    device_name = Column(String(255), nullable=True)   # human-readable label, e.g. "Work MacBook"
    remember_device = Column(Boolean, default=False, nullable=False)  # 90-day vs 7-day token
    ip_address = Column(String(45), nullable=True)  # IPv6 max length
    user_agent = Column(Text, nullable=True)
    revoked = Column(Boolean, default=False, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    # Indexes
    __table_args__ = (
        Index('idx_sessions_user_id', 'user_id'),
        Index('idx_sessions_refresh_token', 'refresh_token'),
    )



class Activity(Base):
    """Activity event model."""
    __tablename__ = "activities"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Activity type and source
    type = Column(String(50), nullable=False, index=True)  # window_focus, web_visit, meeting, etc.
    source = Column(String(50), nullable=True)  # desktop, web, mobile
    source_version = Column(String(50), nullable=True)  # e.g., "ext-0.1.3", "desktop-1.2.0"
    client_generated_id = Column(String(255), nullable=True, index=True)  # for idempotent ingestion
    
    # Activity details
    app = Column(String(255), nullable=True)
    title = Column(Text, nullable=True)
    domain = Column(String(255), nullable=True, index=True)
    url = Column(Text, nullable=True)
    
    # Duration
    duration_seconds = Column(Integer, nullable=True)
    
    # Flexible data storage
    data = Column(JSON, default=dict, nullable=True)
    
    # Flexible context storage (new schema)
    context = Column(JSON, default=dict, nullable=False)  # Replaces individual fields over time
    ingestion_metadata = Column(JSON, default=dict, nullable=False)  # schema_version, ip_hash, etc.
    
    # Timestamps
    occurred_at = Column(DateTime(timezone=True), nullable=False, index=True)  # When activity happened
    received_at = Column(DateTime(timezone=True), server_default=func.now())  # When server received it
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    synced_at = Column(DateTime(timezone=True), nullable=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_activities_user_id', 'user_id'),
        Index('idx_activities_type', 'type'),
        Index('idx_activities_created_at', 'created_at'),
        Index('idx_activities_user_created', 'user_id', 'created_at'),
        Index('idx_activities_user_source_client', 'user_id', 'source', 'client_generated_id'),
        Index('idx_activities_user_occurred', 'user_id', 'occurred_at'),
        Index('idx_activities_user_type_occurred', 'user_id', 'type', 'occurred_at'),
    )
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "type": self.type,
            "source": self.source,
            "source_version": self.source_version,
            "client_generated_id": self.client_generated_id,
            # Legacy fields (backward compatibility)
            "app": self.app,
            "title": self.title,
            "domain": self.domain,
            "url": self.url,
            # New schema
            "context": self.context,
            "duration_seconds": self.duration_seconds,
            "data": self.data,
            "ingestion_metadata": self.ingestion_metadata,
            # Timestamps
            "occurred_at": self.occurred_at.isoformat() if self.occurred_at else None,
            "received_at": self.received_at.isoformat() if self.received_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "synced_at": self.synced_at.isoformat() if self.synced_at else None,
        }


class Entity(Base):
    """Extracted entity model (NER results). Matches actual DB schema."""
    __tablename__ = "entities"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Entity details (matches actual DB columns)
    entity_type = Column(String(50), nullable=False)  # person, project, skill, concept, organization, artifact, event, interaction
    name = Column(String(500), nullable=False)
    canonical_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Confidence and occurrence count
    confidence = Column(Float, default=1.0, nullable=True)
    occurrence_count = Column(Integer, default=1, nullable=True)
    
    # Metadata (JSONB in DB) - use 'entity_metadata' as Python attr since 'metadata' is reserved by SQLAlchemy
    entity_metadata = Column('metadata', JSON, nullable=True)
    
    # Timestamps
    first_seen = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    last_seen = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=True)
    synced_at = Column(DateTime(timezone=True), nullable=True)
    
    # Use extend_existing to avoid conflicts with existing indexes
    __table_args__ = (
        {'extend_existing': True},
    )
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "name": self.name,
            "entity_type": self.entity_type,
            "canonical_id": str(self.canonical_id) if self.canonical_id else None,
            "confidence": self.confidence,
            "occurrence_count": self.occurrence_count,
            "metadata": self.entity_metadata,
            "first_seen": self.first_seen.isoformat() if self.first_seen else None,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
        }


class ActivityEntityLink(Base):
    """
    Links entities to activities. Matches actual DB table 'activity_entity_links'.
    
    Composite primary key: (activity_id, entity_id).
    """
    __tablename__ = "activity_entity_links"
    
    activity_id = Column(UUID(as_uuid=True), ForeignKey('activities.id', ondelete='CASCADE'), primary_key=True)
    entity_id = Column(UUID(as_uuid=True), ForeignKey('entities.id', ondelete='CASCADE'), primary_key=True)
    relevance_score = Column(Float, default=1.0, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=True)
    synced_at = Column(DateTime(timezone=True), nullable=True)
    
    __table_args__ = (
        {'extend_existing': True},
    )
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            "activity_id": str(self.activity_id),
            "entity_id": str(self.entity_id),
            "relevance_score": self.relevance_score,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# Keep EntityOccurrence as an alias for backward compatibility in imports
EntityOccurrence = ActivityEntityLink



class AuditLog(Base):
    """Audit log for compliance."""
    __tablename__ = "audit_logs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Action details
    action = Column(String(100), nullable=False, index=True)
    resource_type = Column(String(100), nullable=True)
    resource_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Request details
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    
    # Changes
    changes = Column(JSON, default=dict, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_audit_logs_user_id', 'user_id'),
        Index('idx_audit_logs_action', 'action'),
        Index('idx_audit_logs_created_at', 'created_at'),
    )


class UserGoal(Base):
    """User goals model — persisted to PostgreSQL."""
    __tablename__ = "user_goals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)

    title = Column(String(255), nullable=False)
    category = Column(String(50), nullable=False, default='custom')  # focus, productivity, learning, wellness, custom
    target_value = Column(Float, nullable=False, default=1.0)
    current_value = Column(Float, nullable=False, default=0.0)
    unit = Column(String(50), nullable=False, default='sessions')
    deadline = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(50), nullable=False, default='active')  # active, completed, paused
    streak_count = Column(Integer, nullable=False, default=0)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    synced_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index('idx_user_goals_user_id', 'user_id'),
        Index('idx_user_goals_status', 'status'),
        {'extend_existing': True},
    )


class ContentItem(Base):
    """Knowledge Base content item — persisted to PostgreSQL."""
    __tablename__ = "content_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)

    # Content metadata
    url = Column(Text, default="")
    title = Column(String(500), nullable=False)
    doc_type = Column(String(50), default="webpage")   # webpage, pdf, docx, xlsx, pptx, code
    full_text = Column(Text, nullable=False)            # stored for RAG re-indexing
    text_snippet = Column(String(500), default="")

    # NLP analysis results
    word_count = Column(Integer, default=0)
    reading_time_seconds = Column(Integer, default=0)
    keyphrases = Column(JSON, default=list)
    entities = Column(JSON, default=list)
    topic = Column(JSON, nullable=True)                 # {"primary": str, "confidence": float}
    language = Column(String(10), default="en")
    complexity = Column(Float, default=0.0)

    # Extra metadata
    content_metadata = Column('metadata', JSON, default=dict)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    synced_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index('idx_content_items_user_id', 'user_id'),
        Index('idx_content_items_user_created', 'user_id', 'created_at'),
        Index('idx_content_items_doc_type', 'doc_type'),
        {'extend_existing': True},
    )

    def to_dict(self):
        """Convert to API-compatible dictionary."""
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "url": self.url or "",
            "title": self.title,
            "doc_type": self.doc_type,
            "full_text": self.full_text,
            "text_snippet": self.text_snippet or "",
            "word_count": self.word_count or 0,
            "reading_time_seconds": self.reading_time_seconds or 0,
            "keyphrases": self.keyphrases or [],
            "entities": self.entities or [],
            "topic": self.topic,
            "language": self.language or "en",
            "complexity": self.complexity or 0.0,
            "metadata": self.content_metadata or {},
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class SyncHistory(Base):
    """Tracks individual cloud sync runs for a user."""
    __tablename__ = "sync_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)

    started_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(20), nullable=False, default="running")  # running, completed, failed
    trigger = Column(String(20), nullable=False, default="manual")  # manual, scheduled
    results = Column(JSON, default=dict, nullable=True)  # per-target breakdown
    error = Column(Text, nullable=True)
    records_synced = Column(Integer, default=0, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index('idx_sync_history_user_id', 'user_id'),
        Index('idx_sync_history_started_at', 'started_at'),
        {'extend_existing': True},
    )

    def to_dict(self):
        return {
            "id": str(self.id),
            "user_id": str(self.user_id),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "trigger": self.trigger,
            "results": self.results,
            "error": self.error,
            "records_synced": self.records_synced,
        }


