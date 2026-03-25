"""
Database models for Settings and AI Chat
"""
from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey, JSON, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

Base = declarative_base()

# ============================================================================
# USER SETTINGS MODELS
# ============================================================================

class UserSettings(Base):
    """User settings table - stores all user preferences in one place"""
    __tablename__ = "user_settings"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False, unique=True)
    
    # Profile settings (store as JSON)
    profile = Column(JSON, default={})
    
    # Tracking settings
    tracking_enabled = Column(Boolean, default=True)
    track_projects = Column(Boolean, default=True)
    track_files = Column(Boolean, default=True)
    track_commits = Column(Boolean, default=True)
    track_documents = Column(Boolean, default=True)
    track_ide = Column(Boolean, default=True)
    track_browser = Column(Boolean, default=True)
    track_writing = Column(Boolean, default=True)
    track_communication = Column(Boolean, default=True)
    track_video_calls = Column(Boolean, default=True)
    idle_threshold_minutes = Column(Integer, default=5)
    pause_on_lock = Column(Boolean, default=True)
    
    # Focus settings
    focus_enabled = Column(Boolean, default=True)
    auto_detect_deep_work = Column(Boolean, default=True)
    min_duration_minutes = Column(Integer, default=30)
    default_duration_minutes = Column(Integer, default=90)
    auto_break_minutes = Column(Integer, default=15)
    
    # Privacy settings
    https_only = Column(Boolean, default=True)
    filter_credit_cards = Column(Boolean, default=True)
    filter_ssn = Column(Boolean, default=True)
    filter_api_keys = Column(Boolean, default=True)
    filter_emails = Column(Boolean, default=True)
    local_encryption = Column(Boolean, default=True)
    e2e_encryption = Column(Boolean, default=True)
    retention_days = Column(Integer, default=365)
    auto_delete = Column(Boolean, default=True)
    
    # Notification settings
    in_app_enabled = Column(Boolean, default=True)
    email_enabled = Column(Boolean, default=True)
    browser_enabled = Column(Boolean, default=True)
    daily_summary = Column(Boolean, default=True)
    deadline_reminders = Column(Boolean, default=True)
    focus_reminders = Column(Boolean, default=True)
    break_suggestions = Column(Boolean, default=True)
    wellness_summary = Column(Boolean, default=True)
    ai_insights = Column(Boolean, default=True)
    sync_errors = Column(Boolean, default=True)
    dnd_enabled = Column(Boolean, default=True)
    dnd_from = Column(String, default="18:00")
    dnd_to = Column(String, default="09:00")
    
    # Appearance
    theme = Column(String, default="system")
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="settings")


class TwoFactorAuth(Base):
    """Two-factor authentication settings"""
    __tablename__ = "two_factor_auth"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False, unique=True)
    
    # TOTP secret (encrypted)
    secret = Column(String, nullable=False)
    
    # Backup codes (encrypted, stored as JSON array)
    backup_codes = Column(JSON, default=[])
    
    # Status
    enabled = Column(Boolean, default=False)
    verified = Column(Boolean, default=False)
    
    # Timestamps
    enabled_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="two_factor")


class Backup(Base):
    """User data backups"""
    __tablename__ = "backups"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    
    # Backup metadata
    filename = Column(String, nullable=False)
    size_bytes = Column(Integer, default=0)
    format = Column(String, default="zip")  # zip, json, csv
    
    # Storage location (S3, local, etc.)
    storage_type = Column(String, default="local")
    storage_path = Column(String, nullable=False)
    
    # Status
    status = Column(String, default="completed")  # pending, in_progress, completed, failed
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="backups")


# ============================================================================
# AI CHAT MODELS
# ============================================================================

class Conversation(Base):
    """AI chat conversations"""
    __tablename__ = "conversations"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    
    # Conversation metadata
    title = Column(String, nullable=True)  # Auto-generated from first message
    summary = Column(Text, nullable=True)  # AI-generated summary
    
    # Settings
    context_enabled = Column(Boolean, default=True)  # Use user data for context
    
    # Status
    archived = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_message_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="conversations")
    messages = relationship("ChatMessage", back_populates="conversation", cascade="all, delete-orphan")


class ChatMessage(Base):
    """Individual messages in AI chat conversations"""
    __tablename__ = "chat_messages"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    conversation_id = Column(String, ForeignKey("conversations.id"), nullable=False)
    
    # Message content
    role = Column(String, nullable=False)  # "user" or "assistant"
    content = Column(Text, nullable=False)
    
    # Metadata
    model = Column(String, nullable=True)  # LLM model used (gpt-4, llama2, etc.)
    tokens = Column(Integer, nullable=True)  # Token count if available
    context_used = Column(JSON, nullable=True)  # What context was provided to AI
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    conversation = relationship("Conversation", back_populates="messages")


class AIInteraction(Base):
    """Track AI interactions for analytics and billing"""
    __tablename__ = "ai_interactions"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    
    # Interaction details
    interaction_type = Column(String, nullable=False)  # chat, insight, report, summary
    model = Column(String, nullable=True)
    
    # Usage metrics
    tokens_input = Column(Integer, default=0)
    tokens_output = Column(Integer, default=0)
    tokens_total = Column(Integer, default=0)
    
    # Cost (if using paid API)
    cost_usd = Column(Integer, default=0)  # Store as cents
    
    # Performance
    latency_ms = Column(Integer, nullable=True)
    
    # Status
    success = Column(Boolean, default=True)
    error_message = Column(Text, nullable=True)
    
    # Timestamp
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="ai_interactions")


# ============================================================================
# ADD TO EXISTING USER MODEL (reference)
# ============================================================================
"""
Add these relationships to your existing User model:

class User(Base):
    # ... existing fields ...
    
    # Relationships
    settings = relationship("UserSettings", back_populates="user", uselist=False)
    two_factor = relationship("TwoFactorAuth", back_populates="user", uselist=False)
    backups = relationship("Backup", back_populates="user")
    conversations = relationship("Conversation", back_populates="user")
    ai_interactions = relationship("AIInteraction", back_populates="user")
"""
