"""
FastAPI backend endpoints for Settings and AI Chat integration
"""
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from auth.jwt_handler import get_current_user
from database.postgres_client import get_db
from sqlalchemy.orm import Session
import pyotp
import base64
import io
from datetime import datetime

# Settings Router
settings_router = APIRouter(prefix="/api/settings", tags=["settings"])

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class UserProfile(BaseModel):
    full_name: str
    email: str
    account_type: str
    timezone: str
    two_factor_enabled: bool

class TrackingSettings(BaseModel):
    enabled: bool
    track_projects: bool
    track_files: bool
    track_commits: bool
    track_documents: bool
    track_ide: bool
    track_browser: bool
    track_writing: bool
    track_communication: bool
    track_video_calls: bool
    idle_threshold_minutes: int
    pause_on_lock: bool

class FocusSettings(BaseModel):
    enabled: bool
    auto_detect_deep_work: bool
    min_duration_minutes: int
    default_duration_minutes: int
    auto_break_minutes: int

class PrivacySettings(BaseModel):
    https_only: bool
    filter_credit_cards: bool
    filter_ssn: bool
    filter_api_keys: bool
    filter_emails: bool
    local_encryption: bool
    e2e_encryption: bool
    retention_days: int
    auto_delete: bool

class NotificationSettings(BaseModel):
    in_app_enabled: bool
    email_enabled: bool
    browser_enabled: bool
    daily_summary: bool
    deadline_reminders: bool
    focus_reminders: bool
    break_suggestions: bool
    wellness_summary: bool
    ai_insights: bool
    sync_errors: bool
    dnd_enabled: bool
    dnd_from: str
    dnd_to: str

class AllSettings(BaseModel):
    profile: UserProfile
    tracking: TrackingSettings
    focus: FocusSettings
    privacy: PrivacySettings
    notifications: NotificationSettings
    theme: str

# ============================================================================
# SETTINGS ENDPOINTS
# ============================================================================

@settings_router.get("", response_model=AllSettings)
async def get_all_settings(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all user settings"""
    # Fetch settings from database
    # This is a placeholder - implement actual DB query
    return {
        "profile": {
            "full_name": current_user.get("full_name", ""),
            "email": current_user.get("email", ""),
            "account_type": current_user.get("account_type", "PhD Researcher"),
            "timezone": current_user.get("timezone", "America/Chicago"),
            "two_factor_enabled": current_user.get("two_factor_enabled", False)
        },
        "tracking": {
            "enabled": True,
            "track_projects": True,
            "track_files": True,
            "track_commits": True,
            "track_documents": True,
            "track_ide": True,
            "track_browser": True,
            "track_writing": True,
            "track_communication": True,
            "track_video_calls": True,
            "idle_threshold_minutes": 5,
            "pause_on_lock": True
        },
        "focus": {
            "enabled": True,
            "auto_detect_deep_work": True,
            "min_duration_minutes": 30,
            "default_duration_minutes": 90,
            "auto_break_minutes": 15
        },
        "privacy": {
            "https_only": True,
            "filter_credit_cards": True,
            "filter_ssn": True,
            "filter_api_keys": True,
            "filter_emails": True,
            "local_encryption": True,
            "e2e_encryption": True,
            "retention_days": 365,
            "auto_delete": True
        },
        "notifications": {
            "in_app_enabled": True,
            "email_enabled": True,
            "browser_enabled": True,
            "daily_summary": True,
            "deadline_reminders": True,
            "focus_reminders": True,
            "break_suggestions": True,
            "wellness_summary": True,
            "ai_insights": True,
            "sync_errors": True,
            "dnd_enabled": True,
            "dnd_from": "18:00",
            "dnd_to": "09:00"
        },
        "theme": "system"
    }

@settings_router.put("/profile")
async def update_profile(
    profile: UserProfile,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update user profile"""
    # Update profile in database
    # Placeholder implementation
    return {"message": "Profile updated successfully"}

@settings_router.put("/tracking")
async def update_tracking_settings(
    settings: TrackingSettings,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update tracking settings"""
    # Update tracking settings in database
    return {"message": "Tracking settings updated successfully"}

@settings_router.put("/focus")
async def update_focus_settings(
    settings: FocusSettings,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update focus settings"""
    # Update focus settings in database
    return {"message": "Focus settings updated successfully"}

@settings_router.put("/privacy")
async def update_privacy_settings(
    settings: PrivacySettings,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update privacy settings"""
    # Update privacy settings in database
    return {"message": "Privacy settings updated successfully"}

@settings_router.put("/notifications")
async def update_notification_settings(
    settings: NotificationSettings,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update notification settings"""
    # Update notification settings in database
    return {"message": "Notification settings updated successfully"}

# ============================================================================
# AUTHENTICATION & SECURITY ENDPOINTS
# ============================================================================

class PasswordChange(BaseModel):
    old_password: str
    new_password: str

@settings_router.post("/auth/change-password")
async def change_password(
    passwords: PasswordChange,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Change user password"""
    from auth.password import verify_password, hash_password
    
    # Verify old password
    # Hash and update new password
    # Placeholder implementation
    return {"message": "Password changed successfully"}

@settings_router.post("/auth/2fa/enable")
async def enable_2fa(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Enable 2FA for user account"""
    # Generate TOTP secret
    secret = pyotp.random_base32()
    
    # Generate QR code URI
    totp = pyotp.TOTP(secret)
    uri = totp.provisioning_uri(
        name=current_user.get("email"),
        issuer_name="MiniMe"
    )
    
    # Save secret to database (encrypted)
    # Return secret and QR code URI
    return {
        "secret": secret,
        "qr_code_uri": uri,
        "backup_codes": [
            "XXXX-XXXX-XXXX",
            "YYYY-YYYY-YYYY",
            "ZZZZ-ZZZZ-ZZZZ"
        ]
    }

class Disable2FA(BaseModel):
    code: str

@settings_router.post("/auth/2fa/disable")
async def disable_2fa(
    request: Disable2FA,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Disable 2FA for user account"""
    # Verify 2FA code
    # Remove 2FA from database
    return {"message": "2FA disabled successfully"}

# ============================================================================
# DATA MANAGEMENT ENDPOINTS
# ============================================================================

@settings_router.get("/data/export")
async def export_data(
    format: str = "json",
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Export all user data"""
    # Fetch all user data
    # Format as JSON/CSV/other
    # Return file
    return {"message": "Export initiated", "format": format}

@settings_router.post("/backups/create")
async def create_backup(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Create a backup of user data"""
    # Create backup
    # Store backup metadata
    return {"message": "Backup created successfully", "backup_id": "backup_123"}

@settings_router.get("/backups")
async def get_backups(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get list of user backups"""
    return {
        "backups": [
            {"id": "backup_123", "created_at": "2026-01-31T20:00:00Z", "size_mb": 2.4},
            {"id": "backup_122", "created_at": "2026-01-30T20:00:00Z", "size_mb": 2.3}
        ]
    }
