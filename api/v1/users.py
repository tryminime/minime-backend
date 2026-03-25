"""
User profile and settings API endpoints.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from typing import Optional, Dict, Any
import structlog
import uuid as uuid_lib

from database.postgres import get_db
from models import User
from auth.jwt_handler import security
from auth.password import hash_password, validate_password_strength

logger = structlog.get_logger()
router = APIRouter()
security = HTTPBearer()


# =====================================================
# REQUEST/RESPONSE MODELS
# =====================================================

class UpdateProfileRequest(BaseModel):
    full_name: Optional[str] = None
    bio: Optional[str] = None
    avatar_url: Optional[str] = None


class UpdatePreferencesRequest(BaseModel):
    preferences: Dict[str, Any]


class UpdatePrivacySettingsRequest(BaseModel):
    privacy_settings: Dict[str, Any]


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class UserProfileResponse(BaseModel):
    id: str
    email: str
    full_name: Optional[str]
    bio: Optional[str]
    avatar_url: Optional[str]
    tier: str
    subscription_status: str
    email_verified: bool
    preferences: Dict[str, Any]
    privacy_settings: Dict[str, Any]
    created_at: str
    updated_at: str


# =====================================================
# HELPER FUNCTIONS
# =====================================================

async def get_current_user_from_token(
    credentials: HTTPAuthorizationCredentials,
    db: AsyncSession
) -> User:
    """Extract and validate user from JWT token.
    
    Reuses decode_token/verify_token_type from jwt_handler to avoid duplication.
    """
    from auth.jwt_handler import decode_token, verify_token_type
    
    token = credentials.credentials
    payload = decode_token(token)
    
    if not payload or not verify_token_type(payload, "access"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token"
        )
    
    user_id = payload.get("sub")
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return user


# =====================================================
# ENDPOINTS
# =====================================================

@router.get("/me", response_model=UserProfileResponse)
async def get_me(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Get current user profile (alias for /me/profile)."""
    return await get_my_profile(credentials, db)


@router.get("/me/profile", response_model=UserProfileResponse)
async def get_my_profile(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's full profile."""
    user = await get_current_user_from_token(credentials, db)
    
    return UserProfileResponse(
        id=str(user.id),
        email=user.email,
        full_name=user.full_name,
        bio=user.bio,
        avatar_url=user.avatar_url,
        tier=user.tier,
        subscription_status=user.subscription_status,
        email_verified=user.email_verified,
        preferences=user.preferences or {},
        privacy_settings=user.privacy_settings or {},
        created_at=user.created_at.isoformat() if user.created_at else None,
        updated_at=user.updated_at.isoformat() if user.updated_at else None,
    )


@router.put("/me/profile")
async def update_my_profile(
    request: UpdateProfileRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Update current user's profile."""
    user = await get_current_user_from_token(credentials, db)
    
    # Update fields if provided
    if request.full_name is not None:
        user.full_name = request.full_name
    if request.bio is not None:
        user.bio = request.bio
    if request.avatar_url is not None:
        user.avatar_url = request.avatar_url
    
    await db.commit()
    await db.refresh(user)
    
    logger.info("User profile updated", user_id=str(user.id))
    
    return {"message": "Profile updated successfully"}


@router.get("/me/preferences")
async def get_my_preferences(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's preferences."""
    user = await get_current_user_from_token(credentials, db)
    return {"preferences": user.preferences or {}}


@router.put("/me/preferences")
async def update_my_preferences(
    request: UpdatePreferencesRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Update current user's preferences."""
    user = await get_current_user_from_token(credentials, db)
    
    user.preferences = request.preferences
    await db.commit()
    
    logger.info("User preferences updated", user_id=str(user.id))
    
    return {"message": "Preferences updated successfully"}


@router.get("/me/privacy")
async def get_my_privacy_settings(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's privacy settings."""
    user = await get_current_user_from_token(credentials, db)
    return {"privacy_settings": user.privacy_settings or {}}


@router.put("/me/privacy")
async def update_my_privacy_settings(
    request: UpdatePrivacySettingsRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Update current user's privacy settings."""
    user = await get_current_user_from_token(credentials, db)
    
    user.privacy_settings = request.privacy_settings
    await db.commit()
    
    logger.info("User privacy settings updated", user_id=str(user.id))
    
    return {"message": "Privacy settings updated successfully"}


@router.post("/me/change-password")
async def change_my_password(
    request: ChangePasswordRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Change current user's password."""
    from auth.password import verify_password
    
    user = await get_current_user_from_token(credentials, db)
    
    # Verify current password
    if not verify_password(request.current_password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect"
        )
    
    # Validate new password strength
    is_valid, error_message = validate_password_strength(request.new_password)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_message
        )
    
    # Update password
    user.password_hash = hash_password(request.new_password)
    await db.commit()
    
    logger.info("User password changed", user_id=str(user.id))
    
    return {"message": "Password changed successfully"}


@router.get("/me/export-data")
async def export_my_data(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Export all user data as JSON (GDPR compliance)."""
    from datetime import datetime, timezone
    from models import Activity
    from fastapi.responses import JSONResponse

    user = await get_current_user_from_token(credentials, db)

    # Get all activities
    result = await db.execute(
        select(Activity).where(Activity.user_id == user.id).order_by(Activity.occurred_at.desc())
    )
    activities = result.scalars().all()

    export = {
        "user": {
            "id": str(user.id),
            "email": user.email,
            "full_name": user.full_name,
            "bio": user.bio,
            "tier": user.tier,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "preferences": user.preferences or {},
            "privacy_settings": user.privacy_settings or {},
        },
        "activities": [
            {
                "id": str(a.id),
                "activity_type": a.activity_type,
                "source": a.source,
                "title": a.title,
                "description": a.description,
                "occurred_at": a.occurred_at.isoformat() if a.occurred_at else None,
                "duration_seconds": a.duration_seconds,
                "data": a.data,
            }
            for a in activities
        ],
        "total_activities": len(activities),
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }

    return JSONResponse(
        content=export,
        headers={"Content-Disposition": "attachment; filename=minime_export.json"}
    )


@router.delete("/me/data")
async def delete_my_data(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """Delete all user data across all datastores (keeps account).

    Deletes from: PG tables, Neo4j, Qdrant, Redis.
    """
    from models import Activity
    from sqlalchemy import delete as sql_delete, text

    user = await get_current_user_from_token(credentials, db)
    user_id = str(user.id)
    deleted = {}

    # ── PostgreSQL tables (order matters for FK constraints) ────────────────
    pg_tables = [
        "activity_entity_links",  # FK → activities + entities
        "entities",
        "content_items",
        "user_goals",
        "daily_metrics",
        "daily_summaries",
        "weekly_reports",
        "integrations",
        "activities",
        "sync_history",
    ]
    for table in pg_tables:
        try:
            if table == "activity_entity_links":
                # Junction table — join via activities for user scope
                result = await db.execute(text(
                    """DELETE FROM activity_entity_links
                       WHERE activity_id IN (
                           SELECT id FROM activities WHERE user_id = :uid
                       )"""
                ), {"uid": user_id})
            else:
                result = await db.execute(text(
                    f"DELETE FROM {table} WHERE user_id = :uid"
                ), {"uid": user_id})
            deleted[table] = result.rowcount
        except Exception as e:
            logger.warning("delete_table_error", table=table, error=str(e)[:200])
            deleted[table] = 0

    await db.commit()

    # ── Neo4j — delete all user nodes and their relationships ──────────────
    try:
        from database.neo4j_client import get_neo4j_driver
        driver = get_neo4j_driver()
        async with driver.session() as session:
            result = await session.run(
                "MATCH (n {user_id: $uid}) DETACH DELETE n RETURN count(n) as cnt",
                {"uid": user_id},
            )
            record = await result.single()
            deleted["neo4j_nodes"] = record["cnt"] if record else 0
    except Exception as e:
        logger.warning("delete_neo4j_error", error=str(e)[:200])
        deleted["neo4j_nodes"] = 0

    # ── Qdrant — delete vectors matching user_id ───────────────────────────
    try:
        from database.qdrant_client import get_qdrant_client
        from qdrant_client.models import Filter, FieldCondition, MatchValue
        client = get_qdrant_client()
        for coll_name in ["activities", "entities"]:
            try:
                await client.delete(
                    collection_name=coll_name,
                    points_selector=Filter(
                        must=[FieldCondition(key="user_id", match=MatchValue(value=user_id))]
                    ),
                )
                deleted[f"qdrant_{coll_name}"] = "cleared"
            except Exception:
                deleted[f"qdrant_{coll_name}"] = "skipped"
    except Exception as e:
        logger.warning("delete_qdrant_error", error=str(e)[:200])

    # ── Redis — delete user-specific keys ──────────────────────────────────
    try:
        from database.redis_client import get_redis_client
        redis = get_redis_client()
        prefixes = [f"user:{user_id}:", f"sync:{user_id}:", f"prefs:{user_id}"]
        redis_deleted = 0
        for prefix in prefixes:
            async for key in redis.scan_iter(match=f"{prefix}*"):
                await redis.delete(key)
                redis_deleted += 1
        deleted["redis_keys"] = redis_deleted
    except Exception as e:
        logger.warning("delete_redis_error", error=str(e)[:200])
        deleted["redis_keys"] = 0

    total = sum(v for v in deleted.values() if isinstance(v, int))
    logger.info("user_data_deleted", user_id=user_id, breakdown=deleted, total=total)

    return {
        "message": f"Deleted all data ({total} records)",
        "deleted_count": total,
        "breakdown": deleted,
    }


@router.delete("/me/account")
async def delete_my_account(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete current user's account (soft delete for GDPR compliance).
    This marks the account as deleted but retains data for compliance period.
    """
    from datetime import datetime, timezone
    
    user = await get_current_user_from_token(credentials, db)
    
    # Soft delete
    user.deleted_at = datetime.now(timezone.utc)
    user.subscription_status = "deleted"
    await db.commit()
    
    logger.info("User account deleted (soft)", user_id=str(user.id))
    
    return {"message": "Account deleted successfully"}
