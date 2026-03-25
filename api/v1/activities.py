"""
Activity API endpoints - Real CRUD implementation.
Handles activity creation, retrieval, listing, and deletion.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
import uuid
import structlog

from database.postgres import get_db
from models import Activity
from auth.jwt_handler import get_current_user as get_current_user_from_token
from websocket.manager import notify_activity_created

logger = structlog.get_logger()
router = APIRouter()
security = HTTPBearer()


# =====================================================
# REQUEST/RESPONSE MODELS
# =====================================================

class ActivityCreate(BaseModel):
    type: str  # window_focus, web_visit, meeting, break, idle, etc.
    source: Optional[str] = None  # desktop, web, mobile
    app: Optional[str] = None
    title: Optional[str] = None
    domain: Optional[str] = None
    url: Optional[str] = None
    duration_seconds: Optional[int] = None
    data: Optional[Dict[str, Any]] = None


class ActivityResponse(BaseModel):
    id: str
    type: str
    source: Optional[str]
    app: Optional[str]
    title: Optional[str]
    domain: Optional[str]
    duration_seconds: Optional[int]
    created_at: str
    data: Optional[dict] = None
    context: Optional[dict] = None


class ActivityBatchSync(BaseModel):
    activities: List[ActivityCreate]


# =====================================================
# ENDPOINTS
# =====================================================

@router.post("/", response_model=ActivityResponse, status_code=status.HTTP_201_CREATED)
async def create_activity(
    activity: ActivityCreate,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new activity event.
    Persists the activity to the database and returns the created record.
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    now = datetime.utcnow()
    # Normalize activity types
    TYPE_MAP = {"page_view": "web_visit", "tab_focus": "web_visit",
                 "window_focus": "app_focus", "reading_analytics": "reading_analytics"}
    normalized_type = TYPE_MAP.get(activity.type, activity.type)

    # Build context — merge client data into context alongside backward-compatible fields
    base_context = {
        "app_name": activity.app,
        "title": activity.title,
        "domain": activity.domain,
        "url": activity.url,
    }
    # Merge any reading/enrichment data from the data field
    if activity.data:
        base_context.update(activity.data)

    new_activity = Activity(
        id=uuid.uuid4(),
        user_id=user_id,
        type=normalized_type,
        source=activity.source or "api",
        app=activity.app,
        title=activity.title,
        domain=activity.domain,
        url=activity.url,
        duration_seconds=activity.duration_seconds,
        data=activity.data or {},
        context=base_context,
        ingestion_metadata={"source": "api", "received_at": now.isoformat()},
        occurred_at=now,
        received_at=now,
    )
    
    db.add(new_activity)
    await db.commit()
    await db.refresh(new_activity)
    
    logger.info("Activity created", activity_id=str(new_activity.id), type=activity.type, user_id=str(user_id))
    
    # Push real-time WebSocket notification
    try:
        await notify_activity_created(str(user_id), {
            "activity_id": str(new_activity.id),
            "type": activity.type,
        })
    except Exception:
        pass  # Non-critical — dashboard will still poll
    
    return ActivityResponse(
        id=str(new_activity.id),
        type=new_activity.type,
        source=new_activity.source,
        app=new_activity.app,
        title=new_activity.title,
        domain=new_activity.domain,
        duration_seconds=new_activity.duration_seconds,
        created_at=new_activity.created_at.isoformat() if new_activity.created_at else now.isoformat(),
        context=new_activity.context,
    )


class ActivityListResponse(BaseModel):
    """Paginated list of activities."""
    activities: List[ActivityResponse]
    total: int
    limit: int
    offset: int


@router.get("/", response_model=ActivityListResponse)
async def list_activities(
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    type: Optional[str] = None
):
    """
    List user's activities with pagination and filtering.
    Returns {activities, total, limit, offset} for frontend pagination.
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    # Count query (for total)
    count_stmt = select(func.count(Activity.id)).where(Activity.user_id == user_id)
    if type:
        count_stmt = count_stmt.where(Activity.type == type)
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0

    # Data query
    stmt = select(Activity).where(
        Activity.user_id == user_id
    ).order_by(Activity.created_at.desc()).limit(limit).offset(offset)

    if type:
        stmt = stmt.where(Activity.type == type)

    result = await db.execute(stmt)
    activities = result.scalars().all()

    logger.info("Activities listed", count=len(activities), total=total, user_id=str(user_id))

    return ActivityListResponse(
        activities=[
            ActivityResponse(
                id=str(a.id),
                type=a.type,
                source=a.source,
                app=a.app,
                title=a.title,
                domain=a.domain,
                duration_seconds=a.duration_seconds,
                created_at=a.created_at.isoformat() if a.created_at else "",
                data=a.data,
                context=a.context,
            )
            for a in activities
        ],
        total=total,
        limit=limit,
        offset=offset,
    )



@router.get("/{activity_id}", response_model=ActivityResponse)
async def get_activity(
    activity_id: str,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db)
):
    """
    Get a specific activity by ID.
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    try:
        act_uuid = UUID(activity_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid activity ID format"
        )
    
    stmt = select(Activity).where(
        Activity.id == act_uuid,
        Activity.user_id == user_id
    )
    result = await db.execute(stmt)
    activity = result.scalar_one_or_none()
    
    if not activity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Activity not found"
        )
    
    return ActivityResponse(
        id=str(activity.id),
        type=activity.type,
        source=activity.source,
        app=activity.app,
        title=activity.title,
        domain=activity.domain,
        duration_seconds=activity.duration_seconds,
        created_at=activity.created_at.isoformat() if activity.created_at else "",
        data=activity.data,
        context=activity.context,
    )


@router.post("/sync")
async def sync_activities(
    batch: ActivityBatchSync,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db)
):
    """
    Batch sync multiple activities (for offline sync from desktop/mobile).
    Creates all activities in a single transaction.
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    now = datetime.utcnow()
    created_ids = []
    
    for act in batch.activities:
        # Build context — merge client data into context alongside backward-compatible fields
        base_context = {
            "app_name": act.app,
            "title": act.title,
            "domain": act.domain,
            "url": act.url,
        }
        if act.data:
            base_context.update(act.data)

        new_activity = Activity(
            id=uuid.uuid4(),
            user_id=user_id,
            type=act.type,
            source=act.source or "sync",
            app=act.app,
            title=act.title,
            domain=act.domain,
            url=act.url,
            duration_seconds=act.duration_seconds,
            data=act.data or {},
            context=base_context,
            ingestion_metadata={"source": "sync", "received_at": now.isoformat()},
            occurred_at=now,
            received_at=now,
        )
        db.add(new_activity)
        created_ids.append(str(new_activity.id))
    
    await db.commit()
    
    logger.info("Activities synced", count=len(created_ids), user_id=str(user_id))
    
    # Push real-time WebSocket notification
    try:
        await notify_activity_created(str(user_id), {
            "count": len(created_ids),
            "source": "sync",
        })
    except Exception:
        pass  # Non-critical
    
    return {
        "synced": len(created_ids),
        "failed": 0,
        "activity_ids": created_ids,
        "message": f"Successfully synced {len(created_ids)} activities"
    }


@router.post("/bulk-delete")
async def bulk_delete_activities(
    body: dict,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db)
):
    """
    Bulk delete activities by list of IDs (for privacy/GDPR compliance).
    Body: { "activity_ids": ["uuid1", "uuid2", ...] }
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    activity_ids = body.get("activity_ids", [])
    
    if not activity_ids or len(activity_ids) > 500:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide 1-500 activity IDs"
        )
    
    try:
        uuids = [UUID(aid) for aid in activity_ids]
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid activity ID format"
        )
    
    from sqlalchemy import delete as sa_delete
    stmt = sa_delete(Activity).where(
        Activity.id.in_(uuids),
        Activity.user_id == user_id
    )
    result = await db.execute(stmt)
    await db.commit()
    
    deleted_count = result.rowcount
    logger.info("Bulk delete activities", count=deleted_count, user_id=str(user_id))
    
    return {"message": f"Deleted {deleted_count} activities", "deleted_count": deleted_count}


@router.delete("/{activity_id}")
async def delete_activity(
    activity_id: str,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete an activity (for privacy/GDPR compliance).
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    try:
        act_uuid = UUID(activity_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid activity ID format"
        )
    
    stmt = select(Activity).where(
        Activity.id == act_uuid,
        Activity.user_id == user_id
    )
    result = await db.execute(stmt)
    activity = result.scalar_one_or_none()
    
    if not activity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Activity not found"
        )
    
    await db.delete(activity)
    await db.commit()
    
    logger.info("Activity deleted", activity_id=activity_id, user_id=str(user_id))
    
    return {"message": f"Activity {activity_id} deleted successfully"}
