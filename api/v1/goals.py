"""
Goal Tracking API Router

Exposes GoalTrackingService via REST endpoints:
- CRUD for goals
- Progress tracking
- Status management (complete, pause, resume, archive)
- Analytics (stats, streaks, deadlines)
- Auto-progress from activity data

All endpoints are user-scoped via JWT authentication.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import structlog

from auth.jwt_handler import decode_token, verify_token_type
from services.goal_tracking_service import goal_tracking_service

logger = structlog.get_logger()
security = HTTPBearer()

router = APIRouter(prefix="/api/v1/goals", tags=["goals"])


def _get_user_id(credentials: HTTPAuthorizationCredentials) -> str:
    """Extract user_id from JWT token."""
    token = credentials.credentials
    payload = decode_token(token)
    if not payload or not verify_token_type(payload, "access"):
        raise HTTPException(status_code=401, detail="Invalid token")
    user_id = payload.get("sub") or payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    return user_id


# ============================================================================
# REQUEST MODELS
# ============================================================================

class GoalCreateRequest(BaseModel):
    title: str
    category: str = "custom"
    description: str = ""
    target_value: float = 100
    unit: str = "percent"
    deadline: Optional[str] = None
    milestones: Optional[List[Dict[str, Any]]] = None


class GoalUpdateRequest(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    target_value: Optional[float] = None
    deadline: Optional[str] = None


class ProgressUpdateRequest(BaseModel):
    value: float
    note: Optional[str] = None


class ProgressIncrementRequest(BaseModel):
    delta: float
    note: Optional[str] = None


# ============================================================================
# CRUD ENDPOINTS
# ============================================================================

@router.post("")
async def create_goal(
    request: GoalCreateRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Create a new goal."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.create_goal(
        user_id=user_id,
        title=request.title,
        category=request.category,
        description=request.description,
        target_value=request.target_value,
        unit=request.unit,
        deadline=request.deadline,
        milestones=request.milestones,
    )
    return result


@router.get("")
async def list_goals(
    status_filter: Optional[str] = None,
    category: Optional[str] = None,
    include_archived: bool = False,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """List all goals for the authenticated user."""
    user_id = _get_user_id(credentials)
    return goal_tracking_service.list_goals(
        user_id=user_id,
        status=status_filter,
        category=category,
        include_archived=include_archived,
    )


@router.get("/stats")
async def get_goal_stats(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get goal tracking statistics."""
    user_id = _get_user_id(credentials)
    return goal_tracking_service.get_goal_stats(user_id)


@router.get("/streaks")
async def get_completion_streaks(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get goal completion streak data."""
    user_id = _get_user_id(credentials)
    return goal_tracking_service.get_completion_streaks(user_id)


@router.get("/deadlines")
async def get_upcoming_deadlines(
    days_ahead: int = 7,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get goals with upcoming deadlines."""
    user_id = _get_user_id(credentials)
    return goal_tracking_service.get_upcoming_deadlines(user_id, days_ahead=days_ahead)


@router.get("/{goal_id}")
async def get_goal(
    goal_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Get a single goal by ID."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.get_goal(user_id, goal_id)
    if not result:
        raise HTTPException(status_code=404, detail="Goal not found")
    return result


@router.put("/{goal_id}")
async def update_goal(
    goal_id: str,
    request: GoalUpdateRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Update goal metadata."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.update_goal(
        user_id=user_id,
        goal_id=goal_id,
        title=request.title,
        description=request.description,
        target_value=request.target_value,
        deadline=request.deadline,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Goal not found")
    return result


@router.delete("/{goal_id}")
async def delete_goal(
    goal_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Delete a goal permanently."""
    user_id = _get_user_id(credentials)
    deleted = goal_tracking_service.delete_goal(user_id, goal_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Goal not found")
    return {"success": True}


# ============================================================================
# PROGRESS ENDPOINTS
# ============================================================================

@router.post("/{goal_id}/progress")
async def update_progress(
    goal_id: str,
    request: ProgressUpdateRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Update goal progress (absolute value)."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.update_progress(
        user_id=user_id,
        goal_id=goal_id,
        value=request.value,
        note=request.note,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Goal not found")
    return result


@router.post("/{goal_id}/increment")
async def increment_progress(
    goal_id: str,
    request: ProgressIncrementRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Increment goal progress by a delta."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.add_progress_increment(
        user_id=user_id,
        goal_id=goal_id,
        delta=request.delta,
        note=request.note,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Goal not found")
    return result


# ============================================================================
# STATUS ENDPOINTS
# ============================================================================

@router.post("/{goal_id}/complete")
async def complete_goal(
    goal_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Mark a goal as completed."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.complete_goal(user_id, goal_id)
    if not result:
        raise HTTPException(status_code=404, detail="Goal not found")
    return result


@router.post("/{goal_id}/pause")
async def pause_goal(
    goal_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Pause a goal."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.pause_goal(user_id, goal_id)
    if not result:
        raise HTTPException(status_code=404, detail="Goal not found")
    return result


@router.post("/{goal_id}/resume")
async def resume_goal(
    goal_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Resume a paused goal."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.resume_goal(user_id, goal_id)
    if not result:
        raise HTTPException(status_code=404, detail="Goal not found")
    return result


@router.post("/{goal_id}/archive")
async def archive_goal(
    goal_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Archive a goal."""
    user_id = _get_user_id(credentials)
    result = goal_tracking_service.archive_goal(user_id, goal_id)
    if not result:
        raise HTTPException(status_code=404, detail="Goal not found")
    return result
