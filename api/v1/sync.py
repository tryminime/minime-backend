"""
Sync API endpoints — Phase 3b/3c.

Provides:
  POST /api/v1/sync/trigger     — manual sync now (Pro/Enterprise)
  GET  /api/v1/sync/schedule    — get user's sync schedule
  PUT  /api/v1/sync/schedule    — update schedule (frequency / time)
  GET  /api/v1/sync/history     — last 20 sync runs
"""

from __future__ import annotations

import uuid
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from auth.jwt_handler import get_current_user
from database.postgres import get_db
from middleware.feature_gate import FeatureGate
from models import User

logger = structlog.get_logger()
router = APIRouter(prefix="/api/v1/sync", tags=["Cloud Sync"])


# ── Request / Response models ────────────────────────────────────────────────

class ScheduleUpdate(BaseModel):
    frequency: str = "daily"        # daily, twice_daily, weekly, biweekly, monthly
    sync_time: str = "02:00"        # HH:MM in user's local time


class ScheduleResponse(BaseModel):
    frequency: str
    sync_time: str
    last_synced_at: Optional[str] = None
    next_sync_at: Optional[str] = None


# ── Auth helper (same pattern as cloud_backup.py) ────────────────────────────

async def _require_pro_user(
    user_data: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Validate JWT and load User ORM for tier check."""
    user_id = user_data.get("user_id") or user_data.get("sub")
    if not user_id:
        raise HTTPException(401, "Invalid token")
    result = await db.execute(
        text("SELECT * FROM users WHERE id = :uid"),
        {"uid": user_id},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(404, "User not found")

    # Build User ORM from row
    user = User()
    for col in row.keys():
        if hasattr(user, col):
            setattr(user, col, row[col])

    FeatureGate(user).require("cloud_sync")
    return user


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/trigger")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    user: User = Depends(_require_pro_user),
):
    """Manually trigger a full cloud sync (Pro/Enterprise only).
    
    Runs in background so the HTTP response is instant.
    """
    from services.cloud_sync_service import CloudSyncService

    user_id = str(user.id)
    logger.info("manual_sync_triggered", user_id=user_id)

    async def _run_sync():
        try:
            service = CloudSyncService()
            await service.sync_all(user_id, trigger="manual")
        except Exception as e:
            logger.error("background_sync_failed", user_id=user_id, error=str(e))

    background_tasks.add_task(_run_sync)

    return {
        "status": "started",
        "message": "Cloud sync started in background",
    }


@router.get("/schedule")
async def get_schedule(
    user: User = Depends(_require_pro_user),
    db: AsyncSession = Depends(get_db),
):
    """Get user's sync schedule settings."""
    prefs = user.preferences or {}
    return ScheduleResponse(
        frequency=prefs.get("sync_frequency", "daily"),
        sync_time=prefs.get("sync_time", "02:00"),
        last_synced_at=prefs.get("last_synced_at"),
        next_sync_at=prefs.get("next_sync_at"),
    )


@router.put("/schedule")
async def update_schedule(
    body: ScheduleUpdate,
    user: User = Depends(_require_pro_user),
    db: AsyncSession = Depends(get_db),
):
    """Update sync schedule (frequency + preferred time)."""
    valid_frequencies = {"daily", "twice_daily", "weekly", "biweekly", "monthly"}
    if body.frequency not in valid_frequencies:
        raise HTTPException(400, f"Invalid frequency. Choose from: {valid_frequencies}")

    user_id = str(user.id)

    # Update preferences
    await db.execute(
        text("""
            UPDATE users
            SET preferences = jsonb_set(
                jsonb_set(
                    COALESCE(preferences, '{}'::jsonb),
                    '{sync_frequency}',
                    to_jsonb(CAST(:freq AS text))
                ),
                '{sync_time}',
                to_jsonb(CAST(:time AS text))
            )
            WHERE id = :uid
        """),
        {"freq": body.frequency, "time": body.sync_time, "uid": uuid.UUID(user_id)},
    )
    await db.commit()

    # Re-register with scheduler
    try:
        from services.sync_scheduler import schedule_user_sync
        await schedule_user_sync(user_id, body.frequency, body.sync_time)
    except Exception as e:
        logger.warning("scheduler_update_failed", error=str(e))

    logger.info("sync_schedule_updated", user_id=user_id, frequency=body.frequency)
    return {"frequency": body.frequency, "sync_time": body.sync_time, "updated": True}


@router.get("/history")
async def get_sync_history(
    user: User = Depends(_require_pro_user),
    db: AsyncSession = Depends(get_db),
):
    """Return last 20 sync runs for the user."""
    result = await db.execute(
        text("""
            SELECT id, started_at, completed_at, status, trigger,
                   results, error, records_synced
            FROM sync_history
            WHERE user_id = :uid
            ORDER BY started_at DESC
            LIMIT 20
        """),
        {"uid": str(user.id)},
    )
    rows = result.mappings().all()
    return {
        "history": [
            {
                "id": str(r["id"]),
                "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
                "status": r["status"],
                "trigger": r["trigger"],
                "results": r["results"],
                "error": r["error"],
                "records_synced": r["records_synced"],
            }
            for r in rows
        ],
        "count": len(rows),
    }


@router.post("/restore")
async def restore_from_cloud(
    background_tasks: BackgroundTasks,
    user: User = Depends(_require_pro_user),
):
    """Pull all data from cloud DBs → local (Pro/Enterprise only).

    Runs in background so the HTTP response is instant.
    Track progress via GET /api/v1/sync/history (trigger='restore').
    """
    from services.cloud_sync_service import CloudSyncService

    user_id = str(user.id)
    logger.info("cloud_restore_triggered", user_id=user_id)

    async def _run_restore():
        try:
            service = CloudSyncService()
            await service.restore_all(user_id)
        except Exception as e:
            logger.error("background_restore_failed", user_id=user_id, error=str(e))

    background_tasks.add_task(_run_restore)

    return {
        "status": "started",
        "message": "Cloud restore started in background",
    }

