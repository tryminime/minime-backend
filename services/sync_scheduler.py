"""
Sync scheduler — Phase 3c.

Uses APScheduler's AsyncIOScheduler to run periodic cloud sync jobs.
On startup, loads all Pro/Enterprise users' schedules and registers jobs.
"""

from __future__ import annotations

import structlog
from datetime import datetime, timezone

logger = structlog.get_logger()

# ── Frequency → interval mapping ──────────────────────────────────────────────

FREQUENCY_MAP = {
    "daily":       {"hours": 24},
    "twice_daily": {"hours": 12},
    "weekly":      {"days": 7},
    "biweekly":    {"days": 14},
    "monthly":     {"days": 30},
}

_scheduler = None


def _get_scheduler():
    global _scheduler
    if _scheduler is None:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        _scheduler = AsyncIOScheduler(
            job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600}
        )
    return _scheduler


# ── Job function ──────────────────────────────────────────────────────────────

async def _run_sync_job(user_id: str):
    """Execute a scheduled sync for one user."""
    try:
        from services.cloud_sync_service import CloudSyncService
        service = CloudSyncService()
        result = await service.sync_all(user_id, trigger="scheduled")
        logger.info("scheduled_sync_done", user_id=user_id, ok=result.ok, records=result.total_records)
    except Exception as e:
        logger.error("scheduled_sync_failed", user_id=user_id, error=str(e))


# ── Public API ────────────────────────────────────────────────────────────────

async def schedule_user_sync(user_id: str, frequency: str, sync_time: str = "02:00"):
    """Add or replace a user's scheduled sync job."""
    scheduler = _get_scheduler()
    job_id = f"cloud_sync_{user_id}"

    interval = FREQUENCY_MAP.get(frequency)
    if not interval:
        logger.warning("invalid_sync_frequency", frequency=frequency)
        return

    # Parse preferred time
    try:
        hour, minute = int(sync_time.split(":")[0]), int(sync_time.split(":")[1])
    except (ValueError, IndexError):
        hour, minute = 2, 0

    # Remove existing job if any
    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)

    scheduler.add_job(
        _run_sync_job,
        trigger="interval",
        id=job_id,
        args=[user_id],
        **interval,
        start_date=datetime.now(timezone.utc).replace(hour=hour, minute=minute, second=0),
        replace_existing=True,
    )
    logger.info("sync_job_registered", user_id=user_id, frequency=frequency, sync_time=sync_time)


async def remove_user_sync(user_id: str):
    """Remove a user's scheduled sync job."""
    scheduler = _get_scheduler()
    job_id = f"cloud_sync_{user_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        logger.info("sync_job_removed", user_id=user_id)


async def start_scheduler():
    """Load all Pro/Enterprise users' schedules and start the scheduler."""
    try:
        from database.postgres import async_session_factory
        from sqlalchemy import text

        scheduler = _get_scheduler()

        if async_session_factory is None:
            logger.warning("scheduler_skipped_no_db", reason="DB not initialized yet")
            return

        async with async_session_factory() as session:
            result = await session.execute(
                text("""
                    SELECT id, preferences
                    FROM users
                    WHERE tier IN ('premium', 'pro', 'enterprise')
                      AND deleted_at IS NULL
                """)
            )
            rows = result.mappings().all()

        for row in rows:
            prefs = row.get("preferences") or {}
            freq = prefs.get("sync_frequency", "daily")
            sync_time = prefs.get("sync_time", "02:00")
            await schedule_user_sync(str(row["id"]), freq, sync_time)

        if not scheduler.running:
            scheduler.start()
        logger.info("sync_scheduler_started", users_loaded=len(rows))

    except Exception as e:
        logger.error("sync_scheduler_start_failed", error=str(e))


async def stop_scheduler():
    """Gracefully shut down the scheduler."""
    scheduler = _get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("sync_scheduler_stopped")
