"""
Wearable Data Sync — Celery Periodic Task.

Runs every 15 minutes to sync wearable data from all connected providers.
"""

from tasks.celery_app import celery_app
import structlog

logger = structlog.get_logger()


@celery_app.task(name="sync_wearable_data", bind=True, max_retries=3, default_retry_delay=60)
def sync_wearable_data(self):
    """
    Periodic task: sync all active wearable integrations.

    For each user with an active wearable connection:
    1. Check if token is still valid (refresh if needed)
    2. Pull latest data from provider API
    3. Store normalized metrics in wearable_data table
    """
    import asyncio
    asyncio.run(_sync_all_wearables())


async def _sync_all_wearables():
    """Async implementation of the sync task."""
    from sqlalchemy import text
    from database.postgres import async_session_factory
    from services.wearable_service import WearableService

    try:
        async with async_session_factory() as db:
            # Get all active integrations
            result = await db.execute(
                text("""
                    SELECT user_id, provider, access_token, refresh_token, token_expires_at
                    FROM wearable_integrations
                    WHERE is_active = true
                      AND access_token != 'mobile-sync'
                """)
            )

            integrations = result.fetchall()
            logger.info("wearable_sync_start", active_integrations=len(integrations))

            service = WearableService(db)
            synced_count = 0

            for row in integrations:
                user_id = str(row[0])
                provider = row[1]
                access_token = row[2]

                try:
                    if provider == "fitbit":
                        sync_result = await service.sync_fitbit(access_token, user_id)
                    elif provider == "oura":
                        sync_result = await service.sync_oura(access_token, user_id)
                    else:
                        continue  # apple_health is mobile-push only

                    data_points = sync_result.get("data_points", [])
                    if data_points:
                        stored = await service.store_data_points(user_id, provider, data_points)
                        synced_count += stored
                        logger.info("wearable_user_synced", user_id=user_id, provider=provider, points=stored)

                except Exception as e:
                    logger.error("wearable_sync_user_error", user_id=user_id, provider=provider, error=str(e))

            logger.info("wearable_sync_complete", total_synced=synced_count)

    except Exception as e:
        logger.error("wearable_sync_failed", error=str(e))
        raise


# Register periodic schedule (every 15 minutes)
celery_app.conf.beat_schedule = {
    **getattr(celery_app.conf, 'beat_schedule', {}),
    'sync-wearable-data': {
        'task': 'sync_wearable_data',
        'schedule': 900.0,  # 15 minutes in seconds
    },
}
