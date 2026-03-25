"""
Celery Beat configuration for periodic tasks.

Schedules:
- Batch embedding generation
- Duplicate scanning
- Graph synchronization
"""

from celery.schedules import crontab
from config.celery_config import celery_app


# Configure Celery Beat periodic tasks
celery_app.conf.beat_schedule = {
    # Generate embeddings for entities every hour
    'batch-embeddings-hourly': {
        'task': 'batch_generate_embeddings',
        'schedule': crontab(minute=0),  # Every hour at :00
        'args': (100,)  # Process 100 entities at a time
    },
    
    # Scan for duplicates every 6 hours
    'scan-duplicates-6h': {
        'task': 'scan_all_entities_for_duplicates',
        'schedule': crontab(hour='*/6', minute=30),  # Every 6 hours at :30
    },
    
    # Sync to Neo4j every 30 minutes (if configured)
    'neo4j-sync-30min': {
        'task': 'sync_all_entities_to_neo4j',
        'schedule': crontab(minute='*/30'),  # Every 30 minutes
    },

    # Scheduled report delivery — runs daily at 9:00 AM
    'scheduled-reports-daily': {
        'task': 'process_scheduled_reports',
        'schedule': crontab(hour=9, minute=0),  # Daily at 09:00 UTC
    },
}

# Timezone for scheduled tasks
celery_app.conf.timezone = 'UTC'
