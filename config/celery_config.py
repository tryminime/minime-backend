"""
Celery configuration for MiniMe background tasks.

This module sets up the Celery application for asynchronous task processing:
- NER worker for entity extraction
- Activity processing pipelines
- Analytics computation
"""

from celery import Celery
from config import settings
import structlog

logger = structlog.get_logger()

# Initialize Celery app
celery_app = Celery(
    "minime_tasks",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=['backend.tasks.ner_worker']
)

# Configuration
celery_app.conf.update(
    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # Timezone
    timezone='UTC',
    enable_utc=True,
    
    # Task tracking
    task_track_started=True,
    task_send_sent_event=True,
    
    # Time limits
    task_time_limit=300,  # 5 minutes hard limit
    task_soft_time_limit=240,  # 4 minutes soft limit
    
    # Worker behavior
    worker_prefetch_multiplier=1,  # Only fetch one task at a time
    worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks
    worker_disable_rate_limits=False,
    
    # Results
    result_expires=3600,  # Results expire after 1 hour
    result_backend_transport_options={'master_name': 'mymaster'},
    
    # Broker settings
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
)

# Task routes (for future task organization)
celery_app.conf.task_routes = {
    'backend.tasks.ner_worker.*': {'queue': 'ner'},
    'backend.tasks.analytics.*': {'queue': 'analytics'},
}

logger.info("Celery application configured", broker=settings.REDIS_URL)
