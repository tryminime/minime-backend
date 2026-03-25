"""
Celery configuration for MiniMe Analytics.

Configures Celery Beat for scheduled analytics tasks:
- Daily metrics computation: 08:30 UTC
- Daily summary generation: 08:45 UTC  
- Daily email delivery: 09:00 UTC
- Weekly reports: Mondays 09:00 UTC
"""

from celery import Celery
from celery.schedules import crontab
from config.settings import settings

# Initialize Celery app
app = Celery('minime')

# Configuration
app.conf.broker_url = settings.REDIS_URL or 'redis://localhost:6379/0'
app.conf.result_backend = settings.REDIS_URL or 'redis://localhost:6379/0'

# Task settings
app.conf.task_serializer = 'json'
app.conf.result_serializer = 'json'
app.conf.accept_content = ['json']
app.conf.timezone = 'UTC'
app.conf.enable_utc = True

# Task routing
app.conf.task_routes = {
    'analytics.*': {'queue': 'analytics'},
    'default': {'queue': 'default'},
}

# Performance tuning
app.conf.worker_prefetch_multiplier = 1
app.conf.worker_max_tasks_per_child = 1000

# Beat Schedule - Scheduled Tasks
app.conf.beat_schedule = {
    # Daily Metrics - Every day at 08:30 UTC
    'compute-daily-metrics': {
       'task': 'analytics.schedule_daily_metrics',
        'schedule': crontab(hour=8, minute=30),
        'options': {'queue': 'analytics'}
    },
    
    # Daily Summaries - Every day at 08:45 UTC
    'generate-daily-summaries': {
        'task': 'analytics.schedule_daily_summaries',
        'schedule': crontab(hour=8, minute=45),
        'options': {'queue': 'analytics'}
    },
    
    # Daily Emails - Every day at 09:00 UTC
    'send-daily-emails': {
        'task': 'analytics.schedule_daily_emails',
        'schedule': crontab(hour=9, minute=0),
        'options': {'queue': 'analytics'}
    },
    
    # Weekly Reports - Every Monday at 09:00 UTC
    'generate-weekly-reports': {
        'task': 'analytics.schedule_weekly_reports',
        'schedule': crontab(day_of_week=1, hour=9, minute=0),
        'options': {'queue': 'analytics'}
    },
    
    # Weekly Emails - Every Monday at 09:30 UTC
    'send-weekly-emails': {
        'task': 'analytics.schedule_weekly_emails',
        'schedule': crontab(day_of_week=1, hour=9, minute=30),
        'options': {'queue': 'analytics'}
    },
}

# Auto-discover tasks
app.autodiscover_tasks(['backend.tasks'])

if __name__ == '__main__':
    app.start()
