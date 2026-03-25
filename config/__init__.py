"""
Backend configuration package.

Contains configuration modules for:
- Celery task queue
- Application settings
"""

from config.settings import settings

__all__ = ["settings"]
