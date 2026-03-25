# Analytics Models Package
# This package shadows backend/models.py so we re-export from there

# Import analytics models from this package
from models.analytics_models import DailyMetrics, DailySummary, WeeklyReport, AnalyticsEmail, UserGoal
from models.integration_models import Integration

# Re-export models from backend/models.py (the main ORM models file)
# Import as module to avoid recursion
import sys
import os

# Get path to models.py
models_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models.py')

# Load models.py directly
import importlib.util
spec = importlib.util.spec_from_file_location("_root_models", models_file)
_root_models = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_root_models)

# Re-export the ORM models
User = getattr(_root_models, 'User')
Entity = getattr(_root_models, 'Entity')
ActivityEntityLink = getattr(_root_models, 'ActivityEntityLink')
EntityOccurrence = getattr(_root_models, 'EntityOccurrence')  # Alias for ActivityEntityLink
Activity = getattr(_root_models, 'Activity')
Session = getattr(_root_models, 'Session')
AuditLog = getattr(_root_models, 'AuditLog')
ContentItem = getattr(_root_models, 'ContentItem')
SyncHistory = getattr(_root_models, 'SyncHistory')

__all__ = [
    # Analytics models (from package)
    "DailyMetrics",
    "DailySummary",
    "WeeklyReport",
    "AnalyticsEmail",
    "UserGoal",
    # Integration model
    "Integration",
    # ORM models (from models.py)
    "User",
    "Entity",
    "ActivityEntityLink",
    "EntityOccurrence",  # Alias for ActivityEntityLink
    "Activity",
    "Session",
    "AuditLog",
    "ContentItem",
    "SyncHistory",
]

