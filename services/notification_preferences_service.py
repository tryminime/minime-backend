"""
Notification Preferences Service

Manages user notification preferences, Do-Not-Disturb scheduling,
notification dispatching logic, digest generation, and notification history.

Standalone service — no database dependencies for core logic.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, time
from enum import Enum
from collections import defaultdict
import uuid
import structlog

logger = structlog.get_logger()


# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

class NotificationChannel(str, Enum):
    IN_APP = 'in_app'
    EMAIL = 'email'
    BROWSER_PUSH = 'browser_push'
    DESKTOP = 'desktop'


class NotificationType(str, Enum):
    DAILY_SUMMARY = 'daily_summary'
    WEEKLY_DIGEST = 'weekly_digest'
    DEADLINE_REMINDER = 'deadline_reminder'
    FOCUS_REMINDER = 'focus_reminder'
    BREAK_SUGGESTION = 'break_suggestion'
    WELLNESS_ALERT = 'wellness_alert'
    AI_INSIGHT = 'ai_insight'
    SYNC_ERROR = 'sync_error'
    GOAL_PROGRESS = 'goal_progress'
    ACHIEVEMENT = 'achievement'
    SYSTEM = 'system'


class NotificationPriority(str, Enum):
    LOW = 'low'
    MEDIUM = 'medium'
    HIGH = 'high'
    CRITICAL = 'critical'


# Priority mappings for notification types
NOTIFICATION_PRIORITY = {
    NotificationType.DAILY_SUMMARY: NotificationPriority.LOW,
    NotificationType.WEEKLY_DIGEST: NotificationPriority.LOW,
    NotificationType.DEADLINE_REMINDER: NotificationPriority.HIGH,
    NotificationType.FOCUS_REMINDER: NotificationPriority.MEDIUM,
    NotificationType.BREAK_SUGGESTION: NotificationPriority.MEDIUM,
    NotificationType.WELLNESS_ALERT: NotificationPriority.HIGH,
    NotificationType.AI_INSIGHT: NotificationPriority.LOW,
    NotificationType.SYNC_ERROR: NotificationPriority.HIGH,
    NotificationType.GOAL_PROGRESS: NotificationPriority.MEDIUM,
    NotificationType.ACHIEVEMENT: NotificationPriority.MEDIUM,
    NotificationType.SYSTEM: NotificationPriority.CRITICAL,
}

# Default channel settings
DEFAULT_PREFERENCES = {
    'channels': {
        NotificationChannel.IN_APP.value: True,
        NotificationChannel.EMAIL.value: False,
        NotificationChannel.BROWSER_PUSH.value: False,
        NotificationChannel.DESKTOP.value: True,
    },
    'types': {
        NotificationType.DAILY_SUMMARY.value: True,
        NotificationType.WEEKLY_DIGEST.value: True,
        NotificationType.DEADLINE_REMINDER.value: True,
        NotificationType.FOCUS_REMINDER.value: True,
        NotificationType.BREAK_SUGGESTION.value: True,
        NotificationType.WELLNESS_ALERT.value: True,
        NotificationType.AI_INSIGHT.value: True,
        NotificationType.SYNC_ERROR.value: True,
        NotificationType.GOAL_PROGRESS.value: True,
        NotificationType.ACHIEVEMENT.value: True,
        NotificationType.SYSTEM.value: True,
    },
    'dnd': {
        'enabled': False,
        'start_hour': 22,
        'end_hour': 8,
        'override_critical': True,
    },
    'frequency_caps': {
        'max_per_hour': 10,
        'max_per_day': 50,
        'digest_mode': False,
    },
}


# ============================================================================
# SERVICE
# ============================================================================

class NotificationPreferencesService:
    """
    Manages notification preferences, DND, scheduling, and history.
    Uses in-memory storage — can be backed by a database later.
    """

    def __init__(self):
        self._preferences: Dict[str, Dict] = {}
        self._notifications: Dict[str, List[Dict]] = defaultdict(list)
        self._send_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {'hour': 0, 'day': 0, 'last_hour': '', 'last_day': ''})

    # ========================================================================
    # PREFERENCES CRUD
    # ========================================================================

    def get_preferences(self, user_id: str) -> Dict[str, Any]:
        """Get user notification preferences, creating defaults if needed."""
        if user_id not in self._preferences:
            self._preferences[user_id] = _deep_copy(DEFAULT_PREFERENCES)
        return self._preferences[user_id]

    def update_preferences(self, user_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update notification preferences. Merges with existing."""
        prefs = self.get_preferences(user_id)

        if 'channels' in updates:
            prefs['channels'].update(updates['channels'])
        if 'types' in updates:
            prefs['types'].update(updates['types'])
        if 'dnd' in updates:
            prefs['dnd'].update(updates['dnd'])
        if 'frequency_caps' in updates:
            prefs['frequency_caps'].update(updates['frequency_caps'])

        self._preferences[user_id] = prefs
        logger.info('notification_preferences_updated', user_id=user_id)
        return prefs

    def set_channel(self, user_id: str, channel: str, enabled: bool) -> Dict[str, Any]:
        """Enable/disable a specific notification channel."""
        prefs = self.get_preferences(user_id)
        prefs['channels'][channel] = enabled
        return prefs

    def set_type(self, user_id: str, notification_type: str, enabled: bool) -> Dict[str, Any]:
        """Enable/disable a specific notification type."""
        prefs = self.get_preferences(user_id)
        prefs['types'][notification_type] = enabled
        return prefs

    def reset_to_defaults(self, user_id: str) -> Dict[str, Any]:
        """Reset preferences to defaults."""
        self._preferences[user_id] = _deep_copy(DEFAULT_PREFERENCES)
        return self._preferences[user_id]

    # ========================================================================
    # DO-NOT-DISTURB
    # ========================================================================

    def set_dnd(
        self,
        user_id: str,
        enabled: bool,
        start_hour: Optional[int] = None,
        end_hour: Optional[int] = None,
        override_critical: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Configure Do-Not-Disturb settings."""
        prefs = self.get_preferences(user_id)
        prefs['dnd']['enabled'] = enabled
        if start_hour is not None:
            prefs['dnd']['start_hour'] = max(0, min(23, start_hour))
        if end_hour is not None:
            prefs['dnd']['end_hour'] = max(0, min(23, end_hour))
        if override_critical is not None:
            prefs['dnd']['override_critical'] = override_critical
        return prefs['dnd']

    def is_dnd_active(self, user_id: str, check_time: Optional[datetime] = None) -> bool:
        """Check if DND is currently active for a user."""
        prefs = self.get_preferences(user_id)
        dnd = prefs['dnd']

        if not dnd['enabled']:
            return False

        now = check_time or datetime.now()
        current_hour = now.hour
        start = dnd['start_hour']
        end = dnd['end_hour']

        # Handle overnight DND (e.g., 22:00 to 08:00)
        if start > end:
            return current_hour >= start or current_hour < end
        else:
            return start <= current_hour < end

    # ========================================================================
    # NOTIFICATION SCHEDULING
    # ========================================================================

    def should_send(
        self,
        user_id: str,
        notification_type: str,
        channel: str,
        priority: Optional[str] = None,
        check_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Determine whether a notification should be sent based on preferences,
        DND state, and frequency caps.

        Returns dict with 'send' (bool) and 'reason' (str).
        """
        prefs = self.get_preferences(user_id)
        effective_priority = priority or NOTIFICATION_PRIORITY.get(
            notification_type, NotificationPriority.MEDIUM
        )
        if isinstance(effective_priority, NotificationPriority):
            effective_priority = effective_priority.value

        # Check channel enabled
        if not prefs['channels'].get(channel, False):
            return {'send': False, 'reason': f'channel_{channel}_disabled'}

        # Check type enabled
        if not prefs['types'].get(notification_type, True):
            return {'send': False, 'reason': f'type_{notification_type}_disabled'}

        # Check DND
        if self.is_dnd_active(user_id, check_time):
            dnd = prefs['dnd']
            if effective_priority == 'critical' and dnd.get('override_critical', True):
                pass  # Critical overrides DND
            else:
                return {'send': False, 'reason': 'dnd_active'}

        # Check frequency caps
        caps = prefs['frequency_caps']
        if caps.get('digest_mode') and effective_priority in ('low', 'medium'):
            return {'send': False, 'reason': 'digest_mode_active'}

        counts = self._get_send_counts(user_id, check_time)
        if counts['hour'] >= caps.get('max_per_hour', 10):
            return {'send': False, 'reason': 'hourly_cap_reached'}
        if counts['day'] >= caps.get('max_per_day', 50):
            return {'send': False, 'reason': 'daily_cap_reached'}

        return {'send': True, 'reason': 'approved'}

    def _get_send_counts(self, user_id: str, check_time: Optional[datetime] = None) -> Dict[str, int]:
        """Get current send counts, resetting if period has changed."""
        now = check_time or datetime.now()
        current_hour = now.strftime('%Y-%m-%d-%H')
        current_day = now.strftime('%Y-%m-%d')

        counts = self._send_counts[user_id]
        if counts['last_hour'] != current_hour:
            counts['hour'] = 0
            counts['last_hour'] = current_hour
        if counts['last_day'] != current_day:
            counts['day'] = 0
            counts['last_day'] = current_day

        return counts

    def record_send(self, user_id: str, check_time: Optional[datetime] = None):
        """Record that a notification was sent (for frequency cap tracking)."""
        counts = self._get_send_counts(user_id, check_time)
        counts['hour'] += 1
        counts['day'] += 1

    # ========================================================================
    # DIGEST GENERATION
    # ========================================================================

    def generate_digest(
        self,
        user_id: str,
        period: str = 'daily',
    ) -> Dict[str, Any]:
        """
        Generate a digest of notifications for the given period.
        Groups by type and priority.
        """
        notifications = self._notifications.get(user_id, [])
        now = datetime.now()

        if period == 'daily':
            cutoff = now - timedelta(days=1)
        elif period == 'weekly':
            cutoff = now - timedelta(weeks=1)
        else:
            cutoff = now - timedelta(days=1)

        # Filter to period
        period_notifications = [
            n for n in notifications
            if datetime.fromisoformat(n['created_at']) >= cutoff
        ]

        # Group by type
        by_type: Dict[str, List] = defaultdict(list)
        for n in period_notifications:
            by_type[n['type']].append(n)

        # Count by priority
        by_priority: Dict[str, int] = defaultdict(int)
        for n in period_notifications:
            by_priority[n.get('priority', 'medium')] += 1

        unread_count = sum(1 for n in period_notifications if not n.get('read', False))

        return {
            'period': period,
            'start': cutoff.isoformat(),
            'end': now.isoformat(),
            'total_count': len(period_notifications),
            'unread_count': unread_count,
            'by_type': {k: len(v) for k, v in by_type.items()},
            'by_priority': dict(by_priority),
            'items': period_notifications[:50],  # Cap at 50
        }

    # ========================================================================
    # NOTIFICATION HISTORY
    # ========================================================================

    def add_notification(
        self,
        user_id: str,
        notification_type: str,
        title: str,
        message: str,
        priority: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Add a notification to history."""
        effective_priority = priority or NOTIFICATION_PRIORITY.get(
            notification_type, NotificationPriority.MEDIUM
        )
        if isinstance(effective_priority, NotificationPriority):
            effective_priority = effective_priority.value

        notification = {
            'id': str(uuid.uuid4()),
            'type': notification_type,
            'title': title,
            'message': message,
            'priority': effective_priority,
            'read': False,
            'created_at': datetime.now().isoformat(),
            'metadata': metadata or {},
        }

        self._notifications[user_id].append(notification)
        logger.info('notification_added', user_id=user_id, type=notification_type)
        return notification

    def get_notifications(
        self,
        user_id: str,
        unread_only: bool = False,
        notification_type: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get notification history with optional filters."""
        notifications = self._notifications.get(user_id, [])

        if unread_only:
            notifications = [n for n in notifications if not n.get('read', False)]

        if notification_type:
            notifications = [n for n in notifications if n['type'] == notification_type]

        # Most recent first
        notifications = sorted(notifications, key=lambda x: x['created_at'], reverse=True)
        return notifications[:limit]

    def mark_read(self, user_id: str, notification_id: str) -> bool:
        """Mark a notification as read."""
        for n in self._notifications.get(user_id, []):
            if n['id'] == notification_id:
                n['read'] = True
                return True
        return False

    def mark_all_read(self, user_id: str) -> int:
        """Mark all notifications as read. Returns count marked."""
        count = 0
        for n in self._notifications.get(user_id, []):
            if not n.get('read', False):
                n['read'] = True
                count += 1
        return count

    def get_unread_count(self, user_id: str) -> int:
        """Get count of unread notifications."""
        return sum(
            1 for n in self._notifications.get(user_id, [])
            if not n.get('read', False)
        )

    def delete_notification(self, user_id: str, notification_id: str) -> bool:
        """Delete a specific notification."""
        notifications = self._notifications.get(user_id, [])
        for i, n in enumerate(notifications):
            if n['id'] == notification_id:
                notifications.pop(i)
                return True
        return False

    def clear_all(self, user_id: str) -> int:
        """Clear all notifications for a user. Returns count cleared."""
        count = len(self._notifications.get(user_id, []))
        self._notifications[user_id] = []
        return count


def _deep_copy(d: Dict) -> Dict:
    """Simple deep copy for nested dicts."""
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            result[k] = _deep_copy(v)
        elif isinstance(v, list):
            result[k] = v[:]
        else:
            result[k] = v
    return result
