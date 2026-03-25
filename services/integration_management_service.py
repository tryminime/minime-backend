"""
Integration Management Service

Manages third-party integrations: registry, connection lifecycle,
sync scheduling, data flow controls, and health monitoring.

Standalone service — no database dependencies for core logic.
The existing integrations.py API handles OAuth flows; this service
manages the higher-level integration lifecycle.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import uuid
import structlog

logger = structlog.get_logger()


# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

class IntegrationStatus(str, Enum):
    AVAILABLE = 'available'
    CONNECTED = 'connected'
    DISCONNECTED = 'disconnected'
    ERROR = 'error'
    EXPIRED = 'expired'
    SYNCING = 'syncing'


class SyncFrequency(str, Enum):
    REALTIME = 'realtime'
    HOURLY = 'hourly'
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MANUAL = 'manual'


# Integration registry — available integrations and their capabilities
INTEGRATION_REGISTRY = {
    'github': {
        'name': 'GitHub',
        'description': 'Track commits, pull requests, code reviews, and repositories',
        'icon': 'github',
        'capabilities': ['commits', 'pull_requests', 'code_reviews', 'repositories', 'issues'],
        'required_scopes': ['repo', 'read:user'],
        'auth_type': 'oauth2',
        'default_sync_frequency': SyncFrequency.HOURLY.value,
    },
    'google_calendar': {
        'name': 'Google Calendar',
        'description': 'Import calendar events, meetings, and scheduling data',
        'icon': 'calendar',
        'capabilities': ['events', 'meetings', 'scheduling', 'attendees'],
        'required_scopes': ['calendar.readonly', 'calendar.events.readonly'],
        'auth_type': 'oauth2',
        'default_sync_frequency': SyncFrequency.HOURLY.value,
    },
    'notion': {
        'name': 'Notion',
        'description': 'Sync pages, databases, and workspace activity',
        'icon': 'file-text',
        'capabilities': ['pages', 'databases', 'blocks', 'comments'],
        'required_scopes': ['read_content'],
        'auth_type': 'oauth2',
        'default_sync_frequency': SyncFrequency.DAILY.value,
    },
    'slack': {
        'name': 'Slack',
        'description': 'Track messages, channels, and communication patterns',
        'icon': 'message-square',
        'capabilities': ['messages', 'channels', 'reactions', 'threads'],
        'required_scopes': ['channels:history', 'users:read'],
        'auth_type': 'oauth2',
        'default_sync_frequency': SyncFrequency.HOURLY.value,
    },
    'jira': {
        'name': 'Jira',
        'description': 'Track tickets, sprints, and project progress',
        'icon': 'clipboard',
        'capabilities': ['issues', 'sprints', 'boards', 'worklogs'],
        'required_scopes': ['read:jira-work'],
        'auth_type': 'oauth2',
        'default_sync_frequency': SyncFrequency.HOURLY.value,
    },
}


# ============================================================================
# SERVICE
# ============================================================================

class IntegrationManagementService:
    """
    Manages integration lifecycle, sync state, and health monitoring.
    Uses in-memory storage — can be backed by a database later.
    """

    def __init__(self):
        self._connections: Dict[str, Dict[str, Dict]] = defaultdict(dict)
        self._sync_history: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))

    # ========================================================================
    # INTEGRATION REGISTRY
    # ========================================================================

    def list_available(self) -> List[Dict[str, Any]]:
        """List all available integrations with metadata."""
        return [
            {
                'provider': provider,
                **info,
                'status': IntegrationStatus.AVAILABLE.value,
            }
            for provider, info in INTEGRATION_REGISTRY.items()
        ]

    def get_integration_info(self, provider: str) -> Optional[Dict[str, Any]]:
        """Get detailed info about a specific integration."""
        if provider not in INTEGRATION_REGISTRY:
            return None
        return {
            'provider': provider,
            **INTEGRATION_REGISTRY[provider],
        }

    # ========================================================================
    # CONNECTION MANAGEMENT
    # ========================================================================

    def connect(
        self,
        user_id: str,
        provider: str,
        access_token: Optional[str] = None,
        username: Optional[str] = None,
        email: Optional[str] = None,
        token_expires_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Register a new integration connection."""
        if provider not in INTEGRATION_REGISTRY:
            raise ValueError(f'Unknown integration provider: {provider}')

        info = INTEGRATION_REGISTRY[provider]
        connection = {
            'id': str(uuid.uuid4()),
            'provider': provider,
            'status': IntegrationStatus.CONNECTED.value,
            'username': username,
            'email': email,
            'connected_at': datetime.now().isoformat(),
            'token_expires_at': token_expires_at,
            'sync_frequency': info['default_sync_frequency'],
            'last_sync': None,
            'sync_errors': 0,
            'data_permissions': {cap: True for cap in info['capabilities']},
            'enabled': True,
        }

        self._connections[user_id][provider] = connection
        logger.info('integration_connected', user_id=user_id, provider=provider)
        return connection

    def disconnect(self, user_id: str, provider: str) -> bool:
        """Disconnect an integration."""
        if provider in self._connections.get(user_id, {}):
            self._connections[user_id][provider]['status'] = IntegrationStatus.DISCONNECTED.value
            self._connections[user_id][provider]['disconnected_at'] = datetime.now().isoformat()
            logger.info('integration_disconnected', user_id=user_id, provider=provider)
            return True
        return False

    def reconnect(self, user_id: str, provider: str) -> Dict[str, Any]:
        """Reconnect a previously disconnected integration."""
        conn = self._connections.get(user_id, {}).get(provider)
        if not conn:
            raise ValueError(f'No existing connection for {provider}')

        conn['status'] = IntegrationStatus.CONNECTED.value
        conn['reconnected_at'] = datetime.now().isoformat()
        conn['sync_errors'] = 0
        logger.info('integration_reconnected', user_id=user_id, provider=provider)
        return conn

    def get_connection(self, user_id: str, provider: str) -> Optional[Dict[str, Any]]:
        """Get connection details for a specific integration."""
        return self._connections.get(user_id, {}).get(provider)

    def get_all_connections(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all connections for a user."""
        connections = []
        for provider, info in INTEGRATION_REGISTRY.items():
            conn = self._connections.get(user_id, {}).get(provider)
            if conn:
                connections.append(conn)
            else:
                connections.append({
                    'provider': provider,
                    'status': IntegrationStatus.AVAILABLE.value,
                    'name': info['name'],
                })
        return connections

    # ========================================================================
    # SYNC MANAGEMENT
    # ========================================================================

    def set_sync_frequency(self, user_id: str, provider: str, frequency: str) -> Dict[str, Any]:
        """Set sync frequency for an integration."""
        conn = self._connections.get(user_id, {}).get(provider)
        if not conn:
            raise ValueError(f'No connection for {provider}')
        conn['sync_frequency'] = frequency
        return conn

    def record_sync(
        self,
        user_id: str,
        provider: str,
        success: bool,
        items_synced: int = 0,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record a sync event."""
        conn = self._connections.get(user_id, {}).get(provider)
        if not conn:
            raise ValueError(f'No connection for {provider}')

        sync_record = {
            'id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'items_synced': items_synced,
            'error_message': error_message,
        }

        self._sync_history[user_id][provider].append(sync_record)

        if success:
            conn['last_sync'] = sync_record['timestamp']
            conn['sync_errors'] = 0
            conn['status'] = IntegrationStatus.CONNECTED.value
        else:
            conn['sync_errors'] = conn.get('sync_errors', 0) + 1
            if conn['sync_errors'] >= 3:
                conn['status'] = IntegrationStatus.ERROR.value

        return sync_record

    def get_sync_history(
        self,
        user_id: str,
        provider: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Get sync history for an integration."""
        history = self._sync_history.get(user_id, {}).get(provider, [])
        return sorted(history, key=lambda x: x['timestamp'], reverse=True)[:limit]

    def needs_sync(self, user_id: str, provider: str) -> bool:
        """Check if an integration needs syncing based on frequency."""
        conn = self._connections.get(user_id, {}).get(provider)
        if not conn or conn['status'] != IntegrationStatus.CONNECTED.value:
            return False
        if not conn.get('enabled'):
            return False

        freq = conn.get('sync_frequency', SyncFrequency.DAILY.value)
        if freq == SyncFrequency.MANUAL.value:
            return False

        last_sync = conn.get('last_sync')
        if not last_sync:
            return True

        last_dt = datetime.fromisoformat(last_sync)
        now = datetime.now()

        intervals = {
            SyncFrequency.REALTIME.value: timedelta(minutes=5),
            SyncFrequency.HOURLY.value: timedelta(hours=1),
            SyncFrequency.DAILY.value: timedelta(days=1),
            SyncFrequency.WEEKLY.value: timedelta(weeks=1),
        }

        interval = intervals.get(freq, timedelta(days=1))
        return (now - last_dt) >= interval

    # ========================================================================
    # DATA FLOW CONTROLS
    # ========================================================================

    def set_data_permissions(
        self,
        user_id: str,
        provider: str,
        permissions: Dict[str, bool],
    ) -> Dict[str, bool]:
        """Set per-capability data permissions for an integration."""
        conn = self._connections.get(user_id, {}).get(provider)
        if not conn:
            raise ValueError(f'No connection for {provider}')
        conn['data_permissions'].update(permissions)
        return conn['data_permissions']

    def get_data_permissions(self, user_id: str, provider: str) -> Dict[str, bool]:
        """Get data permissions for an integration."""
        conn = self._connections.get(user_id, {}).get(provider)
        if not conn:
            return {}
        return conn.get('data_permissions', {})

    # ========================================================================
    # HEALTH MONITORING
    # ========================================================================

    def get_health(self, user_id: str, provider: str) -> Dict[str, Any]:
        """Get health status for a specific integration."""
        conn = self._connections.get(user_id, {}).get(provider)
        if not conn:
            return {'status': 'not_connected', 'healthy': False}

        # Check token expiry
        token_expired = False
        if conn.get('token_expires_at'):
            try:
                expires = datetime.fromisoformat(conn['token_expires_at'])
                token_expired = datetime.now() >= expires
            except (ValueError, TypeError):
                pass

        if token_expired:
            conn['status'] = IntegrationStatus.EXPIRED.value

        # Determine health
        healthy = (
            conn['status'] == IntegrationStatus.CONNECTED.value
            and not token_expired
            and conn.get('sync_errors', 0) < 3
        )

        # Recent sync info
        history = self._sync_history.get(user_id, {}).get(provider, [])
        recent_failures = sum(
            1 for h in history[-5:]
            if not h.get('success', True)
        )

        return {
            'status': conn['status'],
            'healthy': healthy,
            'token_expired': token_expired,
            'sync_errors': conn.get('sync_errors', 0),
            'recent_failures': recent_failures,
            'last_sync': conn.get('last_sync'),
            'needs_sync': self.needs_sync(user_id, provider),
            'recommendations': self._health_recommendations(conn, token_expired, recent_failures),
        }

    def get_all_health(self, user_id: str) -> List[Dict[str, Any]]:
        """Get health status for all connected integrations."""
        results = []
        for provider in self._connections.get(user_id, {}):
            health = self.get_health(user_id, provider)
            health['provider'] = provider
            results.append(health)
        return results

    def _health_recommendations(
        self,
        conn: Dict,
        token_expired: bool,
        recent_failures: int,
    ) -> List[str]:
        """Generate health recommendations."""
        recs = []
        if token_expired:
            recs.append('Token expired — please reconnect this integration.')
        if recent_failures >= 3:
            recs.append('Multiple recent sync failures. Check your connection.')
        if conn.get('sync_errors', 0) >= 3:
            recs.append('Integration is in error state. Try disconnecting and reconnecting.')
        if not conn.get('enabled'):
            recs.append('This integration is disabled. Enable it to resume syncing.')
        return recs
