"""
Privacy Settings Service

Consolidates privacy controls into a standalone service:
- Privacy profile management (get/update settings)
- Data retention enforcement (purge policy)
- Sensitive data detection (PII: credit cards, SSNs, API keys, emails)
- GDPR-compliant data export
- Privacy audit log

Standalone service — no database dependencies for core logic.
"""

from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
from enum import Enum
import re
import json
import uuid
import structlog

logger = structlog.get_logger()


# ============================================================================
# CONSTANTS & PATTERNS
# ============================================================================

class EncryptionLevel(str, Enum):
    NONE = 'none'
    LOCAL = 'local'
    E2E = 'e2e'


class RetentionPolicy(str, Enum):
    DAYS_30 = '30_days'
    DAYS_90 = '90_days'
    DAYS_180 = '180_days'
    DAYS_365 = '365_days'
    FOREVER = 'forever'


RETENTION_DAYS = {
    RetentionPolicy.DAYS_30: 30,
    RetentionPolicy.DAYS_90: 90,
    RetentionPolicy.DAYS_180: 180,
    RetentionPolicy.DAYS_365: 365,
    RetentionPolicy.FOREVER: None,
}

# PII detection patterns
PII_PATTERNS = {
    'credit_card': [
        re.compile(r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b'),
    ],
    'ssn': [
        re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
        re.compile(r'\b\d{9}\b'),  # SSN without dashes
    ],
    'api_key': [
        re.compile(r'\b(?:sk|pk|api|key|token|secret|password)[_-]?[a-zA-Z0-9]{16,}\b', re.IGNORECASE),
        re.compile(r'\b[A-Za-z0-9]{32,}\b'),  # Long alphanumeric strings
    ],
    'email': [
        re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
    ],
    'phone': [
        re.compile(r'\b(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'),
    ],
}

DEFAULT_PRIVACY_SETTINGS = {
    'data_filtering': {
        'filter_credit_cards': True,
        'filter_ssn': True,
        'filter_api_keys': True,
        'filter_emails': False,
        'filter_phone_numbers': False,
        'custom_patterns': [],
    },
    'encryption': {
        'level': EncryptionLevel.LOCAL.value,
        'https_only': True,
    },
    'retention': {
        'policy': RetentionPolicy.DAYS_90.value,
        'auto_delete': True,
        'retention_days': 90,
    },
    'data_collection': {
        'track_window_titles': True,
        'track_urls': True,
        'track_file_paths': True,
        'track_screenshots': False,
        'anonymize_collaborators': False,
    },
}


# ============================================================================
# SERVICE
# ============================================================================

class PrivacySettingsService:
    """
    Manages privacy settings, PII detection, data retention, and audit logging.
    Uses in-memory storage — can be backed by a database later.
    """

    def __init__(self):
        self._settings: Dict[str, Dict] = {}
        self._audit_log: Dict[str, List[Dict]] = {}
        self._export_history: Dict[str, List[Dict]] = {}

    # ========================================================================
    # PRIVACY PROFILE MANAGEMENT
    # ========================================================================

    def get_settings(self, user_id: str) -> Dict[str, Any]:
        """Get user privacy settings, creating defaults if needed."""
        if user_id not in self._settings:
            self._settings[user_id] = _deep_copy(DEFAULT_PRIVACY_SETTINGS)
        return self._settings[user_id]

    def update_settings(self, user_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update privacy settings. Merges with existing."""
        settings = self.get_settings(user_id)
        old_settings = _deep_copy(settings)

        if 'data_filtering' in updates:
            settings['data_filtering'].update(updates['data_filtering'])
        if 'encryption' in updates:
            settings['encryption'].update(updates['encryption'])
        if 'retention' in updates:
            settings['retention'].update(updates['retention'])
            # Sync retention_days
            policy = settings['retention'].get('policy', RetentionPolicy.DAYS_90.value)
            try:
                days = RETENTION_DAYS.get(RetentionPolicy(policy))
                settings['retention']['retention_days'] = days
            except ValueError:
                pass
        if 'data_collection' in updates:
            settings['data_collection'].update(updates['data_collection'])

        self._settings[user_id] = settings

        # Audit the change
        self._log_audit(user_id, 'settings_updated', {
            'changes': _diff_settings(old_settings, settings),
        })

        logger.info('privacy_settings_updated', user_id=user_id)
        return settings

    def reset_to_defaults(self, user_id: str) -> Dict[str, Any]:
        """Reset privacy settings to defaults."""
        self._settings[user_id] = _deep_copy(DEFAULT_PRIVACY_SETTINGS)
        self._log_audit(user_id, 'settings_reset', {'reason': 'user_initiated'})
        return self._settings[user_id]

    # ========================================================================
    # DATA RETENTION ENFORCEMENT
    # ========================================================================

    def get_retention_policy(self, user_id: str) -> Dict[str, Any]:
        """Get the current retention policy for a user."""
        settings = self.get_settings(user_id)
        ret = settings['retention']
        return {
            'policy': ret['policy'],
            'retention_days': ret.get('retention_days'),
            'auto_delete': ret.get('auto_delete', True),
        }

    def compute_purge_cutoff(self, user_id: str) -> Optional[datetime]:
        """
        Compute the cutoff date for data purging based on retention policy.
        Returns None if retention is 'forever'.
        """
        policy = self.get_retention_policy(user_id)
        days = policy.get('retention_days')

        if days is None:
            return None

        return datetime.now() - timedelta(days=days)

    def identify_purgeable_data(
        self,
        user_id: str,
        data_items: List[Dict[str, Any]],
        date_field: str = 'created_at',
    ) -> Dict[str, Any]:
        """
        Identify which data items should be purged based on retention policy.
        Returns items to keep and items to purge.
        """
        cutoff = self.compute_purge_cutoff(user_id)
        if cutoff is None:
            return {
                'to_purge': [],
                'to_keep': data_items,
                'purge_count': 0,
                'keep_count': len(data_items),
            }

        to_purge = []
        to_keep = []

        for item in data_items:
            item_date = item.get(date_field)
            if item_date:
                if isinstance(item_date, str):
                    try:
                        item_dt = datetime.fromisoformat(item_date)
                    except ValueError:
                        to_keep.append(item)
                        continue
                else:
                    item_dt = item_date

                if item_dt < cutoff:
                    to_purge.append(item)
                else:
                    to_keep.append(item)
            else:
                to_keep.append(item)

        return {
            'to_purge': to_purge,
            'to_keep': to_keep,
            'purge_count': len(to_purge),
            'keep_count': len(to_keep),
            'cutoff_date': cutoff.isoformat(),
        }

    # ========================================================================
    # SENSITIVE DATA DETECTION
    # ========================================================================

    def detect_pii(
        self,
        text: str,
        categories: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Detect PII in text. Returns found categories and match details.
        """
        if not text:
            return {'has_pii': False, 'findings': [], 'categories_found': []}

        check_categories = categories or list(PII_PATTERNS.keys())
        findings = []
        categories_found: Set[str] = set()

        for category in check_categories:
            patterns = PII_PATTERNS.get(category, [])
            for pattern in patterns:
                matches = pattern.findall(text)
                if matches:
                    categories_found.add(category)
                    findings.append({
                        'category': category,
                        'count': len(matches),
                        'severity': 'high' if category in ('credit_card', 'ssn') else 'medium',
                    })
                    break  # One match per category is enough

        return {
            'has_pii': len(findings) > 0,
            'findings': findings,
            'categories_found': sorted(categories_found),
        }

    def filter_pii(
        self,
        user_id: str,
        text: str,
    ) -> Dict[str, Any]:
        """
        Filter PII from text based on user's privacy settings.
        Returns sanitized text and details of what was filtered.
        """
        settings = self.get_settings(user_id)
        filtering = settings['data_filtering']
        sanitized = text
        filtered_categories = []

        category_map = {
            'credit_card': filtering.get('filter_credit_cards', True),
            'ssn': filtering.get('filter_ssn', True),
            'api_key': filtering.get('filter_api_keys', True),
            'email': filtering.get('filter_emails', False),
            'phone': filtering.get('filter_phone_numbers', False),
        }

        for category, should_filter in category_map.items():
            if not should_filter:
                continue
            patterns = PII_PATTERNS.get(category, [])
            for pattern in patterns:
                if pattern.search(sanitized):
                    filtered_categories.append(category)
                    mask = f'[{category.upper()}_REDACTED]'
                    sanitized = pattern.sub(mask, sanitized)

        return {
            'original_length': len(text),
            'sanitized_text': sanitized,
            'sanitized_length': len(sanitized),
            'filtered_categories': filtered_categories,
            'was_modified': sanitized != text,
        }

    # ========================================================================
    # GDPR DATA EXPORT
    # ========================================================================

    def generate_data_export(
        self,
        user_id: str,
        data_sections: Dict[str, Any],
        format: str = 'json',
    ) -> Dict[str, Any]:
        """
        Generate a GDPR-compliant data export package.
        """
        export_id = str(uuid.uuid4())
        now = datetime.now()

        export = {
            'export_id': export_id,
            'user_id': user_id,
            'generated_at': now.isoformat(),
            'format': format,
            'gdpr_compliant': True,
            'includes': list(data_sections.keys()),
            'data': data_sections,
            'metadata': {
                'total_sections': len(data_sections),
                'privacy_settings': self.get_settings(user_id),
            },
        }

        # Track export
        if user_id not in self._export_history:
            self._export_history[user_id] = []
        self._export_history[user_id].append({
            'export_id': export_id,
            'generated_at': now.isoformat(),
            'format': format,
            'sections': list(data_sections.keys()),
        })

        self._log_audit(user_id, 'data_export', {
            'export_id': export_id,
            'sections': list(data_sections.keys()),
        })

        if format == 'json':
            export['content'] = json.dumps(export['data'], indent=2, default=str)
        else:
            export['content'] = json.dumps(export['data'], default=str)

        return export

    def get_export_history(self, user_id: str) -> List[Dict[str, Any]]:
        """Get history of data exports for a user."""
        return self._export_history.get(user_id, [])

    # ========================================================================
    # PRIVACY AUDIT LOG
    # ========================================================================

    def _log_audit(
        self,
        user_id: str,
        action: str,
        details: Optional[Dict] = None,
    ):
        """Record a privacy-related action in the audit log."""
        if user_id not in self._audit_log:
            self._audit_log[user_id] = []

        entry = {
            'id': str(uuid.uuid4()),
            'action': action,
            'timestamp': datetime.now().isoformat(),
            'details': details or {},
        }

        self._audit_log[user_id].append(entry)

    def get_audit_log(
        self,
        user_id: str,
        action: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get privacy audit log entries."""
        log = self._audit_log.get(user_id, [])

        if action:
            log = [e for e in log if e['action'] == action]

        return sorted(log, key=lambda x: x['timestamp'], reverse=True)[:limit]

    def get_audit_summary(self, user_id: str) -> Dict[str, Any]:
        """Get summary of audit log."""
        log = self._audit_log.get(user_id, [])

        action_counts = {}
        for entry in log:
            a = entry['action']
            action_counts[a] = action_counts.get(a, 0) + 1

        return {
            'total_entries': len(log),
            'action_counts': action_counts,
            'last_action': log[-1] if log else None,
        }


# ============================================================================
# HELPERS
# ============================================================================

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


def _diff_settings(old: Dict, new: Dict, prefix: str = '') -> List[Dict]:
    """Compute differences between two settings dicts."""
    diffs = []
    for key in set(list(old.keys()) + list(new.keys())):
        old_val = old.get(key)
        new_val = new.get(key)
        path = f'{prefix}.{key}' if prefix else key

        if isinstance(old_val, dict) and isinstance(new_val, dict):
            diffs.extend(_diff_settings(old_val, new_val, path))
        elif old_val != new_val:
            diffs.append({'path': path, 'old': old_val, 'new': new_val})

    return diffs
