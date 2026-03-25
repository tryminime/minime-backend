"""
Cross-Activity Entity Resolution Service.

Links the same entities across different platforms and activity types:
- Browser (URLs, domains)
- Desktop (window titles, app names)
- Meetings (attendees, platforms)
- Email (senders, recipients)

Resolution strategies:
1. Exact name match within time window
2. Alias matching across platforms
3. Fuzzy matching with configurable threshold
"""

from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import re
import structlog

logger = structlog.get_logger()


# ============================================================================
# PLATFORM CONSTANTS
# ============================================================================

PLATFORM_TYPES = {
    "browser": ["chrome", "firefox", "edge", "safari", "brave", "opera"],
    "desktop": ["vscode", "terminal", "finder", "explorer", "xcode", "intellij"],
    "meeting": ["zoom", "google_meet", "teams", "webex", "slack_huddle"],
    "email": ["gmail", "outlook", "thunderbird", "protonmail"],
    "chat": ["slack", "discord", "teams", "telegram"],
}


class CrossActivityResolver:
    """
    Service for resolving entities across different activity types and platforms.

    Identifies when the same entity (person, project, tool) appears across
    multiple platforms and links them to a single canonical identity.
    """

    def __init__(
        self,
        time_window_minutes: int = 60,
        fuzzy_threshold: float = 0.85
    ):
        """
        Initialize cross-activity resolver.

        Args:
            time_window_minutes: Time window for co-occurrence resolution
            fuzzy_threshold: Minimum similarity for fuzzy matching (0-1)
        """
        self.time_window = timedelta(minutes=time_window_minutes)
        self.fuzzy_threshold = fuzzy_threshold

    def resolve_entity(
        self,
        entity: Dict[str, Any],
        existing_entities: List[Dict[str, Any]],
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Resolve a single entity against existing entities.

        Args:
            entity: Entity to resolve (text, label, source, timestamp, platform)
            existing_entities: Pool of known entities to match against
            user_id: Optional user ID for multi-tenant resolution

        Returns:
            Dict with:
            - resolved: bool — whether a match was found
            - canonical_entity: Dict — matched entity or the original
            - matches: List[Dict] — all candidate matches with scores
            - resolution_method: str — method used (exact, alias, fuzzy, time_window)
            - cross_platform: bool — whether match spans multiple platforms
        """
        if not entity or not existing_entities:
            return {
                'resolved': False,
                'canonical_entity': entity,
                'matches': [],
                'resolution_method': 'none',
                'cross_platform': False,
            }

        entity_text = entity.get('text', '').strip()
        entity_label = entity.get('label', '')
        entity_platform = entity.get('platform', 'unknown')

        matches = []

        for existing in existing_entities:
            existing_text = existing.get('text', '').strip()
            existing_label = existing.get('label', '')
            existing_platform = existing.get('platform', 'unknown')

            # Skip same entity
            if entity.get('id') and entity.get('id') == existing.get('id'):
                continue

            # 1. Exact match (case-insensitive)
            if entity_text.lower() == existing_text.lower() and entity_label == existing_label:
                is_cross = entity_platform != existing_platform
                matches.append({
                    'entity': existing,
                    'score': 1.0,
                    'method': 'exact',
                    'cross_platform': is_cross,
                })
                continue

            # 2. Alias matching
            entity_aliases = self._generate_aliases(entity_text, entity_label)
            existing_aliases = self._generate_aliases(existing_text, existing_label)

            alias_overlap = entity_aliases & existing_aliases
            if alias_overlap and entity_label == existing_label:
                is_cross = entity_platform != existing_platform
                score = 0.90 if len(alias_overlap) > 1 else 0.85
                matches.append({
                    'entity': existing,
                    'score': score,
                    'method': 'alias',
                    'cross_platform': is_cross,
                    'shared_aliases': list(alias_overlap),
                })
                continue

            # 3. Time-window co-occurrence (same label, similar time)
            if entity_label == existing_label:
                time_match = self._check_time_proximity(entity, existing)
                if time_match and self._fuzzy_match(entity_text, existing_text) > 0.7:
                    is_cross = entity_platform != existing_platform
                    matches.append({
                        'entity': existing,
                        'score': 0.75,
                        'method': 'time_window',
                        'cross_platform': is_cross,
                    })

            # 4. Fuzzy matching (for PERSON and ORG entities)
            if entity_label in ('PERSON', 'ORG') and existing_label == entity_label:
                sim = self._fuzzy_match(entity_text, existing_text)
                if sim >= self.fuzzy_threshold:
                    is_cross = entity_platform != existing_platform
                    matches.append({
                        'entity': existing,
                        'score': round(sim, 3),
                        'method': 'fuzzy',
                        'cross_platform': is_cross,
                    })

        # Sort by score descending
        matches.sort(key=lambda m: m['score'], reverse=True)

        if matches:
            best = matches[0]
            return {
                'resolved': True,
                'canonical_entity': best['entity'],
                'matches': matches[:5],  # Top 5
                'resolution_method': best['method'],
                'cross_platform': best['cross_platform'],
            }

        return {
            'resolved': False,
            'canonical_entity': entity,
            'matches': [],
            'resolution_method': 'none',
            'cross_platform': False,
        }

    def batch_resolve(
        self,
        entities: List[Dict[str, Any]],
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Resolve a batch of entities, identifying clusters of the same entity.

        Args:
            entities: List of entities to resolve
            user_id: Optional user ID

        Returns:
            Dict with:
            - clusters: List[List[Dict]] — groups of resolved entities
            - resolved_count: int — number of entities that matched
            - merge_suggestions: List[Dict] — suggested merges
            - cross_platform_count: int — entities found across platforms
        """
        if not entities:
            return {
                'clusters': [],
                'resolved_count': 0,
                'merge_suggestions': [],
                'cross_platform_count': 0,
            }

        # Build clusters using union-find approach
        clusters: List[List[int]] = []
        assigned: Set[int] = set()

        for i, entity in enumerate(entities):
            if i in assigned:
                continue

            cluster = [i]
            assigned.add(i)

            for j in range(i + 1, len(entities)):
                if j in assigned:
                    continue

                result = self.resolve_entity(entity, [entities[j]])
                if result['resolved'] and result['matches'][0]['score'] >= 0.80:
                    cluster.append(j)
                    assigned.add(j)

            clusters.append(cluster)

        # Build output
        entity_clusters = []
        merge_suggestions = []
        cross_platform_count = 0
        resolved_count = 0

        for cluster_indices in clusters:
            if len(cluster_indices) > 1:
                cluster_entities = [entities[i] for i in cluster_indices]
                entity_clusters.append(cluster_entities)
                resolved_count += len(cluster_indices)

                # Check cross-platform
                platforms = set()
                for e in cluster_entities:
                    platforms.add(e.get('platform', 'unknown'))
                if len(platforms) > 1:
                    cross_platform_count += len(cluster_entities)

                # Create merge suggestion
                canonical = cluster_entities[0]  # First as canonical
                merge_suggestions.append({
                    'canonical': canonical,
                    'duplicates': cluster_entities[1:],
                    'platforms': sorted(list(platforms)),
                    'count': len(cluster_entities),
                })

        return {
            'clusters': entity_clusters,
            'resolved_count': resolved_count,
            'merge_suggestions': merge_suggestions,
            'cross_platform_count': cross_platform_count,
        }

    def _generate_aliases(self, text: str, label: str) -> Set[str]:
        """Generate aliases for entity matching."""
        aliases: Set[str] = set()
        text_lower = text.lower().strip()
        aliases.add(text_lower)

        if label == 'PERSON':
            parts = text.split()
            if len(parts) >= 2:
                # First Last → F. Last, First L.
                aliases.add(f"{parts[0][0]}. {parts[-1]}".lower())
                aliases.add(f"{parts[0]} {parts[-1][0]}.".lower())
                # First name only, last name only
                aliases.add(parts[0].lower())
                aliases.add(parts[-1].lower())
                # Last, First
                aliases.add(f"{parts[-1]}, {parts[0]}".lower())

        elif label == 'ORG':
            # Remove common suffixes
            for suffix in [' inc.', ' inc', ' corp.', ' corp', ' ltd.',
                          ' ltd', ' llc', ' llp', ' co.', ' co']:
                if text_lower.endswith(suffix):
                    aliases.add(text_lower.replace(suffix, '').strip())

            # Acronym
            words = text.split()
            if len(words) >= 2:
                acronym = ''.join(w[0] for w in words if w[0].isupper())
                if len(acronym) >= 2:
                    aliases.add(acronym.lower())

        elif label == 'TOOL':
            # Common variations
            aliases.add(text_lower.replace('.', ''))
            aliases.add(text_lower.replace('-', ''))
            aliases.add(text_lower.replace(' ', ''))

        return aliases

    def _check_time_proximity(self, entity1: Dict, entity2: Dict) -> bool:
        """Check if two entities occurred within the time window."""
        ts1 = self._get_timestamp(entity1)
        ts2 = self._get_timestamp(entity2)

        if ts1 is None or ts2 is None:
            return False

        return abs(ts1 - ts2) <= self.time_window

    def _get_timestamp(self, entity: Dict) -> Optional[datetime]:
        """Extract timestamp from entity metadata."""
        for key in ['timestamp', 'created_at', 'time']:
            val = entity.get(key)
            if isinstance(val, datetime):
                return val
            if isinstance(val, str):
                try:
                    return datetime.fromisoformat(val.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    continue
        return None

    def _fuzzy_match(self, text1: str, text2: str) -> float:
        """
        Simple fuzzy string matching using character-level similarity.

        Returns similarity score 0-1.
        """
        if not text1 or not text2:
            return 0.0

        t1 = text1.lower().strip()
        t2 = text2.lower().strip()

        if t1 == t2:
            return 1.0

        # Levenshtein-ish: character overlap ratio
        set1 = set(t1)
        set2 = set(t2)
        intersection = set1 & set2
        union = set1 | set2

        if not union:
            return 0.0

        jaccard = len(intersection) / len(union)

        # Length ratio penalty
        len_ratio = min(len(t1), len(t2)) / max(len(t1), len(t2))

        # Prefix bonus (for partial matches like "John" and "John Smith")
        prefix_bonus = 0.0
        shorter = t1 if len(t1) <= len(t2) else t2
        longer = t2 if len(t1) <= len(t2) else t1
        if longer.startswith(shorter):
            prefix_bonus = 0.15

        return min(1.0, (jaccard * 0.5 + len_ratio * 0.5 + prefix_bonus))


# Global instance
cross_activity_resolver = CrossActivityResolver()
