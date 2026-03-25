"""
Temporal Enrichment Service.

Extracts time-based context from activity metadata:
- Time-of-day classification (morning, afternoon, evening, night)
- Work-hours detection
- Day-of-week and weekend detection
- Duration categorization
- Session ordering within a day
- Recurring pattern detection
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, time
from collections import defaultdict, Counter
import structlog

logger = structlog.get_logger()


# ============================================================================
# TIME CLASSIFICATION CONSTANTS
# ============================================================================

TIME_PERIODS = {
    "early_morning": (time(5, 0), time(8, 0)),
    "morning": (time(8, 0), time(12, 0)),
    "afternoon": (time(12, 0), time(17, 0)),
    "evening": (time(17, 0), time(21, 0)),
    "night": (time(21, 0), time(23, 59)),
    "late_night": (time(0, 0), time(5, 0)),
}

WORK_HOURS = {
    "start": time(9, 0),
    "end": time(17, 0),
    "core_start": time(10, 0),
    "core_end": time(16, 0),
}

DURATION_CATEGORIES = {
    "micro": (0, 30),           # < 30 seconds
    "brief": (30, 120),         # 30s - 2 min
    "short": (120, 600),        # 2 - 10 min
    "medium": (600, 1800),      # 10 - 30 min
    "long": (1800, 3600),       # 30 - 60 min
    "extended": (3600, 7200),   # 1 - 2 hours
    "deep": (7200, float('inf')),  # 2+ hours
}


class TemporalEnricher:
    """
    Service for extracting temporal context from activities.

    Enriches activities with time-based metadata for pattern analysis
    and productivity insights.
    """

    def __init__(self, timezone_offset: int = 0):
        """
        Initialize temporal enricher.

        Args:
            timezone_offset: UTC offset in hours (e.g., -5 for EST)
        """
        self.timezone_offset = timezone_offset

    def enrich_temporal(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich an activity with temporal metadata.

        Args:
            activity: Activity dict with timestamp, duration_seconds, etc.

        Returns:
            Dict with temporal enrichment data:
            - time_of_day: str (morning, afternoon, evening, night)
            - time_period: str (early_morning, morning, etc.)
            - is_work_hours: bool
            - is_core_hours: bool
            - is_weekend: bool
            - day_of_week: str (Monday, Tuesday, etc.)
            - day_of_week_number: int (0=Monday)
            - hour: int (0-23)
            - duration_category: str (micro, brief, short, etc.)
            - session_label: str (start_of_day, mid_morning, etc.)
        """
        timestamp = self._extract_timestamp(activity)
        if not timestamp:
            return self._empty_enrichment()

        # Apply timezone offset
        local_time = timestamp + timedelta(hours=self.timezone_offset)
        local_t = local_time.time()

        enrichment = {
            'timestamp_utc': timestamp.isoformat(),
            'timestamp_local': local_time.isoformat(),
            'time_of_day': self._classify_time_of_day(local_t),
            'time_period': self._get_time_period(local_t),
            'is_work_hours': self._is_work_hours(local_t),
            'is_core_hours': self._is_core_hours(local_t),
            'is_weekend': local_time.weekday() >= 5,
            'day_of_week': local_time.strftime('%A'),
            'day_of_week_number': local_time.weekday(),
            'hour': local_time.hour,
            'minute': local_time.minute,
            'session_label': self._get_session_label(local_t),
        }

        # Duration categorization
        duration = activity.get('duration_seconds') or activity.get('duration', 0)
        if duration:
            enrichment['duration_seconds'] = duration
            enrichment['duration_category'] = self._categorize_duration(duration)
            enrichment['duration_minutes'] = round(duration / 60, 1)

        return enrichment

    def get_temporal_patterns(
        self,
        activities: List[Dict[str, Any]],
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Analyze temporal patterns from a list of activities.

        Args:
            activities: List of activity dicts with timestamps
            days: Number of days to analyze

        Returns:
            Dict with:
            - hourly_heatmap: Dict[int, int] — activity count by hour
            - daily_heatmap: Dict[str, int] — activity count by day
            - peak_hours: List[int] — top 3 most active hours
            - peak_days: List[str] — top 3 most active days
            - work_hours_percentage: float — % of activities during work hours
            - weekend_percentage: float — % of activities on weekends
            - avg_daily_count: float — average activities per day
            - recurring_patterns: List[Dict] — detected recurring patterns
        """
        hourly: Counter = Counter()
        daily: Counter = Counter()
        work_hours_count = 0
        weekend_count = 0
        total = 0

        for activity in activities:
            ts = self._extract_timestamp(activity)
            if not ts:
                continue

            local = ts + timedelta(hours=self.timezone_offset)
            total += 1

            hourly[local.hour] += 1
            daily[local.strftime('%A')] += 1

            if self._is_work_hours(local.time()):
                work_hours_count += 1
            if local.weekday() >= 5:
                weekend_count += 1

        if total == 0:
            return self._empty_patterns()

        # Find peak hours and days
        peak_hours = [h for h, _ in hourly.most_common(3)]
        peak_days = [d for d, _ in daily.most_common(3)]

        # Detect recurring patterns
        recurring = self._detect_recurring_patterns(activities)

        return {
            'hourly_heatmap': dict(hourly),
            'daily_heatmap': dict(daily),
            'peak_hours': peak_hours,
            'peak_days': peak_days,
            'work_hours_percentage': round(work_hours_count / total * 100, 1),
            'weekend_percentage': round(weekend_count / total * 100, 1),
            'avg_daily_count': round(total / max(days, 1), 1),
            'total_activities': total,
            'recurring_patterns': recurring,
        }

    def _extract_timestamp(self, activity: Dict) -> Optional[datetime]:
        """Extract datetime from activity metadata."""
        for key in ['timestamp', 'created_at', 'started_at', 'time']:
            val = activity.get(key)
            if val is None:
                continue
            if isinstance(val, datetime):
                return val
            if isinstance(val, str):
                try:
                    return datetime.fromisoformat(val.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    continue
            if isinstance(val, (int, float)):
                try:
                    return datetime.fromtimestamp(val)
                except (ValueError, OSError):
                    continue
        return None

    def _classify_time_of_day(self, t: time) -> str:
        """Classify time into broad time-of-day category."""
        hour = t.hour
        if 5 <= hour < 12:
            return "morning"
        elif 12 <= hour < 17:
            return "afternoon"
        elif 17 <= hour < 21:
            return "evening"
        else:
            return "night"

    def _get_time_period(self, t: time) -> str:
        """Get detailed time period."""
        for period, (start, end) in TIME_PERIODS.items():
            if period == "late_night":
                if t >= time(0, 0) and t < time(5, 0):
                    return period
            elif start <= t < end:
                return period
        return "night"

    def _is_work_hours(self, t: time) -> bool:
        """Check if time falls within standard work hours (9-5)."""
        return WORK_HOURS["start"] <= t <= WORK_HOURS["end"]

    def _is_core_hours(self, t: time) -> bool:
        """Check if time falls within core hours (10-4)."""
        return WORK_HOURS["core_start"] <= t <= WORK_HOURS["core_end"]

    def _categorize_duration(self, seconds: float) -> str:
        """Categorize duration into named buckets."""
        for category, (low, high) in DURATION_CATEGORIES.items():
            if low <= seconds < high:
                return category
        return "deep"

    def _get_session_label(self, t: time) -> str:
        """Get a human-readable session label."""
        hour = t.hour
        if 5 <= hour < 7:
            return "early_bird"
        elif 7 <= hour < 9:
            return "morning_warmup"
        elif 9 <= hour < 10:
            return "start_of_day"
        elif 10 <= hour < 12:
            return "mid_morning"
        elif 12 <= hour < 13:
            return "lunch_break"
        elif 13 <= hour < 15:
            return "early_afternoon"
        elif 15 <= hour < 17:
            return "late_afternoon"
        elif 17 <= hour < 19:
            return "end_of_day"
        elif 19 <= hour < 22:
            return "evening_session"
        else:
            return "night_owl"

    def _detect_recurring_patterns(self, activities: List[Dict]) -> List[Dict]:
        """Detect recurring time patterns in activities."""
        patterns = []

        # Group by (day_of_week, hour)
        time_slots: Dict[tuple, int] = defaultdict(int)
        for act in activities:
            ts = self._extract_timestamp(act)
            if not ts:
                continue
            local = ts + timedelta(hours=self.timezone_offset)
            key = (local.strftime('%A'), local.hour)
            time_slots[key] += 1

        # Find recurring slots (appear 3+ times)
        for (day, hour), count in time_slots.items():
            if count >= 3:
                patterns.append({
                    'day': day,
                    'hour': hour,
                    'occurrences': count,
                    'label': f"Recurring {day} {hour}:00",
                })

        # Sort by occurrence count
        patterns.sort(key=lambda p: p['occurrences'], reverse=True)
        return patterns[:10]  # Top 10

    def _empty_enrichment(self) -> Dict[str, Any]:
        """Return empty enrichment when no timestamp available."""
        return {
            'timestamp_utc': None,
            'timestamp_local': None,
            'time_of_day': 'unknown',
            'time_period': 'unknown',
            'is_work_hours': False,
            'is_core_hours': False,
            'is_weekend': False,
            'day_of_week': 'unknown',
            'day_of_week_number': -1,
            'hour': -1,
            'minute': -1,
            'session_label': 'unknown',
        }

    def _empty_patterns(self) -> Dict[str, Any]:
        """Return empty patterns when no activities available."""
        return {
            'hourly_heatmap': {},
            'daily_heatmap': {},
            'peak_hours': [],
            'peak_days': [],
            'work_hours_percentage': 0.0,
            'weekend_percentage': 0.0,
            'avg_daily_count': 0.0,
            'total_activities': 0,
            'recurring_patterns': [],
        }


# Global instance
temporal_enricher = TemporalEnricher()
