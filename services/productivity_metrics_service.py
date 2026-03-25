"""
Productivity Metrics Service for Month 6 Personal Analytics.

Computes 6 core productivity metrics from activity data:
1. Focus Score (0-10): Composite from deep work, context switches, meetings, breaks
2. Deep Work Hours: Continuous focused sessions ≥30min on productive apps
3. Context Switches: Number of distinct app/window switches
4. Meeting Load %: Time spent in meetings vs total tracked time
5. Distraction Index (0-100): Time on non-productive apps
6. Break Quality (0-10): Break distribution and frequency

Uses Redis caching (1h TTL) and stores in PostgreSQL.
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from collections import defaultdict
import structlog
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from config.app_categories import (
    categorize_app, categorize_domain,
    AppCategory
)
from models.analytics_models import DailyMetrics
from database.redis_client import get_redis_client
from database.postgres import get_db
import json

logger = structlog.get_logger(__name__)


class ProductivityMetricsService:
    """Service for computing and managing productivity metrics."""
    
    # Configuration constants
    DEEP_WORK_MIN_DURATION_MINUTES = 30  # Minimum session length for deep work
    DEEP_WORK_MAX_GAP_MINUTES = 5        # Max gap between activities in same session
    CONTEXT_SWITCH_WINDOW_MINUTES = 10   # Window for counting context switches
    BREAK_MIN_DURATION_MINUTES = 5       # Minimum break duration
    BREAK_MAX_DURATION_MINUTES = 60      # Maximum reasonable break
    BREAK_IDEAL_FREQUENCY_HOURS = 2      # Ideal break every 2 hours
    
    # Focus score weights (must sum to 1.0)
    FOCUS_WEIGHT_DEEP_WORK = 0.50
    FOCUS_WEIGHT_CONTEXT = 0.20
    FOCUS_WEIGHT_MEETINGS = 0.15
    FOCUS_WEIGHT_BREAKS = 0.15
    
    def __init__(self, db: AsyncSession, redis_client=None):
        """
        Initialize the metrics service.
        
        Args:
            db: SQLAlchemy async session
            redis_client: Redis client for caching (optional)
        """
        self.db = db
        self.redis = redis_client or get_redis_client()
        
    async def compute_daily_metrics(
        self,
        user_id: str,
        target_date: date,
        activities: List[Dict[str, Any]]
    ) -> DailyMetrics:
        """
        Compute all 6 productivity metrics for a user on a given date.
        
        Idempotent: upserts to daily_metrics table.
        Caches in Redis with 1h TTL.
        
        Args:
            user_id: User UUID
            target_date: Date to compute metrics for
            activities: List of activity dicts with at least:
                {
                    'application_name': str,
                    'window_title': str,
                    'occurred_at': datetime,
                    'duration_seconds': int,
                    'activity_type': str,
                }
        
        Returns:
            DailyMetrics model instance
        """
        logger.info(
            "Computing daily metrics",
            user_id=user_id,
            date=target_date.isoformat(),
            activity_count=len(activities)
        )
        
        # Compute each metric
        deep_work_hours = await self._compute_deep_work_hours(activities)
        context_switches = await self._compute_context_switches(activities)
        meeting_load_pct = await self._compute_meeting_load(activities)
        distraction_index = await self._compute_distraction_index(activities)
        break_quality = await self._compute_break_quality(activities)
        focus_score = await self._compute_focus_score(
            deep_work_hours=deep_work_hours,
            context_switches=context_switches,
            meeting_load_pct=meeting_load_pct,
            break_quality=break_quality,
            total_work_hours=sum(a.get('duration_seconds', 0) for a in activities) / 3600
        )
        
        # Build raw metrics for debugging
        raw_metrics = {
            "total_activities": len(activities),
            "total_duration_hours": sum(a.get('duration_seconds', 0) for a in activities) / 3600,
            "computation_timestamp": datetime.utcnow().isoformat(),
            "version": "1.0"
        }
        
        # Upsert to database
        stmt = select(DailyMetrics).where(
            and_(
                DailyMetrics.user_id == user_id,
                DailyMetrics.date == target_date
            )
        )
        result = await self.db.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            # Update existing
            existing.focus_score = Decimal(str(focus_score))
            existing.deep_work_hours = Decimal(str(deep_work_hours))
            existing.context_switches = context_switches
            existing.meeting_load_pct = Decimal(str(meeting_load_pct))
            existing.distraction_index = Decimal(str(distraction_index))
            existing.break_quality = Decimal(str(break_quality))
            existing.raw_metrics = raw_metrics
            existing.updated_at = datetime.utcnow()
            metrics_obj = existing
        else:
            # Create new
            metrics_obj = DailyMetrics(
                user_id=user_id,
                date=target_date,
                focus_score=Decimal(str(focus_score)),
                deep_work_hours=Decimal(str(deep_work_hours)),
                context_switches=context_switches,
                meeting_load_pct=Decimal(str(meeting_load_pct)),
                distraction_index=Decimal(str(distraction_index)),
                break_quality=Decimal(str(break_quality)),
                raw_metrics=raw_metrics
            )
            self.db.add(metrics_obj)
        
        await self.db.commit()
        await self.db.refresh(metrics_obj)
        
        # Cache in Redis (1h TTL)
        cache_key = f"analytics:metrics:{user_id}:{target_date.isoformat()}"
        await self.redis.setex(
            cache_key,
            3600,  # 1 hour
            json.dumps(metrics_obj.to_dict())
        )
        
        logger.info(
            "Computed daily metrics",
            user_id=user_id,
            date=target_date.isoformat(),
            focus_score=float(focus_score),
            deep_work_hours=float(deep_work_hours)
        )
        
        return metrics_obj
    
    async def get_daily_metrics(
        self,
        user_id: str,
        target_date: date
    ) -> Optional[DailyMetrics]:
        """
        Get daily metrics from cache or database.
        
        Does NOT recompute if missing - returns None.
        Use compute_daily_metrics to force computation.
        
        Args:
            user_id: User UUID
            target_date: Date to retrieve
        
        Returns:
            DailyMetrics or None if not found
        """
        # Try cache first
        cache_key = f"analytics:metrics:{user_id}:{target_date.isoformat()}"
        cached = await self.redis.get(cache_key)
        
        if cached:
            logger.debug("Cache hit for daily metrics", user_id=user_id, date=target_date.isoformat())
            # Note: In production, you'd reconstruct the model from JSON
            # For now, fall through to DB query
        
        # Query database
        stmt = select(DailyMetrics).where(
            and_(
                DailyMetrics.user_id == user_id,
                DailyMetrics.date == target_date
            )
        )
        result = await self.db.execute(stmt)
        metrics = result.scalar_one_or_none()
        
        if metrics and not cached:
            # Populate cache
            await self.redis.setex(
                cache_key,
                3600,
                json.dumps(metrics.to_dict())
            )
        
        return metrics
    
    async def get_weekly_aggregate(
        self,
        user_id: str,
        week_start: date
    ) -> Dict[str, Any]:
        """
        Aggregate daily metrics for a full week (Monday-Sunday).
        
        Args:
            user_id: User UUID
            week_start: Monday of the week
        
        Returns:
            Dict with aggregated metrics and daily breakdown
        """
        week_end = week_start + timedelta(days=6)
        
        # Query all days in week
        stmt = select(DailyMetrics).where(
            and_(
                DailyMetrics.user_id == user_id,
                DailyMetrics.date >= week_start,
                DailyMetrics.date <= week_end
            )
        ).order_by(DailyMetrics.date)
        
        result = await self.db.execute(stmt)
        daily_metrics = result.scalars().all()
        
        if not daily_metrics:
            return {
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "days_tracked": 0,
                "averages": {},
                "totals": {},
                "daily_breakdown": []
            }
        
        # Compute aggregates
        focus_scores = [float(m.focus_score) for m in daily_metrics if m.focus_score]
        deep_work_hours_list = [float(m.deep_work_hours) for m in daily_metrics if m.deep_work_hours]
        meeting_loads = [float(m.meeting_load_pct) for m in daily_metrics if m.meeting_load_pct]
        context_switches_list = [m.context_switches for m in daily_metrics if m.context_switches]
        distraction_indexes = [float(m.distraction_index) for m in daily_metrics if m.distraction_index]
        break_qualities = [float(m.break_quality) for m in daily_metrics if m.break_quality]
        
        return {
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "days_tracked": len(daily_metrics),
            "averages": {
                "focus_score": sum(focus_scores) / len(focus_scores) if focus_scores else 0,
                "deep_work_hours": sum(deep_work_hours_list) / len(deep_work_hours_list) if deep_work_hours_list else 0,
                "meeting_load_pct": sum(meeting_loads) / len(meeting_loads) if meeting_loads else 0,
                "context_switches": sum(context_switches_list) / len(context_switches_list) if context_switches_list else 0,
                "distraction_index": sum(distraction_indexes) / len(distraction_indexes) if distraction_indexes else 0,
                "break_quality": sum(break_qualities) / len(break_qualities) if break_qualities else 0,
            },
            "totals": {
                "deep_work_hours": sum(deep_work_hours_list),
                "context_switches": sum(context_switches_list),
            },
            "best_day": {
                "focus_score": max(daily_metrics, key=lambda m: m.focus_score or 0).date.isoformat() if daily_metrics else None,
                "deep_work": max(daily_metrics, key=lambda m: m.deep_work_hours or 0).date.isoformat() if daily_metrics else None,
            },
            "worst_day": {
                "focus_score": min(daily_metrics, key=lambda m: m.focus_score or 10).date.isoformat() if daily_metrics else None,
            },
            "daily_breakdown": [m.to_dict() for m in daily_metrics]
        }
    
    # =========================================================================
    # PRIVATE METRIC COMPUTATION METHODS
    # =========================================================================
    
    async def _compute_deep_work_hours(self, activities: List[Dict]) -> float:
        """
        Compute deep work hours: continuous focused sessions ≥30min on productive apps.
        
        Algorithm:
        1. Filter productive activities
        2. Group into sessions (max 5min gaps)
        3. Count sessions ≥30min
        """
        if not activities:
            return 0.0
        
        # Filter and categorize
        productive_activities = []
        for act in activities:
            app = act.get('application_name', '')
            category = categorize_app(app)
            
            if category == AppCategory.PRODUCTIVE:
                productive_activities.append({
                    'start': act.get('occurred_at'),
                    'duration': act.get('duration_seconds', 0)
                })
        
        if not productive_activities:
            return 0.0
        
        # Sort by start time
        productive_activities.sort(key=lambda x: x['start'])
        
        # Group into sessions
        sessions = []
        current_session_duration = 0
        last_end_time = None
        
        for act in productive_activities:
            if last_end_time is None:
                # First activity
                current_session_duration = act['duration']
                last_end_time = act['start'] + timedelta(seconds=act['duration'])
            else:
                gap = (act['start'] - last_end_time).total_seconds()
                
                if gap <= self.DEEP_WORK_MAX_GAP_MINUTES * 60:
                    # Continue session
                    current_session_duration += act['duration']
                    last_end_time = act['start'] + timedelta(seconds=act['duration'])
                else:
                    # End session, start new
                    sessions.append(current_session_duration)
                    current_session_duration = act['duration']
                    last_end_time = act['start'] + timedelta(seconds=act['duration'])
        
        # Don't forget last session
        if current_session_duration > 0:
            sessions.append(current_session_duration)
        
        # Filter sessions ≥30min
        deep_work_seconds = sum(
            s for s in sessions
            if s >= self.DEEP_WORK_MIN_DURATION_MINUTES * 60
        )
        
        return deep_work_seconds / 3600  # Convert to hours
    
    async def _compute_context_switches(self, activities: List[Dict]) -> int:
        """
        Compute context switches: number of distinct app/window changes.
        
        Counts switches in 10-minute windows to avoid overcounting rapid switches.
        """
        if len(activities) <= 1:
            return 0
        
        # Sort by time
        sorted_activities = sorted(activities, key=lambda x: x.get('occurred_at'))
        
        switches = 0
        last_app = None
        last_window_time = None
        
        for act in sorted_activities:
            current_app = act.get('application_name', '')
            current_time = act.get('occurred_at')
            
            if last_app and current_app != last_app:
                # Check if enough time has passed (avoid overcounting)
                if last_window_time is None or \
                   (current_time - last_window_time).total_seconds() >= self.CONTEXT_SWITCH_WINDOW_MINUTES * 60:
                    switches += 1
                    last_window_time = current_time
            
            last_app = current_app
        
        return switches
    
    async def _compute_meeting_load(self, activities: List[Dict]) -> float:
        """
        Compute meeting load percentage: (meeting time / total time) * 100.
        """
        if not activities:
            return 0.0
        
        total_duration = sum(a.get('duration_seconds', 0) for a in activities)
        
        if total_duration == 0:
            return 0.0
        
        meeting_duration = sum(
            a.get('duration_seconds', 0)
            for a in activities
            if categorize_app(a.get('application_name', '')) == AppCategory.MEETINGS
        )
        
        return (meeting_duration / total_duration) * 100
    
    async def _compute_distraction_index(self, activities: List[Dict]) -> float:
        """
        Compute distraction index: (distraction time / focus time) * 100.
        
        Higher is worse (more distractions).
        """
        if not activities:
            return 0.0
        
       # Calculate focus time (productive + neutral)
        focus_duration = sum(
            a.get('duration_seconds', 0)
            for a in activities
            if categorize_app(a.get('application_name', '')) in [AppCategory.PRODUCTIVE, AppCategory.NEUTRAL]
        )
        
        distraction_duration = sum(
            a.get('duration_seconds', 0)
            for a in activities
            if categorize_app(a.get('application_name', '')) == AppCategory.DISTRACTIVE
        )
        
        if focus_duration == 0:
            return 100.0 if distraction_duration > 0 else 0.0
        
        return min((distraction_duration / focus_duration) * 100, 100.0)
    
    async def _compute_break_quality(self, activities: List[Dict]) -> float:
        """
        Compute break quality (0-10) based on break distribution and frequency.
        
        Algorithm:
        1. Identify breaks (gaps between activities)
        2. Score based on:
           - Reasonable break lengths (5-60 min)
           - Regular frequency (every ~2 hours)
           - Not skipping breaks entirely
        """
        if len(activities) <= 1:
            return 5.0  # Neutral score for insufficient data
        
        # Sort by time
        sorted_activities = sorted(activities, key=lambda x: x.get('occurred_at'))
        
        # Find breaks (gaps ≥5 min)
        breaks = []
        for i in range(len(sorted_activities) - 1):
            end_time = sorted_activities[i].get('occurred_at') + timedelta(
                seconds=sorted_activities[i].get('duration_seconds', 0)
            )
            next_start = sorted_activities[i + 1].get('occurred_at')
            gap_minutes = (next_start - end_time).total_seconds() / 60
            
            if gap_minutes >= self.BREAK_MIN_DURATION_MINUTES:
                breaks.append(gap_minutes)
        
        if not breaks:
            return 3.0  # Low score for no breaks
        
        # Score components
        
        # 1. Reasonable break lengths (40% of score)
        reasonable_breaks = [
            b for b in breaks
            if self.BREAK_MIN_DURATION_MINUTES <= b <= self.BREAK_MAX_DURATION_MINUTES
        ]
        length_score = (len(reasonable_breaks) / len(breaks)) * 4.0
        
        # 2. Frequency (40% of score)
        total_work_hours = sum(a.get('duration_seconds', 0) for a in sorted_activities) / 3600
        ideal_break_count = total_work_hours / self.BREAK_IDEAL_FREQUENCY_HOURS
        actual_break_count = len(breaks)
        
        frequency_ratio = min(actual_break_count / ideal_break_count, 1.5) if ideal_break_count > 0 else 0
        frequency_score = min(frequency_ratio, 1.0) * 4.0
        
        # 3. Not too many long breaks (20% of score)
        long_breaks = [b for b in breaks if b > self.BREAK_MAX_DURATION_MINUTES]
        long_break_penalty = len(long_breaks) * 0.5
        consistency_score = max(2.0 - long_break_penalty, 0)
        
        total_score = length_score + frequency_score + consistency_score
        
        return min(max(total_score, 0.0), 10.0)
    
    async def _compute_focus_score(
        self,
        deep_work_hours: float,
        context_switches: int,
        meeting_load_pct: float,
        break_quality: float,
        total_work_hours: float
    ) -> float:
        """
        Compute focus score (0-10) as weighted composite.
        
        Formula:
            focus = 10 * (
                0.50 * deep_work_ratio +
                0.20 * context_penalty +
                0.15 * meeting_penalty +
                0.15 * break_score_normalized
            )
        """
        # Deep work ratio
        dw_ratio = min(deep_work_hours / total_work_hours, 1.0) if total_work_hours > 0 else 0
        
        # Context switch penalty (inverse, clamped)
        ctx_penalty = max(1 - (context_switches / 40), 0)
        
        # Meeting penalty (inverse, clamped)
        mtg_penalty = max(1 - (meeting_load_pct / 60), 0)
        
        # Break score (normalized to 0-1)
        break_score_norm = break_quality / 10
        
        # Weighted sum
        focus = 10 * (
            self.FOCUS_WEIGHT_DEEP_WORK * dw_ratio +
            self.FOCUS_WEIGHT_CONTEXT * ctx_penalty +
            self.FOCUS_WEIGHT_MEETINGS * mtg_penalty +
            self.FOCUS_WEIGHT_BREAKS * break_score_norm
        )
        
        return min(max(focus, 0.0), 10.0)



# ============================================================================
# STANDALONE UTILITY FUNCTIONS (work with pre-fetched data, no DB/Redis)
# ============================================================================


def get_time_allocation_by_project(
    activities: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Compute detailed time allocation by project/category.

    Args:
        activities: List of {application_name, window_title, duration_seconds,
                             project (optional), category (optional)}

    Returns:
        Time allocation breakdown by project and category
    """
    if not activities:
        return {
            'total_hours': 0,
            'by_project': [],
            'by_category': [],
            'unallocated_hours': 0,
        }

    by_project: Dict[str, float] = defaultdict(float)
    by_category: Dict[str, float] = defaultdict(float)
    total_seconds = 0

    for act in activities:
        dur = act.get('duration_seconds', 0)
        total_seconds += dur

        # Project allocation
        project = act.get('project', act.get('window_title', 'Unknown'))
        if project:
            by_project[project] += dur

        # Category allocation
        app = act.get('application_name', '')
        category = act.get('category', categorize_app(app).value if app else 'unknown')
        by_category[category] += dur

    total_hours = total_seconds / 3600

    # Build project breakdown
    project_list = sorted(
        [
            {
                'name': name,
                'hours': round(secs / 3600, 2),
                'percentage': round(secs / max(total_seconds, 1) * 100, 1),
            }
            for name, secs in by_project.items()
        ],
        key=lambda x: x['hours'],
        reverse=True,
    )

    # Build category breakdown
    category_list = sorted(
        [
            {
                'category': name,
                'hours': round(secs / 3600, 2),
                'percentage': round(secs / max(total_seconds, 1) * 100, 1),
            }
            for name, secs in by_category.items()
        ],
        key=lambda x: x['hours'],
        reverse=True,
    )

    return {
        'total_hours': round(total_hours, 2),
        'by_project': project_list[:20],
        'by_category': category_list,
        'project_count': len(by_project),
    }


def get_comparative_analytics(
    current_period: Dict[str, Any],
    previous_periods: List[Dict[str, Any]],
    period_type: str = 'week',
) -> Dict[str, Any]:
    """
    Compare current metrics against previous periods.

    Args:
        current_period: {focus_score, deep_work_hours, context_switches,
                         meeting_load_pct, distraction_index, break_quality, total_hours}
        previous_periods: List of similar dicts for prior periods (recent first)
        period_type: 'week' or 'month'

    Returns:
        Comparative analysis with deltas, trends, and rankings
    """
    METRIC_KEYS = [
        'focus_score', 'deep_work_hours', 'context_switches',
        'meeting_load_pct', 'distraction_index', 'break_quality', 'total_hours',
    ]

    # Metrics where higher is worse
    INVERSE_METRICS = {'context_switches', 'distraction_index', 'meeting_load_pct'}

    if not previous_periods:
        return {
            'period_type': period_type,
            'comparisons': {},
            'overall_trend': 'insufficient_data',
            'periods_compared': 0,
        }

    # Compute averages of previous periods
    prev_avg: Dict[str, float] = {}
    for key in METRIC_KEYS:
        values = [p.get(key, 0) for p in previous_periods if key in p]
        prev_avg[key] = sum(values) / len(values) if values else 0

    comparisons: Dict[str, Dict[str, Any]] = {}
    improvements = 0
    regressions = 0

    for key in METRIC_KEYS:
        current_val = current_period.get(key, 0)
        avg_val = prev_avg.get(key, 0)

        delta = current_val - avg_val
        pct_change = (delta / avg_val * 100) if avg_val != 0 else 0

        # Determine if change is positive or negative
        is_inverse = key in INVERSE_METRICS
        if abs(pct_change) < 3:
            direction = 'stable'
        elif (delta > 0 and not is_inverse) or (delta < 0 and is_inverse):
            direction = 'improved'
            improvements += 1
        else:
            direction = 'declined'
            regressions += 1

        # Rank against previous periods
        all_values = [p.get(key, 0) for p in previous_periods] + [current_val]
        if is_inverse:
            all_values_sorted = sorted(all_values)
        else:
            all_values_sorted = sorted(all_values, reverse=True)
        rank = all_values_sorted.index(current_val) + 1

        comparisons[key] = {
            'current': round(current_val, 2),
            'previous_avg': round(avg_val, 2),
            'delta': round(delta, 2),
            'pct_change': round(pct_change, 1),
            'direction': direction,
            'rank': rank,
            'rank_total': len(all_values),
        }

    # Overall trend
    if improvements > regressions + 2:
        overall_trend = 'improving'
    elif regressions > improvements + 2:
        overall_trend = 'declining'
    elif improvements > regressions:
        overall_trend = 'slightly_improving'
    elif regressions > improvements:
        overall_trend = 'slightly_declining'
    else:
        overall_trend = 'stable'

    return {
        'period_type': period_type,
        'comparisons': comparisons,
        'overall_trend': overall_trend,
        'improvements': improvements,
        'regressions': regressions,
        'periods_compared': len(previous_periods),
    }


# Convenience function for dependency injection
async def get_metrics_service(db: AsyncSession) -> ProductivityMetricsService:
    """Get ProductivityMetricsService instance for FastAPI routes."""
    return ProductivityMetricsService(db=db)
