"""
Proactive Insights Service

Generates proactive push insights by detecting patterns and anomalies:
- Anomaly detection (unusual patterns, streaks, drops)
- Scheduled insight generation (daily/weekly triggers)
- Insight categories: productivity, focus, collaboration, wellness
- Priority scoring for surfacing relevant insights
- Deduplication to avoid repeating insights
"""

from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
from collections import defaultdict
import math
import uuid
import structlog

logger = structlog.get_logger()


# ============================================================================
# INSIGHT CATEGORIES & TEMPLATES
# ============================================================================

INSIGHT_CATEGORIES = {
    'productivity': {
        'icon': '📈',
        'color': '#10B981',
        'label': 'Productivity',
    },
    'focus': {
        'icon': '🎯',
        'color': '#6366F1',
        'label': 'Focus',
    },
    'collaboration': {
        'icon': '🤝',
        'color': '#F59E0B',
        'label': 'Collaboration',
    },
    'wellness': {
        'icon': '💚',
        'color': '#EF4444',
        'label': 'Wellness',
    },
    'achievement': {
        'icon': '🏆',
        'color': '#8B5CF6',
        'label': 'Achievement',
    },
    'learning': {
        'icon': '📚',
        'color': '#3B82F6',
        'label': 'Learning',
    },
}


class Insight:
    """Represents a single proactive insight."""

    __slots__ = ('id', 'user_id', 'category', 'title', 'description',
                 'priority', 'data', 'action', 'dismissed', 'created_at',
                 'expires_at', 'source')

    def __init__(
        self,
        user_id: str,
        category: str,
        title: str,
        description: str,
        priority: float = 0.5,
        data: Optional[Dict[str, Any]] = None,
        action: Optional[Dict[str, str]] = None,
        source: str = 'system',
        expires_hours: int = 72,
    ):
        self.id = str(uuid.uuid4())
        self.user_id = user_id
        self.category = category
        self.title = title
        self.description = description
        self.priority = min(max(priority, 0.0), 1.0)
        self.data = data or {}
        self.action = action  # {label, url}
        self.dismissed = False
        self.source = source
        self.created_at = datetime.now(tz=None).isoformat()
        expires = datetime.now(tz=None) + timedelta(hours=expires_hours)
        self.expires_at = expires.isoformat()

    def to_dict(self) -> Dict[str, Any]:
        cat_meta = INSIGHT_CATEGORIES.get(self.category, {})
        return {
            'id': self.id,
            'user_id': self.user_id,
            'category': self.category,
            'category_icon': cat_meta.get('icon', '💡'),
            'category_label': cat_meta.get('label', self.category.title()),
            'title': self.title,
            'description': self.description,
            'priority': round(self.priority, 3),
            'data': self.data,
            'action': self.action,
            'dismissed': self.dismissed,
            'source': self.source,
            'created_at': self.created_at,
            'expires_at': self.expires_at,
        }


class ProactiveInsightsService:
    """
    Service for generating and managing proactive insights.

    Analyzes user activity data to detect patterns, anomalies, and
    achievements, then surfaces them as prioritized insights.
    """

    # Thresholds
    STREAK_MIN_DAYS = 3
    ANOMALY_STDDEV_THRESHOLD = 1.5
    MAX_INSIGHTS_PER_DAY = 5
    DEDUP_HOURS = 48

    def __init__(self):
        self._insights: Dict[str, List[Insight]] = defaultdict(list)  # user_id -> insights
        self._dismissed_titles: Dict[str, Set[str]] = defaultdict(set)

    # ========================================================================
    # INSIGHT GENERATION
    # ========================================================================

    def generate_daily_insights(
        self,
        user_id: str,
        daily_metrics: Dict[str, Any],
        historical_metrics: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate insights based on today's metrics vs historical data.

        Args:
            user_id: User ID
            daily_metrics: Today's metrics (hours, focus_score, meetings, etc.)
            historical_metrics: List of past daily metrics for comparison

        Returns:
            List of generated insight dicts
        """
        historical = historical_metrics or []
        new_insights = []

        # 1. Focus score insights
        focus_insights = self._analyze_focus(user_id, daily_metrics, historical)
        new_insights.extend(focus_insights)

        # 2. Productivity insights
        prod_insights = self._analyze_productivity(user_id, daily_metrics, historical)
        new_insights.extend(prod_insights)

        # 3. Wellness insights
        wellness_insights = self._analyze_wellness(user_id, daily_metrics, historical)
        new_insights.extend(wellness_insights)

        # 4. Achievement insights
        achievement_insights = self._detect_achievements(user_id, daily_metrics, historical)
        new_insights.extend(achievement_insights)

        # 5. Collaboration insights
        collab_insights = self._analyze_collaboration(user_id, daily_metrics, historical)
        new_insights.extend(collab_insights)

        # Deduplicate and prioritize
        new_insights = self._deduplicate(user_id, new_insights)
        new_insights.sort(key=lambda x: x.priority, reverse=True)
        new_insights = new_insights[:self.MAX_INSIGHTS_PER_DAY]

        # Store insights
        for insight in new_insights:
            self._insights[user_id].append(insight)

        logger.info("insights_generated", user_id=user_id, count=len(new_insights))
        return [i.to_dict() for i in new_insights]

    def generate_weekly_insights(
        self,
        user_id: str,
        weekly_summary: Dict[str, Any],
        previous_weeks: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Generate weekly insights by comparing week-over-week."""
        prev = previous_weeks or []
        new_insights = []

        total_hours = weekly_summary.get('total_hours', 0)
        deep_work = weekly_summary.get('deep_work_hours', 0)
        meetings = weekly_summary.get('meeting_hours', 0)
        avg_focus = weekly_summary.get('avg_focus_score', 0)

        # Deep work ratio insight
        if total_hours > 0:
            dw_ratio = deep_work / total_hours
            if dw_ratio >= 0.5:
                new_insights.append(Insight(
                    user_id=user_id,
                    category='productivity',
                    title='Strong deep work week',
                    description=f'You spent {dw_ratio:.0%} of your time in deep work '
                                f'({deep_work:.1f}h). Keep up the great focus!',
                    priority=0.7,
                    data={'deep_work_ratio': dw_ratio, 'deep_work_hours': deep_work},
                ))
            elif dw_ratio < 0.25:
                new_insights.append(Insight(
                    user_id=user_id,
                    category='focus',
                    title='Low deep work this week',
                    description=f'Only {dw_ratio:.0%} of your time was deep work '
                                f'({deep_work:.1f}h). Consider blocking focused time.',
                    priority=0.8,
                    data={'deep_work_ratio': dw_ratio},
                    action={'label': 'Schedule focus time', 'url': '/dashboard/focus'},
                ))

        # Meeting load insight
        if meetings > 15:
            new_insights.append(Insight(
                user_id=user_id,
                category='wellness',
                title='Heavy meeting week',
                description=f'You had {meetings:.1f}h of meetings. '
                            'Consider declining non-essential meetings next week.',
                priority=0.75,
                data={'meeting_hours': meetings},
            ))

        # Week-over-week comparison
        if prev:
            last_week = prev[-1]
            last_hours = last_week.get('total_hours', 0)
            if last_hours > 0 and total_hours > 0:
                change = (total_hours - last_hours) / last_hours
                if change > 0.2:
                    new_insights.append(Insight(
                        user_id=user_id,
                        category='wellness',
                        title='Workload increasing',
                        description=f'You worked {change:.0%} more hours than last week '
                                    f'({total_hours:.1f}h vs {last_hours:.1f}h).',
                        priority=0.65,
                        data={'change_pct': change},
                    ))

        # Deduplicate and store
        new_insights = self._deduplicate(user_id, new_insights)
        for insight in new_insights:
            self._insights[user_id].append(insight)

        return [i.to_dict() for i in new_insights]

    # ========================================================================
    # INSIGHT RETRIEVAL
    # ========================================================================

    def get_active_insights(
        self,
        user_id: str,
        category: Optional[str] = None,
        limit: int = 10,
        include_dismissed: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get active (non-expired, non-dismissed) insights."""
        now = datetime.now(tz=None).isoformat()
        user_insights = self._insights.get(user_id, [])

        results = []
        for insight in user_insights:
            if insight.expires_at < now:
                continue
            if not include_dismissed and insight.dismissed:
                continue
            if category and insight.category != category:
                continue
            results.append(insight.to_dict())

        # Sort by priority descending
        results.sort(key=lambda x: x['priority'], reverse=True)
        return results[:limit]

    def dismiss_insight(
        self,
        user_id: str,
        insight_id: str,
    ) -> bool:
        """Dismiss an insight so it no longer surfaces."""
        user_insights = self._insights.get(user_id, [])
        for insight in user_insights:
            if insight.id == insight_id:
                insight.dismissed = True
                self._dismissed_titles[user_id].add(insight.title)
                return True
        return False

    def get_insight_stats(
        self,
        user_id: str,
    ) -> Dict[str, Any]:
        """Get insight statistics for a user."""
        user_insights = self._insights.get(user_id, [])
        now = datetime.now(tz=None).isoformat()

        total = len(user_insights)
        active = sum(1 for i in user_insights if not i.dismissed and i.expires_at >= now)
        dismissed = sum(1 for i in user_insights if i.dismissed)
        expired = sum(1 for i in user_insights if i.expires_at < now)

        by_category: Dict[str, int] = defaultdict(int)
        for i in user_insights:
            by_category[i.category] += 1

        return {
            'total_generated': total,
            'active': active,
            'dismissed': dismissed,
            'expired': expired,
            'by_category': dict(by_category),
        }

    # ========================================================================
    # ANALYSIS METHODS
    # ========================================================================

    def _analyze_focus(
        self,
        user_id: str,
        today: Dict[str, Any],
        history: List[Dict[str, Any]],
    ) -> List[Insight]:
        """Analyze focus score patterns."""
        insights = []
        focus = today.get('focus_score', 0)

        if not history:
            if focus >= 8:
                insights.append(Insight(
                    user_id=user_id,
                    category='focus',
                    title='Excellent focus today',
                    description=f'Your focus score is {focus}/10. Outstanding concentration!',
                    priority=0.6,
                    data={'focus_score': focus},
                ))
            return insights

        # Compare to average
        avg_focus = sum(h.get('focus_score', 0) for h in history) / len(history)

        if focus > avg_focus * 1.3 and focus >= 7:
            insights.append(Insight(
                user_id=user_id,
                category='focus',
                title='Focus above average',
                description=f'Your focus score ({focus}/10) is {((focus/avg_focus-1)*100):.0f}% '
                            f'above your average ({avg_focus:.1f}). Great job!',
                priority=0.7,
                data={'focus_score': focus, 'avg_focus': avg_focus},
            ))

        # Detect focus streak
        streak = self._count_streak(history, 'focus_score', threshold=7.0)
        if streak >= self.STREAK_MIN_DAYS:
            insights.append(Insight(
                user_id=user_id,
                category='achievement',
                title=f'{streak}-day focus streak!',
                description=f'You\'ve maintained a focus score above 7 for {streak} days. '
                            'Incredible consistency!',
                priority=0.85,
                data={'streak_days': streak},
            ))

        return insights

    def _analyze_productivity(
        self,
        user_id: str,
        today: Dict[str, Any],
        history: List[Dict[str, Any]],
    ) -> List[Insight]:
        """Analyze productivity patterns."""
        insights = []
        hours = today.get('total_hours', 0)
        deep_work = today.get('deep_work_hours', 0)

        if not history:
            return insights

        avg_hours = sum(h.get('total_hours', 0) for h in history) / len(history)
        avg_dw = sum(h.get('deep_work_hours', 0) for h in history) / len(history)

        # Anomaly: unusually high work hours
        if len(history) >= 5:
            hours_list = [h.get('total_hours', 0) for h in history]
            stddev = self._stddev(hours_list)
            if stddev > 0 and hours > avg_hours + self.ANOMALY_STDDEV_THRESHOLD * stddev:
                insights.append(Insight(
                    user_id=user_id,
                    category='wellness',
                    title='Unusually long work day',
                    description=f'You worked {hours:.1f}h today, which is '
                                f'{hours - avg_hours:.1f}h above your average. '
                                'Consider wrapping up and resting.',
                    priority=0.8,
                    data={'hours': hours, 'avg_hours': avg_hours, 'stddev': stddev},
                ))

        # Deep work achievement
        if deep_work > avg_dw * 1.5 and deep_work >= 3:
            insights.append(Insight(
                user_id=user_id,
                category='productivity',
                title='Deep work breakthrough',
                description=f'You logged {deep_work:.1f}h of deep work today — '
                            f'{((deep_work/avg_dw-1)*100):.0f}% above your average!',
                priority=0.7,
                data={'deep_work': deep_work, 'avg_deep_work': avg_dw},
            ))

        return insights

    def _analyze_wellness(
        self,
        user_id: str,
        today: Dict[str, Any],
        history: List[Dict[str, Any]],
    ) -> List[Insight]:
        """Analyze wellness signals."""
        insights = []

        # Check for consistent long hours
        if len(history) >= 5:
            recent = history[-5:]
            long_days = sum(1 for h in recent if h.get('total_hours', 0) > 10)
            if long_days >= 3:
                insights.append(Insight(
                    user_id=user_id,
                    category='wellness',
                    title='Extended work pattern detected',
                    description=f'{long_days} out of last 5 days exceeded 10 hours. '
                                'This pattern may lead to burnout. Take a break!',
                    priority=0.9,
                    data={'long_days_count': long_days},
                    action={'label': 'View wellness dashboard', 'url': '/dashboard/wellness'},
                ))

        # Late-night work
        late_hours = today.get('late_night_hours', 0)
        if late_hours > 2:
            insights.append(Insight(
                user_id=user_id,
                category='wellness',
                title='Late-night work detected',
                description=f'You worked {late_hours:.1f}h after 10 PM. '
                            'Consistent late work affects sleep quality.',
                priority=0.7,
                data={'late_hours': late_hours},
            ))

        return insights

    def _detect_achievements(
        self,
        user_id: str,
        today: Dict[str, Any],
        history: List[Dict[str, Any]],
    ) -> List[Insight]:
        """Detect milestone achievements."""
        insights = []

        # Total tracked hours milestone
        total_career = today.get('total_career_hours', 0)
        milestones = [100, 250, 500, 1000, 2500, 5000]
        for milestone in milestones:
            if total_career >= milestone and total_career - today.get('total_hours', 0) < milestone:
                insights.append(Insight(
                    user_id=user_id,
                    category='achievement',
                    title=f'{milestone}h milestone reached!',
                    description=f'You\'ve tracked {total_career:.0f} hours total. '
                                f'Congratulations on your {milestone}h milestone!',
                    priority=0.9,
                    data={'milestone': milestone, 'total_hours': total_career},
                ))

        # Skills diversity
        skills_count = today.get('skills_used_today', 0)
        if skills_count >= 5:
            insights.append(Insight(
                user_id=user_id,
                category='learning',
                title='Diverse skill day',
                description=f'You worked with {skills_count} different skills today. '
                            'Great cross-domain engagement!',
                priority=0.5,
                data={'skills_count': skills_count},
            ))

        return insights

    def _analyze_collaboration(
        self,
        user_id: str,
        today: Dict[str, Any],
        history: List[Dict[str, Any]],
    ) -> List[Insight]:
        """Analyze collaboration patterns."""
        insights = []

        meetings = today.get('meeting_count', 0)
        meeting_hours = today.get('meeting_hours', 0)

        # Heavy meeting day
        if meeting_hours > 4:
            insights.append(Insight(
                user_id=user_id,
                category='collaboration',
                title='Meeting-heavy day',
                description=f'{meetings} meetings ({meeting_hours:.1f}h). '
                            'Tomorrow, try to protect the morning for deep work.',
                priority=0.6,
                data={'meeting_count': meetings, 'meeting_hours': meeting_hours},
            ))

        # No meetings — solo focus day
        if meetings == 0 and today.get('total_hours', 0) > 4:
            insights.append(Insight(
                user_id=user_id,
                category='focus',
                title='Uninterrupted focus day',
                description='Zero meetings today! Great opportunity for deep work.',
                priority=0.4,
                data={'meeting_count': 0},
            ))

        return insights

    # ========================================================================
    # UTILITY METHODS
    # ========================================================================

    def _count_streak(
        self,
        history: List[Dict[str, Any]],
        metric_key: str,
        threshold: float,
    ) -> int:
        """Count consecutive days where a metric exceeds a threshold (from most recent)."""
        streak = 0
        for entry in reversed(history):
            value = entry.get(metric_key, 0)
            if value >= threshold:
                streak += 1
            else:
                break
        return streak

    def _stddev(self, values: List[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return math.sqrt(variance)

    def _deduplicate(
        self,
        user_id: str,
        insights: List[Insight],
    ) -> List[Insight]:
        """Remove duplicate insights based on title and recency."""
        dismissed = self._dismissed_titles.get(user_id, set())
        seen_titles: Set[str] = set()
        deduplicated = []

        # Check recent insights for duplicates
        now = datetime.now(tz=None)
        cutoff = (now - timedelta(hours=self.DEDUP_HOURS)).isoformat()

        recent_titles = set()
        for insight in self._insights.get(user_id, []):
            if insight.created_at >= cutoff:
                recent_titles.add(insight.title)

        for insight in insights:
            if insight.title in dismissed:
                continue
            if insight.title in seen_titles:
                continue
            if insight.title in recent_titles:
                continue
            seen_titles.add(insight.title)
            deduplicated.append(insight)

        return deduplicated


# Global instance
proactive_insights_service = ProactiveInsightsService()
