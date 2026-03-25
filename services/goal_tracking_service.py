"""
Goal Tracking Service

Provides goal creation, tracking, and progress analytics:
- CRUD for goals (create, update, complete, archive)
- Progress tracking with milestones
- Goal categories (productivity, skill, project, wellness)
- Deadline management and reminders
- Goal completion analytics and streaks
- Auto-progress from activity data
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, date
from enum import Enum
import uuid
import structlog

logger = structlog.get_logger()


# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

class GoalStatus(str, Enum):
    ACTIVE = 'active'
    COMPLETED = 'completed'
    PAUSED = 'paused'
    ARCHIVED = 'archived'
    OVERDUE = 'overdue'


class GoalCategory(str, Enum):
    PRODUCTIVITY = 'productivity'
    SKILL = 'skill'
    PROJECT = 'project'
    WELLNESS = 'wellness'
    CAREER = 'career'
    CUSTOM = 'custom'


CATEGORY_ICONS = {
    'productivity': '📊',
    'skill': '📚',
    'project': '🚀',
    'wellness': '💚',
    'career': '🎯',
    'custom': '⭐',
}


class Goal:
    """Represents a single goal."""

    def __init__(
        self,
        user_id: str,
        title: str,
        category: str = 'custom',
        description: str = '',
        target_value: float = 100,
        current_value: float = 0,
        unit: str = 'percent',
        deadline: Optional[str] = None,
        milestones: Optional[List[Dict[str, Any]]] = None,
        goal_id: Optional[str] = None,
    ):
        self.id = goal_id or str(uuid.uuid4())
        self.user_id = user_id
        self.title = title
        self.category = category
        self.description = description
        self.target_value = target_value
        self.current_value = current_value
        self.unit = unit
        self.deadline = deadline
        self.milestones = milestones or []
        self.status = GoalStatus.ACTIVE
        self.created_at = datetime.now().isoformat()
        self.updated_at = self.created_at
        self.completed_at: Optional[str] = None
        self.progress_history: List[Dict[str, Any]] = []

    @property
    def progress_pct(self) -> float:
        if self.target_value == 0:
            return 100.0
        return min(round(self.current_value / self.target_value * 100, 1), 100)

    @property
    def is_overdue(self) -> bool:
        if not self.deadline or self.status == GoalStatus.COMPLETED:
            return False
        try:
            dl = datetime.fromisoformat(self.deadline)
            return datetime.now() > dl
        except Exception:
            return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'user_id': self.user_id,
            'title': self.title,
            'category': self.category,
            'category_icon': CATEGORY_ICONS.get(self.category, '⭐'),
            'description': self.description,
            'target_value': self.target_value,
            'current_value': self.current_value,
            'unit': self.unit,
            'progress_pct': self.progress_pct,
            'deadline': self.deadline,
            'milestones': self.milestones,
            'status': self.status.value if isinstance(self.status, GoalStatus) else self.status,
            'is_overdue': self.is_overdue,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'completed_at': self.completed_at,
        }


class GoalTrackingService:
    """
    Service for goal creation, tracking, and analytics.
    Uses in-memory store (swap for DB adapter later).
    """

    def __init__(self):
        self._goals: Dict[str, Dict[str, Goal]] = {}  # user_id -> {goal_id -> Goal}

    # ========================================================================
    # CRUD
    # ========================================================================

    def create_goal(
        self,
        user_id: str,
        title: str,
        category: str = 'custom',
        description: str = '',
        target_value: float = 100,
        unit: str = 'percent',
        deadline: Optional[str] = None,
        milestones: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Create a new goal."""
        goal = Goal(
            user_id=user_id,
            title=title,
            category=category,
            description=description,
            target_value=target_value,
            unit=unit,
            deadline=deadline,
            milestones=milestones,
        )

        if user_id not in self._goals:
            self._goals[user_id] = {}
        self._goals[user_id][goal.id] = goal

        logger.info('goal_created', user_id=user_id, goal_id=goal.id, category=category)
        return goal.to_dict()

    def get_goal(self, user_id: str, goal_id: str) -> Optional[Dict[str, Any]]:
        """Get a single goal by ID."""
        goals = self._goals.get(user_id, {})
        goal = goals.get(goal_id)
        if goal:
            self._check_overdue(goal)
            return goal.to_dict()
        return None

    def list_goals(
        self,
        user_id: str,
        status: Optional[str] = None,
        category: Optional[str] = None,
        include_archived: bool = False,
    ) -> Dict[str, Any]:
        """List goals with optional filtering."""
        goals = self._goals.get(user_id, {})

        result = []
        for goal in goals.values():
            self._check_overdue(goal)

            if not include_archived and goal.status == GoalStatus.ARCHIVED:
                continue
            if status and goal.status.value != status:
                continue
            if category and goal.category != category:
                continue

            result.append(goal.to_dict())

        # Sort: active first, then by deadline, then by creation date
        result.sort(key=lambda g: (
            0 if g['status'] == 'active' else 1,
            g.get('deadline') or '9999',
            g['created_at'],
        ))

        return {
            'goals': result,
            'total': len(result),
        }

    def update_goal(
        self,
        user_id: str,
        goal_id: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        target_value: Optional[float] = None,
        deadline: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Update goal metadata."""
        goals = self._goals.get(user_id, {})
        goal = goals.get(goal_id)
        if not goal:
            return None

        if title is not None:
            goal.title = title
        if description is not None:
            goal.description = description
        if target_value is not None:
            goal.target_value = target_value
        if deadline is not None:
            goal.deadline = deadline

        goal.updated_at = datetime.now().isoformat()
        return goal.to_dict()

    def delete_goal(self, user_id: str, goal_id: str) -> bool:
        """Delete a goal permanently."""
        goals = self._goals.get(user_id, {})
        if goal_id in goals:
            del goals[goal_id]
            return True
        return False

    # ========================================================================
    # PROGRESS TRACKING
    # ========================================================================

    def update_progress(
        self,
        user_id: str,
        goal_id: str,
        value: float,
        note: Optional[str] = None,
        auto: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Update goal progress.

        Args:
            user_id: User ID
            goal_id: Goal ID
            value: New current value (absolute, not delta)
            note: Optional note for this update
            auto: Whether this was an auto-update from activity data

        Returns:
            Updated goal dict or None
        """
        goals = self._goals.get(user_id, {})
        goal = goals.get(goal_id)
        if not goal:
            return None

        previous = goal.current_value
        goal.current_value = value
        goal.updated_at = datetime.now().isoformat()

        # Record in history
        goal.progress_history.append({
            'value': value,
            'previous': previous,
            'delta': value - previous,
            'timestamp': goal.updated_at,
            'note': note,
            'auto': auto,
        })

        # Check milestones
        newly_reached = self._check_milestones(goal, previous, value)

        # Auto-complete if target reached
        if value >= goal.target_value and goal.status == GoalStatus.ACTIVE:
            goal.status = GoalStatus.COMPLETED
            goal.completed_at = goal.updated_at
            logger.info('goal_completed', user_id=user_id, goal_id=goal_id)

        result = goal.to_dict()
        result['milestones_reached'] = newly_reached
        return result

    def add_progress_increment(
        self,
        user_id: str,
        goal_id: str,
        delta: float,
        note: Optional[str] = None,
        auto: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Increment progress by a delta value."""
        goals = self._goals.get(user_id, {})
        goal = goals.get(goal_id)
        if not goal:
            return None

        new_value = goal.current_value + delta
        return self.update_progress(user_id, goal_id, new_value, note=note, auto=auto)

    # ========================================================================
    # STATUS MANAGEMENT
    # ========================================================================

    def complete_goal(self, user_id: str, goal_id: str) -> Optional[Dict[str, Any]]:
        """Manually mark a goal as completed."""
        goals = self._goals.get(user_id, {})
        goal = goals.get(goal_id)
        if not goal:
            return None

        goal.status = GoalStatus.COMPLETED
        goal.completed_at = datetime.now().isoformat()
        goal.updated_at = goal.completed_at
        goal.current_value = goal.target_value
        return goal.to_dict()

    def pause_goal(self, user_id: str, goal_id: str) -> Optional[Dict[str, Any]]:
        """Pause a goal."""
        goals = self._goals.get(user_id, {})
        goal = goals.get(goal_id)
        if not goal:
            return None

        goal.status = GoalStatus.PAUSED
        goal.updated_at = datetime.now().isoformat()
        return goal.to_dict()

    def resume_goal(self, user_id: str, goal_id: str) -> Optional[Dict[str, Any]]:
        """Resume a paused goal."""
        goals = self._goals.get(user_id, {})
        goal = goals.get(goal_id)
        if not goal:
            return None

        goal.status = GoalStatus.ACTIVE
        goal.updated_at = datetime.now().isoformat()
        return goal.to_dict()

    def archive_goal(self, user_id: str, goal_id: str) -> Optional[Dict[str, Any]]:
        """Archive a goal."""
        goals = self._goals.get(user_id, {})
        goal = goals.get(goal_id)
        if not goal:
            return None

        goal.status = GoalStatus.ARCHIVED
        goal.updated_at = datetime.now().isoformat()
        return goal.to_dict()

    # ========================================================================
    # ANALYTICS
    # ========================================================================

    def get_goal_stats(self, user_id: str) -> Dict[str, Any]:
        """Get goal tracking statistics."""
        goals = list(self._goals.get(user_id, {}).values())

        if not goals:
            return {
                'total_goals': 0,
                'active': 0,
                'completed': 0,
                'paused': 0,
                'archived': 0,
                'overdue': 0,
                'completion_rate': 0,
                'by_category': {},
                'avg_progress': 0,
            }

        for g in goals:
            self._check_overdue(g)

        active = sum(1 for g in goals if g.status == GoalStatus.ACTIVE)
        completed = sum(1 for g in goals if g.status == GoalStatus.COMPLETED)
        paused = sum(1 for g in goals if g.status == GoalStatus.PAUSED)
        archived = sum(1 for g in goals if g.status == GoalStatus.ARCHIVED)
        overdue = sum(1 for g in goals if g.is_overdue)

        # By category
        by_category: Dict[str, int] = {}
        for g in goals:
            by_category[g.category] = by_category.get(g.category, 0) + 1

        # Average progress of active goals
        active_goals = [g for g in goals if g.status == GoalStatus.ACTIVE]
        avg_progress = (
            sum(g.progress_pct for g in active_goals) / len(active_goals)
            if active_goals else 0
        )

        # Completion rate
        finishable = completed + active + overdue
        completion_rate = completed / max(finishable, 1) * 100

        return {
            'total_goals': len(goals),
            'active': active,
            'completed': completed,
            'paused': paused,
            'archived': archived,
            'overdue': overdue,
            'completion_rate': round(completion_rate, 1),
            'by_category': by_category,
            'avg_progress': round(avg_progress, 1),
        }

    def get_completion_streaks(self, user_id: str) -> Dict[str, Any]:
        """Get goal completion streak data."""
        goals = list(self._goals.get(user_id, {}).values())

        completed = sorted(
            [g for g in goals if g.completed_at],
            key=lambda g: g.completed_at,
        )

        if not completed:
            return {
                'total_completed': 0,
                'current_streak': 0,
                'best_streak': 0,
                'recent_completions': [],
            }

        # Weekly streaks: how many consecutive weeks had at least one completion
        weeks_with_completions = set()
        for g in completed:
            try:
                dt = datetime.fromisoformat(g.completed_at)
                week_key = dt.isocalendar()[:2]  # (year, week_number)
                weeks_with_completions.add(week_key)
            except Exception:
                pass

        # Current + best streak
        sorted_weeks = sorted(weeks_with_completions)
        current_streak = 0
        best_streak = 0
        streak = 0
        prev = None

        for week_key in sorted_weeks:
            if prev is None:
                streak = 1
            else:
                # Check if consecutive: same year, week+1 or year transition
                if (week_key[0] == prev[0] and week_key[1] == prev[1] + 1) or \
                   (week_key[0] == prev[0] + 1 and prev[1] >= 52 and week_key[1] == 1):
                    streak += 1
                else:
                    streak = 1

            best_streak = max(best_streak, streak)
            prev = week_key

        current_streak = streak

        recent = [
            {'title': g.title, 'completed_at': g.completed_at, 'category': g.category}
            for g in completed[-5:]
        ]

        return {
            'total_completed': len(completed),
            'current_streak': current_streak,
            'best_streak': best_streak,
            'recent_completions': recent,
        }

    def get_upcoming_deadlines(
        self,
        user_id: str,
        days_ahead: int = 7,
    ) -> List[Dict[str, Any]]:
        """Get goals with upcoming deadlines."""
        goals = self._goals.get(user_id, {})
        now = datetime.now()
        cutoff = now + timedelta(days=days_ahead)

        upcoming = []
        for goal in goals.values():
            if goal.status != GoalStatus.ACTIVE or not goal.deadline:
                continue

            try:
                dl = datetime.fromisoformat(goal.deadline)
                if now <= dl <= cutoff:
                    days_left = (dl - now).days
                    upcoming.append({
                        **goal.to_dict(),
                        'days_remaining': days_left,
                        'urgency': 'critical' if days_left <= 1 else 'soon' if days_left <= 3 else 'upcoming',
                    })
            except Exception:
                continue

        return sorted(upcoming, key=lambda g: g.get('days_remaining', 999))

    # ========================================================================
    # AUTO-PROGRESS
    # ========================================================================

    def auto_update_from_activity(
        self,
        user_id: str,
        activity_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Auto-update goals based on activity data.

        Args:
            activity_data: {
                'hours_worked': float,
                'deep_work_hours': float,
                'skills_practiced': List[str],
                'focus_score': float,
                'projects_touched': List[str],
            }

        Returns:
            List of goals that were auto-updated
        """
        goals = self._goals.get(user_id, {})
        updates = []

        for goal in goals.values():
            if goal.status != GoalStatus.ACTIVE:
                continue

            delta = self._compute_auto_delta(goal, activity_data)
            if delta > 0:
                result = self.add_progress_increment(
                    user_id, goal.id, delta,
                    note='Auto-updated from activity data',
                    auto=True,
                )
                if result:
                    updates.append(result)

        return updates

    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================

    def _check_overdue(self, goal: Goal):
        """Update overdue status."""
        if goal.status == GoalStatus.ACTIVE and goal.is_overdue:
            goal.status = GoalStatus.OVERDUE

    def _check_milestones(
        self,
        goal: Goal,
        previous: float,
        current: float,
    ) -> List[Dict[str, Any]]:
        """Check if any milestones were crossed."""
        reached = []
        for ms in goal.milestones:
            threshold = ms.get('value', 0)
            if previous < threshold <= current:
                ms['reached'] = True
                ms['reached_at'] = datetime.now().isoformat()
                reached.append(ms)
        return reached

    def _compute_auto_delta(
        self,
        goal: Goal,
        activity_data: Dict[str, Any],
    ) -> float:
        """Compute auto-progress delta based on goal category and activity."""
        cat = goal.category

        if cat == 'productivity':
            # Count hours toward time-based goals
            hours = activity_data.get('hours_worked', 0)
            if goal.unit == 'hours':
                return hours
            elif goal.unit == 'percent':
                return hours / max(goal.target_value, 1) * 10

        elif cat == 'skill':
            # Check if the goal's skill was practiced
            skills = activity_data.get('skills_practiced', [])
            title_lower = goal.title.lower()
            for skill in skills:
                if skill.lower() in title_lower:
                    if goal.unit == 'hours':
                        return activity_data.get('deep_work_hours', 0)
                    return 5  # 5% per session

        elif cat == 'wellness':
            focus = activity_data.get('focus_score', 0)
            if focus >= 7 and goal.unit == 'percent':
                return 3  # 3% for a good focus day

        elif cat == 'project':
            projects = activity_data.get('projects_touched', [])
            title_lower = goal.title.lower()
            for p in projects:
                if p.lower() in title_lower:
                    hours = activity_data.get('hours_worked', 0)
                    if goal.unit == 'hours':
                        return hours
                    return 2

        return 0


# Global instance
goal_tracking_service = GoalTrackingService()
