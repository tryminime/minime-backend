"""
Milestone Celebrations Service

Detects and tracks user milestones/achievements:
- Activity count milestones (100, 500, 1000)
- Focus master (7-day streak)
- Entity milestones (10, 25, 50, 100)
- Time tracked milestones (100h, 500h, 1000h)
- Collaboration milestones
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import uuid
import structlog

from sqlalchemy.orm import Session
from sqlalchemy import select, func

logger = structlog.get_logger()


# Milestone definitions
MILESTONES = [
    # Activity count milestones
    {'id': 'activities_100', 'type': 'activity_count', 'threshold': 100,
     'emoji': '🎉', 'title': '100 Activities Tracked!', 'msg': 'You\'ve tracked your first 100 activities. Your digital twin is growing!'},
    {'id': 'activities_500', 'type': 'activity_count', 'threshold': 500,
     'emoji': '🚀', 'title': '500 Activities!', 'msg': 'Half a thousand activities tracked. MiniMe knows you well now!'},
    {'id': 'activities_1000', 'type': 'activity_count', 'threshold': 1000,
     'emoji': '🌟', 'title': '1,000 Activities!', 'msg': 'A thousand data points — your digital twin is truly taking shape.'},

    # Entity milestones
    {'id': 'entities_10', 'type': 'entity_count', 'threshold': 10,
     'emoji': '📊', 'title': '10 Knowledge Entities', 'msg': 'Your knowledge graph has 10 entities. Connections are forming!'},
    {'id': 'entities_25', 'type': 'entity_count', 'threshold': 25,
     'emoji': '🧩', 'title': '25 Knowledge Entities', 'msg': '25 entities in your knowledge graph. A rich web of connections!'},
    {'id': 'entities_50', 'type': 'entity_count', 'threshold': 50,
     'emoji': '🏗️', 'title': '50 Knowledge Entities', 'msg': '50 entities mapped — your expertise is well-documented!'},
    {'id': 'entities_100', 'type': 'entity_count', 'threshold': 100,
     'emoji': '🎓', 'title': '100 Knowledge Entities!', 'msg': 'A century of knowledge entities. You\'re a knowledge powerhouse!'},

    # Time tracked milestones
    {'id': 'hours_100', 'type': 'hours_tracked', 'threshold': 100,
     'emoji': '⏱️', 'title': '100 Hours Tracked', 'msg': 'You\'ve tracked 100 hours of activity. That\'s dedication!'},
    {'id': 'hours_500', 'type': 'hours_tracked', 'threshold': 500,
     'emoji': '⏰', 'title': '500 Hours Tracked!', 'msg': '500 hours of tracked activity. A true commitment to self-improvement!'},
    {'id': 'hours_1000', 'type': 'hours_tracked', 'threshold': 1000,
     'emoji': '🏆', 'title': '1,000 Hours Tracked!', 'msg': 'A thousand hours. Malcolm Gladwell would be proud!'},

    # Focus milestones
    {'id': 'focus_3day', 'type': 'focus_streak', 'threshold': 3,
     'emoji': '🎯', 'title': '3-Day Focus Streak', 'msg': '3 consecutive days of strong focus. Building the habit!'},
    {'id': 'focus_7day', 'type': 'focus_streak', 'threshold': 7,
     'emoji': '🏅', 'title': 'Focus Master — 7 Days!', 'msg': 'A full week of focused work. You\'re in the zone!'},
    {'id': 'focus_14day', 'type': 'focus_streak', 'threshold': 14,
     'emoji': '👑', 'title': 'Focus Champion — 14 Days!', 'msg': 'Two weeks of consistent focus. Extraordinary discipline!'},

    # App diversity
    {'id': 'apps_5', 'type': 'unique_apps', 'threshold': 5,
     'emoji': '🔧', 'title': '5 Tools Mastered', 'msg': 'You\'re using 5 different tools effectively.'},
    {'id': 'apps_10', 'type': 'unique_apps', 'threshold': 10,
     'emoji': '🛠️', 'title': '10 Tools in Your Arsenal', 'msg': 'A diverse toolkit of 10 applications. Versatile!'},
]


async def check_milestones(user_id: str, db: Session) -> Dict[str, Any]:
    """
    Check all milestones for a user and return newly unlocked ones.

    Returns both unlocked and locked milestones with progress percentages.
    """
    from models import Activity, Entity

    try:
        user_uuid = uuid.UUID(str(user_id))
    except Exception:
        return {'milestones': [], 'unlocked': [], 'total_unlocked': 0}

    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    two_weeks_ago = now - timedelta(days=14)

    # ── Gather metrics ───────────────────────────────────────────────────

    # Activity count
    activity_count_result = db.execute(
        select(func.count()).select_from(Activity).where(
            Activity.user_id == user_uuid,
        )
    )
    activity_count = activity_count_result.scalar() or 0

    # Entity count
    entity_count_result = db.execute(
        select(func.count()).select_from(Entity).where(
            Entity.user_id == user_uuid,
        )
    )
    entity_count = entity_count_result.scalar() or 0

    # Total hours tracked
    hours_result = db.execute(
        select(func.sum(Activity.duration_seconds)).where(
            Activity.user_id == user_uuid,
        )
    )
    total_seconds = hours_result.scalar() or 0
    total_hours = total_seconds / 3600

    # Unique apps
    apps_result = db.execute(
        select(func.count(func.distinct(Activity.app))).where(
            Activity.user_id == user_uuid,
            Activity.app.isnot(None),
        )
    )
    unique_apps = apps_result.scalar() or 0

    # Focus streak — count consecutive days with focus score > 7
    result = db.execute(
        select(Activity).where(
            Activity.user_id == user_uuid,
            Activity.occurred_at >= two_weeks_ago,
        ).order_by(Activity.occurred_at.desc())
    )
    recent_activities = result.scalars().all()

    focus_streak = 0
    if recent_activities:
        daily_focus: Dict[str, float] = defaultdict(float)
        daily_total: Dict[str, float] = defaultdict(float)
        for a in recent_activities:
            if a.occurred_at:
                day_key = a.occurred_at.strftime('%Y-%m-%d')
                daily_total[day_key] += a.duration_seconds or 0
                if a.type in ('window_focus', 'app_focus') and (a.duration_seconds or 0) >= 600:
                    daily_focus[day_key] += a.duration_seconds or 0

        # Walk backwards from today counting streak days
        check_date = now.date()
        for _ in range(14):
            day_key = check_date.strftime('%Y-%m-%d')
            total = daily_total.get(day_key, 0)
            focused = daily_focus.get(day_key, 0)
            if total > 0:
                score = min(10.0, (focused / max(total, 1)) * 10)
                if score >= 7.0:
                    focus_streak += 1
                    check_date -= timedelta(days=1)
                else:
                    break
            else:
                break

    # ── Check milestones ─────────────────────────────────────────────────

    metrics = {
        'activity_count': activity_count,
        'entity_count': entity_count,
        'hours_tracked': total_hours,
        'focus_streak': focus_streak,
        'unique_apps': unique_apps,
    }

    all_milestones = []
    unlocked = []

    for m in MILESTONES:
        current_value = metrics.get(m['type'], 0)
        is_unlocked = current_value >= m['threshold']
        progress = min(100, (current_value / max(m['threshold'], 1)) * 100)

        milestone_data = {
            'id': m['id'],
            'type': m['type'],
            'emoji': m['emoji'],
            'title': m['title'],
            'message': m['msg'],
            'threshold': m['threshold'],
            'current_value': round(current_value, 1) if isinstance(current_value, float) else current_value,
            'progress': round(progress, 1),
            'unlocked': is_unlocked,
        }

        all_milestones.append(milestone_data)
        if is_unlocked:
            unlocked.append(milestone_data)

    return {
        'milestones': all_milestones,
        'unlocked': unlocked,
        'total_unlocked': len(unlocked),
        'total_milestones': len(MILESTONES),
        'metrics': {k: round(v, 1) if isinstance(v, float) else v for k, v in metrics.items()},
    }
