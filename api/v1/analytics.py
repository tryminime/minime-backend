"""
Analytics API endpoints — REAL DATA from activities table.
Computes productivity, focus, and overview metrics from actual user activities.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, date, timezone
from sqlalchemy import select, func, cast, String, case
from sqlalchemy.ext.asyncio import AsyncSession
import structlog
import uuid as uuid_lib

from database.postgres import get_db
from models import Activity, Entity
from auth.jwt_handler import decode_token, verify_token_type

logger = structlog.get_logger()
router = APIRouter()
security = HTTPBearer()


# =====================================================
# HELPERS
# =====================================================

def _get_user_id(credentials) -> str:
    """Extract user_id from JWT token."""
    token = credentials.credentials
    payload = decode_token(token)
    if not payload or not verify_token_type(payload, "access"):
        raise HTTPException(status_code=401, detail="Invalid token")
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    return user_id


# Canonical set of productive activity types used across all analytics
PRODUCTIVE_TYPES = {"window_focus", "app_focus", "web_visit", "page_view"}

# Minimum session duration (in seconds) to count as "focused work"
FOCUS_THRESHOLD_SECONDS = 120  # 2 minutes


def _compute_focus_score(
    activities,
    total_seconds: float,
    *,
    cap_fn=None,
) -> float:
    """
    Compute focus score consistently across all endpoints.

    Focus score = (focused_seconds / total_seconds) * 100, capped at 100.

    For non-web_visit types: an activity counts as "focused" if its duration
    >= FOCUS_THRESHOLD_SECONDS.

    For web_visit types: we aggregate durations per domain first, then
    apply the threshold to the per-domain total. This handles the common
    pattern of many rapid short page visits to the same site.
    """
    def _dur(a):
        if cap_fn:
            return cap_fn(a)
        return a.duration_seconds or 0

    focused_seconds = 0.0

    # Non-web_visit: individual session threshold
    for a in activities:
        if a.type in PRODUCTIVE_TYPES and a.type != "web_visit":
            d = _dur(a)
            if d >= FOCUS_THRESHOLD_SECONDS:
                focused_seconds += d

    # web_visit: aggregate per domain, then apply threshold
    domain_seconds: Dict[str, float] = {}
    for a in activities:
        if a.type == "web_visit":
            key = a.domain or "unknown"
            domain_seconds[key] = domain_seconds.get(key, 0) + _dur(a)
    for domain_total in domain_seconds.values():
        if domain_total >= FOCUS_THRESHOLD_SECONDS:
            focused_seconds += domain_total

    return min(100.0, (focused_seconds / max(total_seconds, 1)) * 100)


def _format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{int(seconds)}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _time_ago(dt: datetime) -> str:
    """Convert datetime to relative time string."""
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    diff = now - dt
    seconds = diff.total_seconds()
    if seconds < 60:
        return "just now"
    minutes = int(seconds / 60)
    if minutes < 60:
        return f"{minutes}m ago"
    hours = int(minutes / 60)
    if hours < 24:
        return f"{hours}h ago"
    days = int(hours / 24)
    return f"{days}d ago"


# =====================================================
# RESPONSE MODELS
# =====================================================

class DashboardOverview(BaseModel):
    total_activities: int = 0
    total_hours: float = 0.0
    focus_score: float = 0.0
    deep_work_hours: float = 0.0
    meetings_count: int = 0
    breaks_count: int = 0
    top_apps: List[Dict[str, Any]] = []
    recent_activities: List[Dict[str, Any]] = []
    activity_types: Dict[str, int] = {}


class ProductivityDaily(BaseModel):
    date: str = ""
    total_seconds: int = 0
    focus_score: float = 0.0
    productivity_score: float = 0.0
    deep_work_sessions: int = 0
    deep_work_hours: float = 0.0
    meeting_load_hours: float = 0.0
    context_switches: int = 0
    activity_count: int = 0


class ProductivityWeekly(BaseModel):
    week_start: str = ""
    week_end: str = ""
    avg_focus_score: float = 0.0
    avg_productivity_score: float = 0.0
    total_deep_work_hours: float = 0.0
    total_meeting_hours: float = 0.0
    total_activities: int = 0
    daily_metrics: List[ProductivityDaily] = []


class ProductivityMetrics(BaseModel):
    total_hours: float = 0.0
    productive_hours: float = 0.0
    productivity_ratio: float = 0.0
    deep_work_hours: float = 0.0
    context_switches: int = 0
    focus_score: float = 0.0
    top_apps: List[Dict[str, Any]] = []
    time_allocation: Dict[str, Any] = {}
    comparison: Dict[str, Any] = {}


class CollaborationMetrics(BaseModel):
    collaboration_score: float = 0.0
    unique_collaborators: int = 0
    meetings_count: int = 0
    communication_volume: int = 0
    network_size: int = 0
    top_collaborators: List[Dict[str, Any]] = []
    network_diversity: Dict[str, Any] = {}
    meeting_patterns: Dict[str, Any] = {}


class SkillItem(BaseModel):
    name: str
    category: str
    mastery: float = 0.0
    time_invested_hours: float = 0.0
    last_used: str = ""
    growth_rate: float = 0.0


class SkillRecommendation(BaseModel):
    name: str
    reason: str
    estimated_time_hours: float = 10.0
    difficulty: str = "intermediate"


class SkillGrowthEntry(BaseModel):
    date: str
    skill_name: str
    mastery: float


class SkillMetrics(BaseModel):
    total_skills: int = 0
    advanced_skills: int = 0
    skill_diversity: float = 0.0
    learning_velocity: float = 0.0
    top_skills: List[SkillItem] = []
    recommended_skills: List[SkillRecommendation] = []
    growth_history: List[SkillGrowthEntry] = []


class CareerInsights(BaseModel):
    growth_trajectory: str = ""
    career_phase: str = ""
    skill_gaps: List[Any] = []
    recommended_next_steps: List[str] = []
    best_fit_role: Dict[str, Any] = {}
    milestone: Dict[str, Any] = {}


class WellnessMetrics(BaseModel):
    overall_score: float = 0.0
    work_life_balance: Dict[str, Any] = {}
    burnout_risk: Dict[str, Any] = {}
    rest_recovery: Dict[str, Any] = {}
    energy_levels: Dict[str, Any] = {}


class Goal(BaseModel):
    id: str
    title: str
    category: str  # focus | productivity | learning | wellness | custom
    target_value: float
    current_value: float = 0.0
    unit: str  # hours | sessions | points | %
    deadline: Optional[str] = None
    status: str = "active"  # active | completed | paused
    streak_count: int = 0
    created_at: str = ""


class GoalCreate(BaseModel):
    title: str
    category: str
    target_value: float
    unit: str
    deadline: Optional[str] = None


class GoalUpdate(BaseModel):
    title: Optional[str] = None
    target_value: Optional[float] = None
    current_value: Optional[float] = None
    status: Optional[str] = None



class SummaryStats(BaseModel):
    total_activities: int = 0
    focus_score: float = 0.0
    productivity_score: float = 0.0
    collaboration_index: float = 0.0
    top_skills: List[str] = []
    key_achievements: List[str] = []


class WeeklySummary(BaseModel):
    week_start: str = ""
    week_end: str = ""
    html_content: str = ""
    summary_stats: SummaryStats = SummaryStats()


# =====================================================
# REAL DATA COMPUTATION
# =====================================================

async def _compute_daily_metrics(
    db: AsyncSession, user_id: str, target_date: date
) -> ProductivityDaily:
    """Compute real productivity metrics for a given date from activities table."""
    start_dt = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
            Activity.occurred_at < end_dt,
        ).order_by(Activity.occurred_at)
    )
    activities = result.scalars().all()

    if not activities:
        return ProductivityDaily(date=target_date.isoformat())

    total_count = len(activities)

    # Cap each activity's duration at 2 hours to prevent runaway totals
    MAX_ACTIVITY_DURATION = 7200  # 2 hours
    def capped(a):
        return min(a.duration_seconds or 0, MAX_ACTIVITY_DURATION)

    raw_total = sum(capped(a) for a in activities)
    # Cap daily total at 24 hours
    total_seconds = min(raw_total, 86400)

    # All productive types (include web_visit since that's our main data source)
    productive_types = {"window_focus", "app_focus", "web_visit", "page_view"}

    # Deep work: sessions >= 5 minutes on productive types
    # For web_visit: aggregate per domain (rapid short visits = one session)
    deep_work_sessions = 0
    deep_work_seconds = 0.0

    # Non-web_visit: individual session check
    for a in activities:
        if a.type in productive_types and a.type != "web_visit":
            d = capped(a)
            if d >= 300:
                deep_work_sessions += 1
                deep_work_seconds += d

    # web_visit: aggregate per domain, then check threshold
    domain_totals: Dict[str, float] = {}
    for a in activities:
        if a.type == "web_visit":
            key = a.domain or "unknown"
            domain_totals[key] = domain_totals.get(key, 0) + capped(a)
    for dtotal in domain_totals.values():
        if dtotal >= 300:
            deep_work_sessions += 1
            deep_work_seconds += dtotal

    deep_work_hours = deep_work_seconds / 3600.0

    # Focus score (unified helper)
    focus_score = _compute_focus_score(activities, total_seconds, cap_fn=capped)

    # Productivity score: productive types / total
    productive_seconds = sum(capped(a) for a in activities if a.type in productive_types)
    productivity_score = min(100.0, (productive_seconds / max(total_seconds, 1)) * 100)

    # Meeting load: type == "meeting" OR known meeting domains
    MEETING_DOMAINS = {"zoom.us", "meet.google.com", "teams.microsoft.com",
                       "webex.com", "whereby.com", "cal.com"}
    meeting_activities = [
        a for a in activities
        if a.type == "meeting"
        or (a.domain and any(md in (a.domain or "") for md in MEETING_DOMAINS))
    ]
    meeting_hours = sum(capped(a) for a in meeting_activities) / 3600.0

    # Context switches: count transitions between distinct app/domain combos
    context_switches = 0
    prev_key = None
    for a in activities:
        key = a.app or a.domain or a.type
        if key and key != prev_key:
            if prev_key is not None:
                context_switches += 1
            prev_key = key

    return ProductivityDaily(
        date=target_date.isoformat(),
        total_seconds=total_seconds,
        focus_score=round(focus_score, 1),
        productivity_score=round(productivity_score, 1),
        deep_work_sessions=deep_work_sessions,
        deep_work_hours=round(deep_work_hours, 2),
        meeting_load_hours=round(meeting_hours, 1),
        context_switches=context_switches,
        activity_count=total_count,
    )


# =====================================================
# ENDPOINTS
# =====================================================

@router.get("/overview", response_model=DashboardOverview)
async def get_dashboard_overview(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Real dashboard overview computed from activities in the database.
    """
    user_id = _get_user_id(credentials)

    # Get all activities for this user (last 30 days)
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)

    # True total count (no LIMIT)
    count_result = await db.execute(
        select(func.count(Activity.id)).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= thirty_days_ago,
        )
    )
    true_total_activities = count_result.scalar() or 0

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= thirty_days_ago,
        ).order_by(Activity.occurred_at.desc()).limit(200)
    )
    activities = result.scalars().all()

    if not activities:
        return DashboardOverview()

    # Total hours
    total_seconds = sum(a.duration_seconds or 0 for a in activities)
    total_hours = total_seconds / 3600.0

    # Focus score (unified helper)
    focus_score = _compute_focus_score(activities, total_seconds)

    # Deep work hours (sessions >= 25 min)
    deep_work = [a for a in activities if a.type in ("window_focus", "app_focus") and (a.duration_seconds or 0) >= 300]
    deep_work_hours = sum(a.duration_seconds or 0 for a in deep_work) / 3600.0

    # Meetings
    meetings = [a for a in activities if a.type == "meeting"]
    meetings_count = len(meetings)

    # Breaks
    breaks = [a for a in activities if a.type == "break"]
    breaks_count = len(breaks)

    # Top apps
    app_time: Dict[str, float] = {}
    for a in activities:
        if a.app:
            app_time[a.app] = app_time.get(a.app, 0) + (a.duration_seconds or 0)
    top_apps = sorted(
        [{"app": k, "hours": round(v / 3600.0, 1), "duration": _format_duration(v)} for k, v in app_time.items()],
        key=lambda x: x["hours"],
        reverse=True,
    )[:5]

    # Recent activities
    recent = []
    for a in activities[:10]:
        title = a.title or a.app or a.type
        recent.append({
            "type": a.type,
            "title": title,
            "app": a.app or "",
            "duration": _format_duration(a.duration_seconds or 0),
            "duration_seconds": a.duration_seconds or 0,
            "time_ago": _time_ago(a.occurred_at) if a.occurred_at else "",
            "occurred_at": a.occurred_at.isoformat() if a.occurred_at else "",
            "domain": a.domain or "",
        })

    # Activity types distribution
    type_counts: Dict[str, int] = {}
    for a in activities:
        type_counts[a.type] = type_counts.get(a.type, 0) + 1

    return DashboardOverview(
        total_activities=true_total_activities,
        total_hours=round(total_hours, 1),
        focus_score=round(focus_score, 1),
        deep_work_hours=round(deep_work_hours, 1),
        meetings_count=meetings_count,
        breaks_count=breaks_count,
        top_apps=top_apps,
        recent_activities=recent,
        activity_types=type_counts,
    )


@router.get("/productivity/daily", response_model=ProductivityDaily)
async def get_productivity_daily(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
):
    """
    Real daily productivity metrics computed from activity data.
    """
    user_id = _get_user_id(credentials)
    if date:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    else:
        target_date = datetime.now(timezone.utc).date()

    return await _compute_daily_metrics(db, user_id, target_date)


@router.get("/productivity/daily-range")
async def get_productivity_daily_range(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    days: int = Query(30, description="Number of days to return"),
):
    """Return daily metrics for a range of days (for heatmap).

    Uses a single DB query for the entire range instead of N separate queries.
    """
    user_id = _get_user_id(credentials)
    today = datetime.now(timezone.utc).date()
    range_start = today - timedelta(days=days - 1)

    start_dt = datetime.combine(range_start, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(today + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc)

    # Single query for the entire range
    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
            Activity.occurred_at < end_dt,
        ).order_by(Activity.occurred_at)
    )
    all_activities = result.scalars().all()

    # Group activities by date
    by_date: Dict[str, list] = {}
    for a in all_activities:
        day_key = a.occurred_at.date().isoformat() if a.occurred_at else None
        if day_key:
            by_date.setdefault(day_key, []).append(a)

    # Compute metrics per day using same logic as _compute_daily_metrics
    MAX_ACTIVITY_DURATION = 7200
    productive_types = {"window_focus", "app_focus", "web_visit", "page_view"}
    MEETING_DOMAINS = {"zoom.us", "meet.google.com", "teams.microsoft.com",
                       "webex.com", "whereby.com", "cal.com"}

    def capped(a):
        return min(a.duration_seconds or 0, MAX_ACTIVITY_DURATION)

    metrics = []
    for i in range(days):
        day = range_start + timedelta(days=i)
        day_key = day.isoformat()
        activities = by_date.get(day_key, [])

        if not activities:
            metrics.append(ProductivityDaily(date=day_key))
            continue

        total_count = len(activities)
        raw_total = sum(capped(a) for a in activities)
        total_seconds = min(raw_total, 86400)

        deep_work_activities = [
            a for a in activities
            if a.type in productive_types and capped(a) >= 300
        ]
        deep_work_sessions = len(deep_work_activities)
        deep_work_hours = sum(capped(a) for a in deep_work_activities) / 3600.0

        focus_score = _compute_focus_score(activities, total_seconds, cap_fn=capped)

        productive_seconds = sum(capped(a) for a in activities if a.type in productive_types)
        productivity_score = min(100.0, (productive_seconds / max(total_seconds, 1)) * 100)

        meeting_activities = [
            a for a in activities
            if a.type == "meeting"
            or (a.domain and any(md in (a.domain or "") for md in MEETING_DOMAINS))
        ]
        meeting_hours = sum(capped(a) for a in meeting_activities) / 3600.0

        context_switches = 0
        prev_key = None
        for a in activities:
            key = a.app or a.domain or a.type
            if key and key != prev_key:
                if prev_key is not None:
                    context_switches += 1
                prev_key = key

        metrics.append(ProductivityDaily(
            date=day_key,
            total_seconds=total_seconds,
            focus_score=round(focus_score, 1),
            productivity_score=round(productivity_score, 1),
            deep_work_sessions=deep_work_sessions,
            deep_work_hours=round(deep_work_hours, 2),
            meeting_load_hours=round(meeting_hours, 1),
            context_switches=context_switches,
            activity_count=total_count,
        ))

    return {"metrics": metrics}


@router.get("/productivity/weekly", response_model=ProductivityWeekly)
async def get_productivity_weekly(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Real weekly productivity metrics — aggregates 7 days of daily metrics.
    """
    user_id = _get_user_id(credentials)
    today = datetime.now(timezone.utc).date()
    week_start = today - timedelta(days=today.weekday())  # Monday
    week_end = week_start + timedelta(days=6)

    daily_metrics = []
    for i in range(7):
        day = week_start + timedelta(days=i)
        m = await _compute_daily_metrics(db, user_id, day)
        daily_metrics.append(m)

    # Aggregate
    days_with_data = [m for m in daily_metrics if m.activity_count > 0]
    count = len(days_with_data) or 1

    avg_focus = sum(m.focus_score for m in days_with_data) / count if days_with_data else 0
    avg_prod = sum(m.productivity_score for m in days_with_data) / count if days_with_data else 0
    total_deep = sum(m.deep_work_hours for m in daily_metrics)
    total_meet = sum(m.meeting_load_hours for m in daily_metrics)
    total_acts = sum(m.activity_count for m in daily_metrics)

    return ProductivityWeekly(
        week_start=week_start.isoformat(),
        week_end=week_end.isoformat(),
        avg_focus_score=round(avg_focus, 1),
        avg_productivity_score=round(avg_prod, 1),
        total_deep_work_hours=round(total_deep, 1),
        total_meeting_hours=round(total_meet, 1),
        total_activities=total_acts,
        daily_metrics=daily_metrics,
    )


@router.get("/productivity", response_model=ProductivityMetrics)
async def get_productivity_metrics(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Real productivity metrics from activity data."""
    user_id = _get_user_id(credentials)

    # Default to last 7 days
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=7)

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        except ValueError:
            pass

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
            Activity.occurred_at < end_dt,
        )
    )
    activities = result.scalars().all()

    if not activities:
        return ProductivityMetrics()

    MAX_ACTIVITY_DURATION = 7200  # 2h cap per activity
    def capped(a):
        return min(a.duration_seconds or 0, MAX_ACTIVITY_DURATION)

    raw_total = sum(capped(a) for a in activities)
    total_seconds = min(raw_total, 86400 * 7)  # Cap at 7 × 24h for weekly view
    total_hours = total_seconds / 3600.0

    productive_types = {"window_focus", "app_focus", "web_visit", "page_view"}
    productive_seconds = sum(capped(a) for a in activities if a.type in productive_types)
    productive_hours = productive_seconds / 3600.0

    deep_work = [a for a in activities if a.type in productive_types and capped(a) >= 300]
    deep_work_hours = sum(capped(a) for a in deep_work) / 3600.0

    focused = [a for a in activities if a.type in productive_types and capped(a) >= 300]
    focused_seconds = sum(capped(a) for a in focused)
    focus_score = min(100.0, (focused_seconds / max(total_seconds, 1)) * 100)

    # Context switches: transitions between app/domain combos
    context_switches = 0
    prev_key = None
    for a in sorted(activities, key=lambda x: x.occurred_at or datetime.min.replace(tzinfo=timezone.utc)):
        key = a.app or a.domain or a.type
        if key and key != prev_key:
            if prev_key is not None:
                context_switches += 1
            prev_key = key

    # Top apps — merge app name + domain for a unified view
    app_time: Dict[str, float] = {}
    for a in activities:
        label = a.app or a.domain or a.type
        if label:
            app_time[label] = app_time.get(label, 0) + capped(a)
    top_apps = sorted(
        [{"app": k, "hours": round(v / 3600, 2)} for k, v in app_time.items()],
        key=lambda x: x["hours"], reverse=True,
    )[:10]

    # Time allocation by app/domain (not raw type)
    time_allocation = {k: round(v / 3600, 2) for k, v in app_time.items() if v > 0}

    # --- Week-over-week comparison (previous 7-day window) ---
    prev_start_dt = start_dt - timedelta(days=7)
    prev_end_dt = start_dt
    prev_result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= prev_start_dt,
            Activity.occurred_at < prev_end_dt,
        )
    )
    prev_activities = prev_result.scalars().all()

    comparison: Dict[str, float] = {}
    if prev_activities:
        prev_total_s = min(sum(capped(a) for a in prev_activities), 86400 * 7)
        prev_total_h = prev_total_s / 3600

        prev_deep = [a for a in prev_activities if a.type in productive_types and capped(a) >= 300]
        prev_deep_h = sum(capped(a) for a in prev_deep) / 3600

        prev_focused = [a for a in prev_activities if a.type in productive_types and capped(a) >= 300]
        prev_focused_s = sum(capped(a) for a in prev_focused)
        prev_focus_score = min(100.0, (prev_focused_s / max(prev_total_s, 1)) * 100)

        prev_ctx = 0
        prev_key2 = None
        for a in sorted(prev_activities, key=lambda x: x.occurred_at or datetime.min.replace(tzinfo=timezone.utc)):
            key = a.app or a.domain or a.type
            if key and key != prev_key2:
                if prev_key2 is not None:
                    prev_ctx += 1
                prev_key2 = key

        comparison = {
            "total_hours": round(total_hours - prev_total_h, 1),
            "deep_work_hours": round(deep_work_hours - prev_deep_h, 1),
            "focus_score": round(focus_score - prev_focus_score, 1),
            "context_switches": context_switches - prev_ctx,
        }

    return ProductivityMetrics(
        total_hours=round(total_hours, 1),
        productive_hours=round(productive_hours, 1),
        productivity_ratio=round(productive_hours / max(total_hours, 0.01), 2),
        deep_work_hours=round(deep_work_hours, 1),
        context_switches=context_switches,
        focus_score=round(focus_score, 1),
        top_apps=top_apps,
        time_allocation=time_allocation,
        comparison=comparison,
    )


@router.get("/collaboration", response_model=CollaborationMetrics)
async def get_collaboration_metrics(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Collaboration metrics from meeting/communication activities."""
    user_id = _get_user_id(credentials)

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=7)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
            Activity.occurred_at < end_dt,
        )
    )
    activities = result.scalars().all()

    meetings = [a for a in activities if a.type == "meeting"]
    # Widen the communication signal: include Chrome/Firefox on communication domains,
    # Slack, Teams, Discord, Zoom, Google Meet, and any app that contains comm keywords
    comm_apps = {"Slack", "Teams", "Discord", "Zoom", "Google Meet", "Google Chat", "Outlook", "Mail"}
    comm_domains = {"slack.com", "teams.microsoft.com", "discord.com", "zoom.us", "meet.google.com",
                    "gmail.com", "mail.google.com", "outlook.com", "calendar.google.com"}
    communication = [
        a for a in activities
        if a.app in comm_apps
        or (a.domain and any(d in (a.domain or "") for d in comm_domains))
        or a.type == "meeting"
        or (a.app and any(kw in (a.app or "").lower() for kw in ["slack", "teams", "discord", "zoom", "meet", "mail"]))
    ]

    # Build top_collaborators from named persons in activity data
    collab_counts: Dict[str, int] = {}
    for a in meetings:
        meta = a.data or {}
        participants = meta.get("participants", [])
        for p in participants:
            name = p if isinstance(p, str) else p.get("name", "")
            if name:
                collab_counts[name] = collab_counts.get(name, 0) + 1
        organizer = meta.get("organizer", "")
        if organizer and organizer not in collab_counts:
            collab_counts[organizer] = collab_counts.get(organizer, 0) + 1

    # ── Also pull person entities from enrichment pipeline ──────────────
    person_result = await db.execute(
        select(Entity).where(
            Entity.user_id == uuid_lib.UUID(user_id),
            Entity.entity_type == "person",
        ).order_by(Entity.occurrence_count.desc())
    )
    person_entities = person_result.scalars().all()
    for pe in person_entities:
        name = pe.name.strip()
        if name and name not in collab_counts:
            collab_counts[name] = pe.occurrence_count or 1

    top_collaborators = [
        {"name": name, "interaction_count": count, "email": None}
        for name, count in sorted(collab_counts.items(), key=lambda x: -x[1])[:10]
    ]

    # Score: communication volume drives the index — exclude meetings already in communication list
    non_meeting_comms = [a for a in communication if a.type != "meeting"]
    total_collab = len(meetings) + len(non_meeting_comms)
    collab_score = min(100.0, max(10.0, total_collab * 4.0)) if total_collab > 0 else 0.0
    net_size = max(len(collab_counts), len(set(a.app for a in communication if a.app)), len(meetings))
    total_unique_collaborators = len(collab_counts)

    return CollaborationMetrics(
        collaboration_score=round(collab_score, 1),
        meetings_count=len(meetings),
        communication_volume=len(communication),
        unique_collaborators=total_unique_collaborators,
        network_size=max(net_size, total_unique_collaborators),
        top_collaborators=top_collaborators,
        network_diversity={"apps": len(set(a.app for a in communication if a.app))},
        meeting_patterns={"total_hours": round(sum(a.duration_seconds or 0 for a in meetings) / 3600, 1)},
    )


@router.get("/skills", response_model=SkillMetrics)
async def get_skill_metrics(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Skill metrics derived from app usage patterns."""
    user_id = _get_user_id(credentials)

    # Limit to last 90 days for performance
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= cutoff,
        ).limit(500)
    )
    activities = result.scalars().all()

    # Derive skills from app usage
    app_time: Dict[str, float] = {}
    app_last_used: Dict[str, str] = {}
    for a in activities:
        if a.app:
            app_time[a.app] = app_time.get(a.app, 0) + (a.duration_seconds or 0)
            ts = a.occurred_at.isoformat() if a.occurred_at else ""
            if ts > app_last_used.get(a.app, ""):
                app_last_used[a.app] = ts

    skill_map = {
        # Development
        "VS Code": ("Programming", "Development"),
        "Code": ("Programming", "Development"),
        "IntelliJ": ("Programming", "Development"),
        "PyCharm": ("Programming", "Development"),
        "WebStorm": ("Programming", "Development"),
        "Xcode": ("Programming", "Development"),
        "Android Studio": ("Programming", "Development"),
        "Terminal": ("DevOps", "Development"),
        "iTerm": ("DevOps", "Development"),
        "Docker": ("Containers", "Development"),
        "Postman": ("API Testing", "Development"),
        "GitHub": ("Version Control", "Development"),
        "GitLab": ("Version Control", "Development"),
        # Browsers & Research
        "Chrome": ("Research", "Knowledge"),
        "Google Chrome": ("Research", "Knowledge"),
        "Firefox": ("Research", "Knowledge"),
        "Safari": ("Research", "Knowledge"),
        "Edge": ("Research", "Knowledge"),
        # Design
        "Figma": ("Design", "Creative"),
        "Sketch": ("Design", "Creative"),
        "Canva": ("Design", "Creative"),
        "Adobe Photoshop": ("Image Editing", "Creative"),
        "Adobe Illustrator": ("Vector Design", "Creative"),
        # Communication
        "Slack": ("Communication", "Collaboration"),
        "Discord": ("Communication", "Collaboration"),
        "Microsoft Teams": ("Communication", "Collaboration"),
        "Zoom": ("Meetings", "Collaboration"),
        "Google Meet": ("Meetings", "Collaboration"),
        # Productivity & Docs
        "Notion": ("Documentation", "Knowledge"),
        "Obsidian": ("Note-taking", "Knowledge"),
        "Google Docs": ("Writing", "Knowledge"),
        "Google Sheets": ("Spreadsheets", "Analytics"),
        "Microsoft Word": ("Writing", "Knowledge"),
        "Microsoft Excel": ("Spreadsheets", "Analytics"),
        "Microsoft PowerPoint": ("Presentations", "Communication"),
        "Google Slides": ("Presentations", "Communication"),
        # Finance & Data
        "Tableau": ("Data Visualization", "Analytics"),
        "Power BI": ("Business Intelligence", "Analytics"),
        # Desktop / System
        "Files": ("File Management", "System"),
        "Finder": ("File Management", "System"),
        "Nautilus": ("File Management", "System"),
        "Document Viewer": ("Documents", "Knowledge"),
        "Evince": ("Documents", "Knowledge"),
        "Preview": ("Documents", "Knowledge"),
        "Text Editor": ("Text Editing", "Development"),
        "gedit": ("Text Editing", "Development"),
        "Sublime Text": ("Text Editing", "Development"),
        "Notepad++": ("Text Editing", "Development"),
        "nano": ("Text Editing", "Development"),
        "vim": ("Text Editing", "Development"),
        "Spyder": ("Data Science", "Analytics"),
        "Jupyter": ("Data Science", "Analytics"),
        "RStudio": ("Data Science", "Analytics"),
        "LibreOffice": ("Office Suite", "Knowledge"),
        "LibreOffice Writer": ("Writing", "Knowledge"),
        "LibreOffice Calc": ("Spreadsheets", "Analytics"),
        "LibreOffice Impress": ("Presentations", "Communication"),
        # Media
        "Spotify": ("Music", "Wellness"),
        "VLC": ("Media Player", "Other"),
    }

    # Case-insensitive fallback lookup
    skill_map_lower = {k.lower(): v for k, v in skill_map.items()}

    skill_usage: Dict[str, Dict] = {}
    categories = set()
    for app, seconds in app_time.items():
        skill_name, category = skill_map.get(app, skill_map_lower.get(app.lower(), (app, "Other")))
        categories.add(category)
        hours = seconds / 3600.0
        if skill_name not in skill_usage:
            skill_usage[skill_name] = {
                "hours": 0, "category": category,
                "last_used": app_last_used.get(app, ""),
            }
        skill_usage[skill_name]["hours"] += hours
        if app_last_used.get(app, "") > skill_usage[skill_name]["last_used"]:
            skill_usage[skill_name]["last_used"] = app_last_used.get(app, "")

    # Build top skills with mastery score based on hours
    total_hours = sum(s["hours"] for s in skill_usage.values()) or 1
    top_skills = []
    for name, info in sorted(skill_usage.items(), key=lambda x: -x[1]["hours"]):
        mastery = min(100, (info["hours"] / total_hours) * 100)
        top_skills.append(SkillItem(
            name=name,
            category=info["category"],
            mastery=round(mastery, 1),
            time_invested_hours=round(info["hours"], 1),
            last_used=info["last_used"],
            growth_rate=round(info["hours"] / 7, 1),  # hours per day as proxy
        ))

    advanced = sum(1 for s in top_skills if s.mastery > 70)
    # Diversity: ratio of unique categories to max expected categories
    MAX_CATEGORIES = 8  # number of distinct category groups in skill_map
    diversity = min(100.0, (len(categories) / MAX_CATEGORIES) * 100)
    velocity = len(skill_usage) / 4.0  # skills per month approximation

    # ── Dynamic Recommendations from actual usage patterns ─────────────
    recommendations = []
    if activities and top_skills:
        existing_skill_names = {s.name for s in top_skills}

        # Build complementary skills map: category → adjacent skills to learn
        # Each entry: (suggested_skill, reason_template, hours, difficulty)
        # reason_template uses {app} and {hours} placeholders filled from real data
        category_complements = {
            "Development": [
                ("Unit Testing", "You spend {hours}h on {app} — add testing to ship more confidently", 15.0, "intermediate"),
                ("CI/CD Pipelines", "Automate deployments for your {app} development work", 12.0, "intermediate"),
                ("Code Review", "Level up your {app} workflow with systematic review practices", 8.0, "beginner"),
            ],
            "Knowledge": [
                ("Speed Reading", "You spend {hours}h researching with {app} — read faster, learn more", 5.0, "beginner"),
                ("Note-taking Systems", "Organize the knowledge you gather from {hours}h of {app} usage", 6.0, "beginner"),
                ("Critical Analysis", "Evaluate sources more effectively during your {app} research", 10.0, "intermediate"),
            ],
            "Analytics": [
                ("Data Visualization", "Present your {app} analysis with compelling charts", 10.0, "beginner"),
                ("SQL Queries", "Go deeper with data — query databases directly", 15.0, "intermediate"),
                ("Statistical Thinking", "Add statistical rigor to your {app} data work", 12.0, "intermediate"),
            ],
            "Collaboration": [
                ("Facilitation", "Lead your {app} meetings more effectively", 6.0, "beginner"),
                ("Async Communication", "Reduce meeting load with better written updates", 5.0, "beginner"),
                ("Conflict Resolution", "Navigate challenging team dynamics", 8.0, "intermediate"),
            ],
            "Creative": [
                ("Design Systems", "Scale your {app} design work with reusable components", 15.0, "intermediate"),
                ("User Research", "Validate your designs with real user feedback", 10.0, "beginner"),
                ("Motion Design", "Add animations to your {app} designs", 12.0, "intermediate"),
            ],
            "System": [
                ("Shell Scripting", "You use {app} regularly — automate your file workflows", 10.0, "beginner"),
                ("System Administration", "Master your OS tools beyond {app}", 15.0, "intermediate"),
            ],
            "Other": [
                ("Workflow Optimization", "You switch between {hours}h worth of apps — streamline your workflow", 5.0, "beginner"),
                ("Keyboard Shortcuts", "Speed up your work across all {count} apps you use", 3.0, "beginner"),
            ],
        }

        # For each category the user actually uses, find complementary skills
        cat_hours: Dict[str, tuple] = {}  # category → (total_hours, top_app_name)
        for s in top_skills:
            cat = s.category
            if cat not in cat_hours:
                cat_hours[cat] = (s.time_invested_hours, s.name)
            else:
                prev_h, prev_app = cat_hours[cat]
                cat_hours[cat] = (prev_h + s.time_invested_hours, prev_app)

        # Sort by hours so we recommend based on what user does MOST
        sorted_cats = sorted(cat_hours.items(), key=lambda x: -x[1][0])

        for cat, (hours, top_app) in sorted_cats:
            complements = category_complements.get(cat, category_complements["Other"])
            for skill_name, reason_tpl, est_hours, difficulty in complements:
                if skill_name in existing_skill_names:
                    continue
                if skill_name in {r.name for r in recommendations}:
                    continue
                reason = reason_tpl.format(
                    app=top_app,
                    hours=round(hours, 1),
                    count=len(skill_usage),
                )
                recommendations.append(SkillRecommendation(
                    name=skill_name,
                    reason=reason,
                    estimated_time_hours=est_hours,
                    difficulty=difficulty,
                ))
                break  # One recommendation per category
            if len(recommendations) >= 4:
                break

    # Growth history: real per-day activity counts for top skills
    today = datetime.now(timezone.utc).date()
    growth_history = []
    for skill in top_skills[:3]:
        # Get day-by-day activity count for apps mapped to this skill (last 14 days)
        inv_skill_map = {v[0]: k for k, v in skill_map.items()}
        app_name = inv_skill_map.get(skill.name, skill.name)
        for d in range(13, -1, -1):
            day = today - timedelta(days=d)
            day_start = datetime.combine(day, datetime.min.time()).replace(tzinfo=timezone.utc)
            day_end = day_start + timedelta(days=1)
            day_acts = [
                a for a in activities
                if a.app == app_name
                and a.occurred_at
                and day_start <= a.occurred_at < day_end
            ]
            day_hours = sum(a.duration_seconds or 0 for a in day_acts) / 3600
            # Mastery on this day = proportion of total skill hours earned by this day
            day_mastery = min(100, (day_hours / max(skill.time_invested_hours, 0.01)) * skill.mastery * 2)
            growth_history.append(SkillGrowthEntry(
                date=day.isoformat(),
                skill_name=skill.name,
                mastery=round(day_mastery, 1),
            ))

    return SkillMetrics(
        total_skills=len(skill_usage),
        advanced_skills=advanced,
        skill_diversity=round(diversity, 1),
        learning_velocity=round(velocity, 1),
        top_skills=top_skills,
        recommended_skills=recommendations,
        growth_history=growth_history,
    )


@router.get("/career", response_model=CareerInsights)
async def get_career_insights(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Career insights computed from real activity patterns."""
    user_id = _get_user_id(credentials)

    now = datetime.now(timezone.utc)
    # Current week activities (last 7 days)
    curr_start = now - timedelta(days=7)
    # Previous week activities (7-14 days ago)
    prev_start = now - timedelta(days=14)

    curr_result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= curr_start,
        )
    )
    curr_acts = curr_result.scalars().all()

    prev_result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= prev_start,
            Activity.occurred_at < curr_start,
        )
    )
    prev_acts = prev_result.scalars().all()

    # --- Career phase from activity volume + diversity ---
    curr_hours = sum(a.duration_seconds or 0 for a in curr_acts) / 3600
    deep_work = [a for a in curr_acts if a.type in ("window_focus", "app_focus") and (a.duration_seconds or 0) >= 300]
    deep_hours = sum(a.duration_seconds or 0 for a in deep_work) / 3600
    unique_apps = len({a.app for a in curr_acts if a.app})
    meetings = [a for a in curr_acts if a.type == "meeting"]

    if curr_hours < 10:
        career_phase = "exploring"
    elif unique_apps >= 8 and len(meetings) >= 5 and curr_hours >= 20:
        career_phase = "lead"
    elif unique_apps >= 6 and deep_hours >= 15:
        career_phase = "senior"
    else:
        career_phase = "growth"

    # --- Growth trajectory from week-over-week activity delta ---
    curr_total = sum(a.duration_seconds or 0 for a in curr_acts)
    prev_total = sum(a.duration_seconds or 0 for a in prev_acts)
    if prev_total == 0:
        growth_trajectory = "steady"
    else:
        delta_pct = (curr_total - prev_total) / prev_total
        if delta_pct >= 0.15:
            growth_trajectory = "accelerating"
        elif delta_pct <= -0.15:
            growth_trajectory = "declining"
        elif -0.05 <= delta_pct <= 0.05:
            growth_trajectory = "steady"
        else:
            growth_trajectory = "plateau"

    # --- Skill gaps: apps used < 1h/week get flagged as weak ---
    app_time: Dict[str, float] = {}
    for a in curr_acts:
        if a.app:
            app_time[a.app] = app_time.get(a.app, 0) + (a.duration_seconds or 0) / 3600

    skill_map_short = {
        "VS Code": "Programming", "Terminal": "DevOps", "Chrome": "Research",
        "Figma": "Design", "Slack": "Communication", "Notion": "Documentation",
        "Zoom": "Meetings", "Google Meet": "Meetings",
    }
    skill_hours = {skill_map_short.get(app, app): hrs for app, hrs in app_time.items()}
    skill_gaps = [name for name, hrs in sorted(skill_hours.items(), key=lambda x: x[1]) if hrs < 1.0][:3]

    # --- Recommended next steps based on actual low-usage areas ---
    step_map = {
        "Research": "Spend 30 min/day reading docs or articles in your domain",
        "Documentation": "Write a short README or decision log for your current project",
        "Communication": "Schedule a sync with a collaborator you haven't spoken to this week",
        "Design": "Review UI patterns for 20 min to improve product intuition",
        "DevOps": "Automate one manual step in your dev workflow this week",
        "Testing": "Add tests for the last feature you shipped",
        "Meetings": "Block focus time to counterbalance your meeting load",
        "Programming": "Pick one algorithmic problem to solve this week for sharpness",
    }
    # Prioritise steps for weakest skills first, then add generic growth steps
    recommended = [step_map[g] for g in skill_gaps if g in step_map]
    if career_phase == "growth" and len(recommended) < 3:
        recommended.append("Deepen one core skill to advanced mastery (>70%) this month")
    if growth_trajectory in ("plateau", "declining") and len(recommended) < 4:
        recommended.append("Review your weekly time allocation and cut low-value activities")
    if len(recommended) < 2:
        recommended.append("Track your work daily — more data means better insights")

    return CareerInsights(
        growth_trajectory=growth_trajectory if curr_acts else "",
        career_phase=career_phase if curr_acts else "",
        skill_gaps=skill_gaps if curr_acts else [],
        recommended_next_steps=recommended[:4] if curr_acts else [],
    )


@router.get("/wellness", response_model=WellnessMetrics)
async def get_wellness_metrics(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Wellness metrics computed from work patterns, session balance, and variety."""
    user_id = _get_user_id(credentials)

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=7)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
        )
    )
    activities = result.scalars().all()

    # No activities = return zero state
    if not activities:
        return WellnessMetrics(
            overall_score=0,
            work_life_balance={},
            burnout_risk={"level": "unknown", "long_sessions": 0},
            rest_recovery={"break_count": 0, "total_break_minutes": 0},
        )

    total_seconds = sum(a.duration_seconds or 0 for a in activities)
    total_hours = total_seconds / 3600.0
    breaks = [a for a in activities if a.type in ("break", "idle")]
    break_seconds = sum(a.duration_seconds or 0 for a in breaks)

    # Compute wellness from multiple signals:
    # 1) Session balance: penalize very long sessions (>2h), reward moderate ones
    long_sessions = [a for a in activities if (a.duration_seconds or 0) > 7200]
    moderate_sessions = [a for a in activities if 600 <= (a.duration_seconds or 0) <= 5400]  # 10min-90min
    session_balance = min(100, (len(moderate_sessions) / max(len(activities), 1)) * 120)

    # 2) Variety score: how many different apps/types used
    unique_apps = len(set(a.app for a in activities if a.app))
    unique_types = len(set(a.type for a in activities))
    variety_score = min(100, (unique_apps + unique_types) * 10)

    # 3) Hours balance: 4-8h/day is ideal, penalize >10h/day
    days_active = max(1, len(set(
        a.occurred_at.date() for a in activities if a.occurred_at
    )))
    avg_daily_hours = total_hours / days_active
    hours_score = 100 if 4 <= avg_daily_hours <= 8 else max(0, 100 - abs(avg_daily_hours - 6) * 15)

    # 4) Break ratio (if any breaks exist)
    break_ratio = break_seconds / max(total_seconds, 1)
    break_score = min(100, break_ratio * 500) if breaks else max(40, hours_score * 0.6)

    # Overall: weighted average
    overall = (session_balance * 0.3 + variety_score * 0.2 + hours_score * 0.3 + break_score * 0.2)
    overall = min(100, max(0, overall))

    balance_combined = (hours_score * 0.5 + session_balance * 0.3 + break_score * 0.2)
    burnout_level = "low" if len(long_sessions) < 3 else "medium" if len(long_sessions) < 6 else "high"

    # Rest: count natural gaps between sessions as implicit breaks
    implicit_breaks = 0
    implicit_break_minutes = 0
    if len(breaks) == 0 and len(activities) > 1:
        sorted_acts = sorted([a for a in activities if a.occurred_at], key=lambda a: a.occurred_at)
        for i in range(1, len(sorted_acts)):
            gap_sec = (sorted_acts[i].occurred_at - sorted_acts[i-1].occurred_at).total_seconds() - (sorted_acts[i-1].duration_seconds or 0)
            if 120 <= gap_sec <= 3600:  # 2min - 1h gaps count as breaks
                implicit_breaks += 1
                implicit_break_minutes += gap_sec / 60

    total_breaks = len(breaks) + implicit_breaks
    total_break_min = (break_seconds / 60) + implicit_break_minutes

    return WellnessMetrics(
        overall_score=round(overall, 1),
        work_life_balance={"score": round(balance_combined, 1), "break_ratio": round(break_ratio + (implicit_break_minutes * 60 / max(total_seconds, 1)), 2)},
        burnout_risk={"level": burnout_level, "long_sessions": len(long_sessions)},
        rest_recovery={"break_count": total_breaks, "total_break_minutes": round(total_break_min, 1)},
    )


@router.get("/summary/weekly", response_model=WeeklySummary)
async def get_weekly_summary(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    week_offset: int = Query(0, ge=0, le=52, description="Weeks ago (0=current week)"),
):
    """Real weekly summary with html_content and summary_stats."""
    user_id = _get_user_id(credentials)

    today = datetime.now(timezone.utc).date()
    week_start = today - timedelta(days=today.weekday() + (week_offset * 7))
    week_end = week_start + timedelta(days=6)

    start_dt = datetime.combine(week_start, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(week_end + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
            Activity.occurred_at < end_dt,
        ).order_by(Activity.occurred_at.desc())
    )
    activities = result.scalars().all()

    # Cap each activity duration at 2h (same as daily productivity) for consistency
    MAX_ACTIVITY_DURATION = 7200  # 2 hours
    def capped_dur(a):
        return min(a.duration_seconds or 0, MAX_ACTIVITY_DURATION)

    total_seconds = sum(capped_dur(a) for a in activities)
    total_hours = total_seconds / 3600.0

    # Top activities by time
    app_time: Dict[str, float] = {}
    for a in activities:
        key = a.app or a.type
        app_time[key] = app_time.get(key, 0) + capped_dur(a)
    top_activities = sorted(
        [{"name": k, "hours": round(v / 3600, 1)} for k, v in app_time.items()],
        key=lambda x: x["hours"], reverse=True,
    )[:5]

    # Focus score (unified helper)
    focus_score = _compute_focus_score(activities, total_seconds)

    # Productivity score
    productive_types = {"window_focus", "app_focus", "page_view", "web_visit"}
    productive_seconds = sum(capped_dur(a) for a in activities if a.type in productive_types)
    productivity_score = min(100.0, (productive_seconds / max(total_seconds, 1)) * 100)

    # Collaboration index
    meetings = [a for a in activities if a.type == "meeting"]
    comm_apps = {"Slack", "Teams", "Discord", "Zoom", "Google Meet"}
    communication = [a for a in activities if a.app in comm_apps]
    collaboration_index = min(10.0, (len(meetings) + len(communication)) / max(len(activities) * 0.1, 1))

    # Top skills (from apps)
    skill_map = {"VS Code": "Programming", "Terminal": "DevOps", "Chrome": "Research",
                 "Figma": "Design", "Slack": "Communication", "Notion": "Documentation"}
    top_skills_set = set()
    for a in top_activities[:5]:
        top_skills_set.add(skill_map.get(a["name"], a["name"]))
    top_skills = list(top_skills_set)[:5]

    # Key achievements
    key_achievements = []
    if activities:
        key_achievements.append(f"Tracked {len(activities)} activities")
        deep_work = [a for a in activities if a.type in ("window_focus", "app_focus") and (a.duration_seconds or 0) >= 300]
        if deep_work:
            dw_hours = sum(capped_dur(a) for a in deep_work) / 3600
            key_achievements.append(f"Completed {len(deep_work)} deep work sessions ({dw_hours:.1f}h)")
        if meetings:
            key_achievements.append(f"Attended {len(meetings)} meetings")
        if top_activities:
            key_achievements.append(f"Most productive app: {top_activities[0]['name']} ({top_activities[0]['hours']}h)")

    # Generate HTML content
    html_parts = []
    html_parts.append(f"<h2>Week of {week_start.strftime('%B %d')} - {week_end.strftime('%B %d, %Y')}</h2>")
    html_parts.append(f"<p>You tracked <strong>{len(activities)} activities</strong> totaling <strong>{_format_duration(total_seconds)}</strong> this week.</p>")

    if top_activities:
        html_parts.append("<h3>🏆 Top Activities</h3><ul>")
        for ta in top_activities:
            html_parts.append(f"<li><strong>{ta['name']}</strong>: {ta['hours']}h</li>")
        html_parts.append("</ul>")

    html_parts.append(f"<h3>📊 Performance</h3>")
    html_parts.append(f"<p>Your focus score was <strong>{focus_score:.0f}</strong> and productivity score was <strong>{productivity_score:.0f}</strong>.</p>")

    if key_achievements:
        html_parts.append("<h3>✅ Achievements</h3><ul>")
        for ach in key_achievements:
            html_parts.append(f"<li>{ach}</li>")
        html_parts.append("</ul>")

    html_parts.append("<h3>💡 Suggestions</h3><ul>")
    html_parts.append("<li>Schedule focused blocks for your most productive apps</li>")
    html_parts.append("<li>Take regular breaks to maintain high performance</li>")
    html_parts.append("<li>Try to minimize context switches during deep work</li>")
    html_parts.append("</ul>")

    html_content = "\n".join(html_parts)

    return WeeklySummary(
        week_start=week_start.isoformat(),
        week_end=week_end.isoformat(),
        html_content=html_content,
        summary_stats=SummaryStats(
            total_activities=len(activities),
            focus_score=round(focus_score, 1),
            productivity_score=round(productivity_score, 1),
            collaboration_index=round(collaboration_index, 1),
            top_skills=top_skills,
            key_achievements=key_achievements,
        ),
    )


@router.get("/export")
async def export_analytics(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    format: str = Query("json", pattern="^(json|csv)$"),
):
    """Export real analytics data."""
    user_id = _get_user_id(credentials)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
        ).order_by(Activity.occurred_at.desc()).limit(1000)
    )
    activities = result.scalars().all()

    data = [
        {
            "id": str(a.id),
            "type": a.type,
            "app": a.app or "",
            "title": a.title or "",
            "domain": a.domain or "",
            "duration_seconds": a.duration_seconds or 0,
            "occurred_at": a.occurred_at.isoformat() if a.occurred_at else "",
        }
        for a in activities
    ]

    return {"activities": data, "total": len(data), "format": format}


@router.get("/report")
async def generate_productivity_report(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    days: int = Query(7, ge=1, le=90),
):
    """Generate a comprehensive HTML productivity report."""
    from fastapi.responses import HTMLResponse

    user_id = _get_user_id(credentials)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start,
        ).order_by(Activity.occurred_at.asc())
    )
    activities = result.scalars().all()

    # ── Compute overview stats ────────────────────────────────────────────
    total_seconds = sum(a.duration_seconds or 0 for a in activities)
    total_hours = round(total_seconds / 3600, 1)
    total_activities = len(activities)

    # App breakdown
    app_map: dict[str, float] = {}
    for a in activities:
        key = a.app or a.domain or "Unknown"
        app_map[key] = app_map.get(key, 0) + (a.duration_seconds or 0)
    top_apps = sorted(app_map.items(), key=lambda x: x[1], reverse=True)[:10]

    # Daily breakdown
    daily_map: dict[str, dict] = {}
    for a in activities:
        if not a.occurred_at:
            continue
        d = a.occurred_at.strftime("%Y-%m-%d")
        if d not in daily_map:
            daily_map[d] = {"seconds": 0, "count": 0}
        daily_map[d]["seconds"] += a.duration_seconds or 0
        daily_map[d]["count"] += 1

    # Deep work sessions (≥25 min focused blocks)
    dw_sessions = []
    current_session: dict | None = None
    for a in activities:
        if not a.occurred_at or not a.duration_seconds or a.duration_seconds < 60:
            continue
        if current_session:
            gap = (a.occurred_at - current_session["end"]).total_seconds()
            if gap <= 300:
                current_session["end"] = a.occurred_at + timedelta(seconds=a.duration_seconds or 0)
                current_session["total_sec"] += a.duration_seconds or 0
            else:
                if current_session["total_sec"] >= 300:
                    dw_sessions.append(current_session)
                current_session = {
                    "start": a.occurred_at,
                    "end": a.occurred_at + timedelta(seconds=a.duration_seconds or 0),
                    "total_sec": a.duration_seconds or 0,
                }
        else:
            current_session = {
                "start": a.occurred_at,
                "end": a.occurred_at + timedelta(seconds=a.duration_seconds or 0),
                "total_sec": a.duration_seconds or 0,
            }
    if current_session and current_session["total_sec"] >= 300:
        dw_sessions.append(current_session)
    dw_total_hours = round(sum(s["total_sec"] for s in dw_sessions) / 3600, 1)

    # Context switches
    switches = 0
    for i in range(1, len(activities)):
        prev_key = activities[i - 1].app or activities[i - 1].domain or ""
        curr_key = activities[i].app or activities[i].domain or ""
        if prev_key and curr_key and prev_key != curr_key:
            switches += 1

    # Break detection (gaps 5-120 min)
    breaks = []
    for i in range(1, len(activities)):
        prev = activities[i - 1]
        curr = activities[i]
        if prev.occurred_at and curr.occurred_at:
            prev_end = prev.occurred_at + timedelta(seconds=prev.duration_seconds or 0)
            gap_min = (curr.occurred_at - prev_end).total_seconds() / 60
            if 5 <= gap_min <= 120:
                breaks.append(round(gap_min))
    avg_break = round(sum(breaks) / len(breaks), 1) if breaks else 0

    # ── Build daily rows HTML ─────────────────────────────────────────────
    daily_rows = ""
    for d in sorted(daily_map.keys()):
        hrs = round(daily_map[d]["seconds"] / 3600, 1)
        cnt = daily_map[d]["count"]
        daily_rows += f'<tr><td>{d}</td><td>{hrs}h</td><td>{cnt}</td></tr>\n'

    # ── Build top apps rows HTML ──────────────────────────────────────────
    app_rows = ""
    for i, (app, secs) in enumerate(top_apps):
        hrs = round(secs / 3600, 1)
        pct = round(secs / max(total_seconds, 1) * 100, 1)
        app_rows += f'<tr><td>{i+1}</td><td>{app}</td><td>{hrs}h</td><td>{pct}%</td></tr>\n'

    # ── Generate report date ──────────────────────────────────────────────
    report_date = now.strftime("%B %d, %Y")
    period_start = start.strftime("%B %d")
    period_end = now.strftime("%B %d, %Y")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>MiniMe Productivity Report — {report_date}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'Inter', sans-serif; color: #1f2937; background: #fff; padding: 40px 48px; max-width: 900px; margin: 0 auto; }}
  @media print {{
    body {{ padding: 20px; }}
    .no-print {{ display: none !important; }}
    @page {{ margin: 1cm; }}
  }}
  h1 {{ font-size: 24px; font-weight: 700; margin-bottom: 4px; color: #111827; }}
  .subtitle {{ color: #6b7280; font-size: 13px; margin-bottom: 28px; }}
  .section {{ margin-bottom: 28px; }}
  .section-title {{ font-size: 15px; font-weight: 600; color: #374151; margin-bottom: 12px; padding-bottom: 6px; border-bottom: 2px solid #e5e7eb; }}
  .stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }}
  .stat-card {{ background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; }}
  .stat-value {{ font-size: 22px; font-weight: 700; color: #111827; }}
  .stat-label {{ font-size: 11px; color: #6b7280; margin-top: 2px; text-transform: uppercase; letter-spacing: 0.5px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ text-align: left; padding: 8px 12px; background: #f3f4f6; color: #374151; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #f3f4f6; color: #4b5563; }}
  tr:last-child td {{ border-bottom: none; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 6px; font-size: 11px; font-weight: 600; }}
  .badge-indigo {{ background: #eef2ff; color: #4338ca; }}
  .badge-amber {{ background: #fffbeb; color: #b45309; }}
  .badge-emerald {{ background: #ecfdf5; color: #065f46; }}
  .footer {{ margin-top: 40px; padding-top: 16px; border-top: 1px solid #e5e7eb; color: #9ca3af; font-size: 11px; text-align: center; }}
  .print-btn {{ background: #6366f1; color: #fff; border: none; padding: 10px 20px; border-radius: 8px; font-size: 13px; font-weight: 600; cursor: pointer; margin-bottom: 24px; }}
  .print-btn:hover {{ background: #4f46e5; }}
</style>
</head>
<body>
<button class="print-btn no-print" onclick="window.print()">🖨️ Print / Save as PDF</button>

<h1>📊 MiniMe Productivity Report</h1>
<p class="subtitle">Period: {period_start} – {period_end} ({days} days) · Generated {report_date}</p>

<div class="section">
  <div class="stats-grid">
    <div class="stat-card">
      <div class="stat-value">{total_hours}h</div>
      <div class="stat-label">Total Hours Tracked</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{total_activities}</div>
      <div class="stat-label">Activities</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{dw_total_hours}h</div>
      <div class="stat-label">Deep Work Hours</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{len(dw_sessions)}</div>
      <div class="stat-label">Deep Work Sessions</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{switches}</div>
      <div class="stat-label">Context Switches</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{round(switches / max(days, 1))}</div>
      <div class="stat-label">Avg Switches / Day</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{len(breaks)}</div>
      <div class="stat-label">Total Breaks</div>
    </div>
    <div class="stat-card">
      <div class="stat-value">{avg_break}m</div>
      <div class="stat-label">Avg Break Duration</div>
    </div>
  </div>
</div>

<div class="section">
  <div class="section-title">📅 Daily Breakdown</div>
  <table>
    <thead><tr><th>Date</th><th>Hours</th><th>Activities</th></tr></thead>
    <tbody>{daily_rows}</tbody>
  </table>
</div>

<div class="section">
  <div class="section-title">🏆 Top Applications</div>
  <table>
    <thead><tr><th>#</th><th>Application</th><th>Hours</th><th>Share</th></tr></thead>
    <tbody>{app_rows}</tbody>
  </table>
</div>

<div class="section">
  <div class="section-title">🧠 Deep Work Summary</div>
  <p style="font-size: 13px; color: #4b5563; margin-bottom: 8px;">
    {len(dw_sessions)} focused sessions totaling <span class="badge badge-indigo">{dw_total_hours}h</span> of deep work.
    {f'Average session length: {round(sum(s["total_sec"] for s in dw_sessions) / max(len(dw_sessions), 1) / 60)}m.' if dw_sessions else 'No deep work sessions detected.'}
  </p>
</div>

<div class="section">
  <div class="section-title">🔀 Context Switch Analysis</div>
  <p style="font-size: 13px; color: #4b5563;">
    <span class="badge badge-amber">{switches} total switches</span> across {days} days
    ({round(switches / max(days, 1))} avg/day).
    {'Consider batching similar tasks to reduce interruptions.' if switches / max(days, 1) > 50 else 'Good focus discipline — context switches are manageable.'}
  </p>
</div>

<div class="section">
  <div class="section-title">☕ Break Patterns</div>
  <p style="font-size: 13px; color: #4b5563;">
    <span class="badge badge-emerald">{len(breaks)} breaks</span> detected (avg {avg_break}m each).
    {'Excellent break habits — regular recovery periods detected.' if len(breaks) >= days * 2 else 'Consider taking more regular breaks to sustain energy levels.'}
  </p>
</div>

<div class="footer">
  Generated by MiniMe · Privacy-first Activity Intelligence · {report_date}
</div>
</body>
</html>"""

    return HTMLResponse(content=html)


# =====================================================
# GOALS ENDPOINTS — persisted to PostgreSQL
# =====================================================

def _goal_to_pydantic(g, deep_work_hours: float = 0, meeting_hours: float = 0) -> Goal:
    """Convert a UserGoal DB row to the Goal Pydantic model, auto-computing progress."""
    current = g.current_value
    status = g.status

    if g.category == "focus" and g.unit == "hours":
        current = round(deep_work_hours, 1)
        if current >= g.target_value and status == "active":
            status = "completed"
    elif g.category == "productivity" and "meeting" in g.title.lower() and g.unit == "hours":
        current = round(meeting_hours, 1)
        if meeting_hours <= g.target_value and status == "active":
            current = g.target_value
            status = "completed"

    return Goal(
        id=str(g.id),
        title=g.title,
        category=g.category,
        target_value=g.target_value,
        current_value=current,
        unit=g.unit,
        deadline=g.deadline.isoformat() if g.deadline else None,
        status=status,
        streak_count=g.streak_count,
        created_at=g.created_at.isoformat() if g.created_at else "",
    )


@router.get("/goals", response_model=List[Goal])
async def list_goals(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """List all goals for the current user with auto-computed progress."""
    from models import UserGoal

    user_id = _get_user_id(credentials)

    result = await db.execute(
        select(UserGoal).where(UserGoal.user_id == uuid_lib.UUID(user_id))
        .order_by(UserGoal.created_at.desc())
    )
    user_goals = result.scalars().all()

    if not user_goals:
        return []

    # Auto-compute progress from recent activities
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=7)
    acts_result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
        )
    )
    activities = acts_result.scalars().all()
    deep_work_hours = sum(
        (a.duration_seconds or 0) / 3600
        for a in activities
        if a.type in ("window_focus", "app_focus") and (a.duration_seconds or 0) >= 300
    )
    meeting_hours = sum(
        (a.duration_seconds or 0) / 3600
        for a in activities if a.type == "meeting"
    )

    return [_goal_to_pydantic(g, deep_work_hours, meeting_hours) for g in user_goals]


@router.post("/goals", response_model=Goal, status_code=201)
async def create_goal(
    goal_data: GoalCreate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Create a new persistent goal."""
    from models import UserGoal

    user_id = _get_user_id(credentials)

    deadline_dt = None
    if goal_data.deadline:
        try:
            deadline_dt = datetime.fromisoformat(goal_data.deadline.replace("Z", "+00:00"))
        except ValueError:
            pass

    new_goal = UserGoal(
        user_id=uuid_lib.UUID(user_id),
        title=goal_data.title,
        category=goal_data.category,
        target_value=goal_data.target_value,
        unit=goal_data.unit,
        deadline=deadline_dt,
    )
    db.add(new_goal)
    await db.commit()
    await db.refresh(new_goal)

    return _goal_to_pydantic(new_goal)


@router.put("/goals/{goal_id}", response_model=Goal)
async def update_goal(
    goal_id: str,
    update: GoalUpdate,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Update a goal."""
    from models import UserGoal

    user_id = _get_user_id(credentials)
    result = await db.execute(
        select(UserGoal).where(
            UserGoal.id == uuid_lib.UUID(goal_id),
            UserGoal.user_id == uuid_lib.UUID(user_id),
        )
    )
    goal = result.scalar_one_or_none()
    if not goal:
        raise HTTPException(status_code=404, detail="Goal not found")

    if update.title is not None:
        goal.title = update.title
    if update.target_value is not None:
        goal.target_value = update.target_value
    if update.current_value is not None:
        goal.current_value = update.current_value
    if update.status is not None:
        goal.status = update.status

    await db.commit()
    await db.refresh(goal)
    return _goal_to_pydantic(goal)


@router.delete("/goals/{goal_id}")
async def delete_goal(
    goal_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Delete a goal."""
    from models import UserGoal
    from sqlalchemy import delete as sql_delete

    user_id = _get_user_id(credentials)
    result = await db.execute(
        sql_delete(UserGoal).where(
            UserGoal.id == uuid_lib.UUID(goal_id),
            UserGoal.user_id == uuid_lib.UUID(user_id),
        )
    )
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Goal not found")

    return {"deleted": True, "id": goal_id}


# =====================================================
# DEEP WORK SESSIONS (Detailed)
# =====================================================

@router.get("/productivity/deep-work-sessions")
async def get_deep_work_sessions(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    days: int = Query(7, description="Number of days to analyze"),
):
    """
    Detailed deep work session analysis.

    Returns individual sessions (≥25min focused blocks), daily aggregates,
    top deep work apps, and longest streak.
    """
    user_id = _get_user_id(credentials)
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=days)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
        ).order_by(Activity.occurred_at)
    )
    activities = result.scalars().all()

    if not activities:
        return {
            "sessions": [], "daily_summary": [], "top_apps": [],
            "total_deep_work_hours": 0, "avg_session_minutes": 0,
            "longest_session_minutes": 0, "total_sessions": 0,
        }

    MAX_DUR = 7200  # 2h cap
    productive_types = {"window_focus", "app_focus", "web_visit", "page_view"}
    MIN_SESSION_SECONDS = 300  # 25 min

    # Build sessions by grouping consecutive productive activities (≤5 min gap)
    MAX_GAP = 300  # 5 min
    sessions = []
    current_session = None

    for a in activities:
        if a.type not in productive_types:
            continue
        dur = min(a.duration_seconds or 0, MAX_DUR)
        if dur < 60:
            continue

        if current_session is None:
            current_session = {
                "start": a.occurred_at,
                "end": a.occurred_at + timedelta(seconds=dur),
                "duration": dur,
                "app": a.app or a.domain or "Unknown",
                "apps": {a.app or a.domain or "Unknown": dur},
            }
        else:
            gap = (a.occurred_at - current_session["end"]).total_seconds()
            if gap <= MAX_GAP:
                # Extend session
                current_session["duration"] += dur
                current_session["end"] = a.occurred_at + timedelta(seconds=dur)
                app_key = a.app or a.domain or "Unknown"
                current_session["apps"][app_key] = current_session["apps"].get(app_key, 0) + dur
            else:
                # Save session if ≥25 min
                if current_session["duration"] >= MIN_SESSION_SECONDS:
                    # Main app = most time spent
                    main_app = max(current_session["apps"], key=current_session["apps"].get)
                    current_session["app"] = main_app
                    sessions.append(current_session)
                current_session = {
                    "start": a.occurred_at,
                    "end": a.occurred_at + timedelta(seconds=dur),
                    "duration": dur,
                    "app": a.app or a.domain or "Unknown",
                    "apps": {a.app or a.domain or "Unknown": dur},
                }

    # Don't forget last session
    if current_session and current_session["duration"] >= MIN_SESSION_SECONDS:
        main_app = max(current_session["apps"], key=current_session["apps"].get)
        current_session["app"] = main_app
        sessions.append(current_session)

    # Format sessions for API response
    formatted_sessions = []
    for s in sessions:
        formatted_sessions.append({
            "start": s["start"].isoformat() if s["start"] else "",
            "end": s["end"].isoformat() if s["end"] else "",
            "duration_minutes": round(s["duration"] / 60, 1),
            "app": s["app"],
            "date": s["start"].strftime("%Y-%m-%d") if s["start"] else "",
        })

    # Daily summary
    daily_map: Dict[str, Dict] = {}
    for s in sessions:
        day = s["start"].strftime("%Y-%m-%d") if s["start"] else "unknown"
        if day not in daily_map:
            daily_map[day] = {"date": day, "sessions": 0, "total_minutes": 0}
        daily_map[day]["sessions"] += 1
        daily_map[day]["total_minutes"] += s["duration"] / 60

    daily_summary = sorted(
        [{"date": v["date"], "sessions": v["sessions"], "total_minutes": round(v["total_minutes"], 1)}
         for v in daily_map.values()],
        key=lambda x: x["date"]
    )

    # Top deep work apps
    app_totals: Dict[str, float] = {}
    for s in sessions:
        for app, dur in s["apps"].items():
            app_totals[app] = app_totals.get(app, 0) + dur
    top_apps = sorted(
        [{"app": k, "hours": round(v / 3600, 2)} for k, v in app_totals.items()],
        key=lambda x: x["hours"], reverse=True
    )[:10]

    total_deep_seconds = sum(s["duration"] for s in sessions)
    longest = max((s["duration"] for s in sessions), default=0)

    return {
        "sessions": formatted_sessions,
        "daily_summary": daily_summary,
        "top_apps": top_apps,
        "total_deep_work_hours": round(total_deep_seconds / 3600, 2),
        "avg_session_minutes": round((total_deep_seconds / 60) / max(len(sessions), 1), 1),
        "longest_session_minutes": round(longest / 60, 1),
        "total_sessions": len(sessions),
    }


# =====================================================
# CONTEXT SWITCH TIMELINE (Detailed)
# =====================================================

@router.get("/productivity/context-switch-timeline")
async def get_context_switch_timeline(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    days: int = Query(7, description="Number of days to analyze"),
):
    """
    Detailed context switch analysis.

    Returns hourly switch counts, most frequent switch patterns (A→B),
    daily totals, and peak switching hours.
    """
    user_id = _get_user_id(credentials)
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=days)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
        ).order_by(Activity.occurred_at)
    )
    activities = result.scalars().all()

    if not activities:
        return {
            "hourly": [], "daily": [], "top_patterns": [],
            "total_switches": 0, "avg_per_day": 0, "peak_hour": None,
        }

    # Count switches per hour and track patterns
    hourly_map: Dict[str, int] = {}   # "YYYY-MM-DD HH" -> count
    daily_map: Dict[str, int] = {}    # "YYYY-MM-DD" -> count
    patterns: Dict[str, int] = {}     # "App A → App B" -> count
    hour_totals: Dict[int, int] = {}  # hour (0-23) -> total switches across all days
    total_switches = 0
    prev_app = None

    for a in activities:
        current_app = a.app or a.domain or a.type
        if not current_app:
            continue

        if prev_app and current_app != prev_app:
            total_switches += 1
            if a.occurred_at:
                hour_key = a.occurred_at.strftime("%Y-%m-%d %H")
                day_key = a.occurred_at.strftime("%Y-%m-%d")
                hourly_map[hour_key] = hourly_map.get(hour_key, 0) + 1
                daily_map[day_key] = daily_map.get(day_key, 0) + 1
                hour = a.occurred_at.hour
                hour_totals[hour] = hour_totals.get(hour, 0) + 1

            pattern_key = f"{prev_app} → {current_app}"
            patterns[pattern_key] = patterns.get(pattern_key, 0) + 1

        prev_app = current_app

    # Format hourly timeline
    hourly = sorted(
        [{"hour": k, "switches": v} for k, v in hourly_map.items()],
        key=lambda x: x["hour"]
    )

    # Format daily
    daily = sorted(
        [{"date": k, "switches": v} for k, v in daily_map.items()],
        key=lambda x: x["date"]
    )

    # Top patterns
    top_patterns = sorted(
        [{"pattern": k, "count": v} for k, v in patterns.items()],
        key=lambda x: x["count"], reverse=True
    )[:10]

    # Peak hour
    peak_hour = max(hour_totals, key=hour_totals.get) if hour_totals else None
    days_with_data = len(daily_map) or 1

    return {
        "hourly": hourly,
        "daily": daily,
        "top_patterns": top_patterns,
        "total_switches": total_switches,
        "avg_per_day": round(total_switches / days_with_data, 1),
        "peak_hour": peak_hour,
        "peak_hour_label": f"{peak_hour}:00-{peak_hour + 1}:00" if peak_hour is not None else None,
    }


# =====================================================
# BREAK PATTERNS (Detailed)
# =====================================================

@router.get("/productivity/break-patterns")
async def get_break_patterns(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    days: int = Query(7, description="Number of days to analyze"),
):
    """
    Detailed break pattern analysis.

    Returns individual breaks (gaps ≥5min between activities), daily break scores,
    avg break duration, frequency, and quality scoring.
    """
    user_id = _get_user_id(credentials)
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=days)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start_dt,
        ).order_by(Activity.occurred_at)
    )
    activities = result.scalars().all()

    if not activities:
        return {
            "breaks": [], "daily_summary": [], "avg_break_minutes": 0,
            "total_breaks": 0, "break_quality_score": 5.0,
            "recommendation": "Start tracking activities to get break insights.",
        }

    MAX_DUR = 7200
    MIN_BREAK_MIN = 5
    MAX_BREAK_MIN = 120

    # Find breaks (gaps ≥5 min between activities)
    breaks = []
    daily_breaks: Dict[str, list] = {}

    for i in range(len(activities) - 1):
        a = activities[i]
        b = activities[i + 1]
        if not a.occurred_at or not b.occurred_at:
            continue

        end_time = a.occurred_at + timedelta(seconds=min(a.duration_seconds or 0, MAX_DUR))
        gap_seconds = (b.occurred_at - end_time).total_seconds()
        gap_minutes = gap_seconds / 60

        if MIN_BREAK_MIN <= gap_minutes <= MAX_BREAK_MIN:
            day_key = a.occurred_at.strftime("%Y-%m-%d")
            break_data = {
                "start": end_time.isoformat(),
                "end": b.occurred_at.isoformat(),
                "duration_minutes": round(gap_minutes, 1),
                "date": day_key,
                "before_app": a.app or a.domain or a.type or "Unknown",
                "after_app": b.app or b.domain or b.type or "Unknown",
            }
            breaks.append(break_data)
            if day_key not in daily_breaks:
                daily_breaks[day_key] = []
            daily_breaks[day_key].append(gap_minutes)

    # Daily summary with quality scores
    IDEAL_BREAK_FREQ_HOURS = 2
    daily_summary = []
    for day_key in sorted(daily_breaks.keys()):
        day_breaks = daily_breaks[day_key]
        count = len(day_breaks)
        avg_dur = sum(day_breaks) / count if count else 0
        total_dur = sum(day_breaks)

        # Quality: reasonable lengths (5-60min) + good frequency
        reasonable = sum(1 for b in day_breaks if 5 <= b <= 60)
        length_score = (reasonable / max(count, 1)) * 4.0

        # Estimate work hours for that day
        day_activities = [a for a in activities
                         if a.occurred_at and a.occurred_at.strftime("%Y-%m-%d") == day_key]
        work_seconds = sum(min(a.duration_seconds or 0, MAX_DUR) for a in day_activities)
        work_hours = work_seconds / 3600
        ideal_breaks = work_hours / IDEAL_BREAK_FREQ_HOURS
        freq_ratio = min(count / max(ideal_breaks, 1), 1.5) if ideal_breaks > 0 else 0
        freq_score = min(freq_ratio, 1.0) * 4.0

        long_breaks = sum(1 for b in day_breaks if b > 60)
        consistency_score = max(2.0 - long_breaks * 0.5, 0)

        quality = min(max(length_score + freq_score + consistency_score, 0), 10)

        daily_summary.append({
            "date": day_key,
            "break_count": count,
            "avg_duration_minutes": round(avg_dur, 1),
            "total_break_minutes": round(total_dur, 1),
            "quality_score": round(quality, 1),
        })

    # Overall stats
    total_breaks = len(breaks)
    all_durations = [b["duration_minutes"] for b in breaks]
    avg_break_min = sum(all_durations) / len(all_durations) if all_durations else 0
    overall_quality = (
        sum(d["quality_score"] for d in daily_summary) / len(daily_summary)
        if daily_summary else 5.0
    )

    # Recommendation
    if overall_quality >= 8:
        rec = "Excellent break habits! You're taking regular, well-timed breaks."
    elif overall_quality >= 6:
        rec = "Good break pattern. Try to be more consistent with break timing."
    elif overall_quality >= 4:
        rec = "Consider taking more regular breaks — aim for one every 2 hours."
    elif total_breaks == 0:
        rec = "No breaks detected! Take short breaks every 90-120 minutes for better focus."
    else:
        rec = "Your breaks are too infrequent or too long. Aim for 5-15 min breaks every 2h."

    return {
        "breaks": breaks[-50:],  # Last 50 breaks
        "daily_summary": daily_summary,
        "avg_break_minutes": round(avg_break_min, 1),
        "total_breaks": total_breaks,
        "break_quality_score": round(overall_quality, 1),
        "recommendation": rec,
    }


# ── Break Classification ────────────────────────────────────────────────────

@router.get("/productivity/break-classification")
async def get_break_classification(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    days: int = Query(7, ge=1, le=90),
):
    """Classify breaks by duration & type with work-break ratio analysis."""
    user_id = _get_user_id(credentials)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start,
        ).order_by(Activity.occurred_at.asc())
    )
    activities = result.scalars().all()

    if len(activities) < 2:
        return {
            "breaks": [],
            "daily_summary": {},
            "break_to_work_ratio": 0,
            "optimal_break_score": 0,
            "total_work_minutes": 0,
            "total_break_minutes": 0,
        }

    # ── Detect gaps & classify ──
    breaks = []
    total_work_secs = 0

    for i in range(1, len(activities)):
        prev = activities[i - 1]
        curr = activities[i]
        gap = (curr.occurred_at - prev.occurred_at).total_seconds()
        work_duration = getattr(prev, "duration", None) or 30
        total_work_secs += work_duration

        if gap < 300:  # <5min = not a break
            continue

        gap_min = gap / 60

        # Duration classification
        if gap_min < 5:
            size = "micro"
        elif gap_min < 15:
            size = "short"
        elif gap_min < 30:
            size = "medium"
        elif gap_min < 60:
            size = "long"
        else:
            size = "extended"

        # Type detection
        prev_app = getattr(prev, "app_name", "") or ""
        curr_app = getattr(curr, "app_name", "") or ""
        if gap_min >= 60:
            break_type = "forced"
        elif prev_app == curr_app:
            break_type = "scheduled"
        else:
            break_type = "natural"

        day_key = curr.occurred_at.strftime("%Y-%m-%d")
        breaks.append({
            "start": prev.occurred_at.isoformat(),
            "end": curr.occurred_at.isoformat(),
            "duration_minutes": round(gap_min, 1),
            "size": size,
            "type": break_type,
            "before_app": prev_app,
            "after_app": curr_app,
            "day": day_key,
        })

    # ── Daily summary ──
    daily = {}
    for b in breaks:
        d = b["day"]
        if d not in daily:
            daily[d] = {"micro": 0, "short": 0, "medium": 0, "long": 0, "extended": 0, "total_minutes": 0, "count": 0}
        daily[d][b["size"]] += 1
        daily[d]["total_minutes"] += b["duration_minutes"]
        daily[d]["count"] += 1

    total_break_min = sum(b["duration_minutes"] for b in breaks)
    total_work_min = total_work_secs / 60

    # Ideal ratio is ~17min break per 52min work ≈ 0.33
    ratio = total_break_min / total_work_min if total_work_min > 0 else 0
    optimal = max(0, 10 - abs(ratio - 0.33) * 20)

    return {
        "breaks": breaks[-100:],
        "daily_summary": daily,
        "break_to_work_ratio": round(ratio, 2),
        "optimal_break_score": round(min(optimal, 10), 1),
        "total_work_minutes": round(total_work_min, 1),
        "total_break_minutes": round(total_break_min, 1),
    }


# ── Focus Periods ────────────────────────────────────────────────────────────

@router.get("/productivity/focus-periods")
async def get_focus_periods(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    days: int = Query(7, ge=1, le=90),
):
    """Analyze activity streaks by focus depth with distraction tracking."""
    user_id = _get_user_id(credentials)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start,
        ).order_by(Activity.occurred_at.asc())
    )
    activities = result.scalars().all()

    if len(activities) < 2:
        return {
            "periods": [],
            "summary": {"total_periods": 0, "avg_quality": 0, "longest_minutes": 0, "depth_distribution": {}},
        }

    # ── Build focus periods (group consecutive same-app blocks, ≤5min gap) ──
    periods = []
    current_app = getattr(activities[0], "app_name", "") or "Unknown"
    period_start = activities[0].occurred_at
    period_acts = [activities[0]]
    distractions = 0  # app switches within a tolerant window

    for i in range(1, len(activities)):
        act = activities[i]
        app = getattr(act, "app_name", "") or "Unknown"
        gap = (act.occurred_at - activities[i - 1].occurred_at).total_seconds()

        if gap > 300:
            # Gap too large — close period
            duration = (activities[i - 1].occurred_at - period_start).total_seconds()
            if duration >= 60:  # min 1 min
                periods.append(_make_focus_period(period_start, duration, current_app, distractions))
            current_app = app
            period_start = act.occurred_at
            period_acts = [act]
            distractions = 0
        elif app != current_app:
            # App switch within tolerance — count as distraction
            distractions += 1
            # If sustained on new app (3+ consecutive), switch primary app
            if distractions >= 3:
                duration = (activities[i - 1].occurred_at - period_start).total_seconds()
                if duration >= 60:
                    periods.append(_make_focus_period(period_start, duration, current_app, distractions - 3))
                current_app = app
                period_start = act.occurred_at
                period_acts = [act]
                distractions = 0
        else:
            period_acts.append(act)

    # Close last period
    if activities:
        duration = (activities[-1].occurred_at - period_start).total_seconds()
        if duration >= 60:
            periods.append(_make_focus_period(period_start, duration, current_app, distractions))

    # ── Summary ──
    depth_dist = {}
    for p in periods:
        d = p["depth"]
        depth_dist[d] = depth_dist.get(d, 0) + 1

    avg_qual = sum(p["quality_score"] for p in periods) / len(periods) if periods else 0
    longest = max((p["duration_minutes"] for p in periods), default=0)

    return {
        "periods": periods[-100:],
        "summary": {
            "total_periods": len(periods),
            "avg_quality": round(avg_qual, 1),
            "longest_minutes": round(longest, 1),
            "depth_distribution": depth_dist,
        },
    }


def _make_focus_period(start_time, duration_secs, app_name, distractions):
    """Build a focus period dict with depth classification and quality score."""
    mins = duration_secs / 60
    if mins >= 45:
        depth = "flow_state"
    elif mins >= 25:
        depth = "deep_work"
    elif mins >= 15:
        depth = "focused"
    elif mins >= 5:
        depth = "moderate"
    else:
        depth = "shallow"

    # Quality: base from depth, penalized by distractions
    base_scores = {"flow_state": 100, "deep_work": 85, "focused": 70, "moderate": 50, "shallow": 25}
    quality = max(0, base_scores[depth] - distractions * 5)

    return {
        "start": start_time.isoformat(),
        "duration_minutes": round(mins, 1),
        "app_name": app_name,
        "depth": depth,
        "distractions": distractions,
        "quality_score": quality,
    }


# ── Screenshot Metadata ─────────────────────────────────────────────────────

@router.get("/screenshots/meta")
async def get_screenshot_metadata(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(50, ge=1, le=200),
):
    """List screenshot metadata from activity records (images stay on-device)."""
    user_id = _get_user_id(credentials)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    result = await db.execute(
        select(Activity).where(
            Activity.user_id == uuid_lib.UUID(user_id),
            Activity.occurred_at >= start,
            Activity.type == "screenshot",
        ).order_by(Activity.occurred_at.desc()).limit(limit)
    )
    screenshots = result.scalars().all()

    items = []
    for s in screenshots:
        meta = getattr(s, "data", None) or {}
        if isinstance(meta, str):
            try:
                import json as _json
                meta = _json.loads(meta)
            except Exception:
                meta = {}
        items.append({
            "id": str(s.id),
            "timestamp": s.occurred_at.isoformat(),
            "app_name": getattr(s, "app", None) or meta.get("app_name", "Unknown"),
            "window_title": getattr(s, "title", None) or meta.get("window_title", ""),
            "label": meta.get("label", ""),
            "width": meta.get("width"),
            "height": meta.get("height"),
            "file_size_bytes": meta.get("file_size_bytes"),
            "encrypted": True,
            "stored_locally": True,
        })

    return {
        "screenshots": items,
        "total": len(items),
        "note": "Screenshot images are encrypted (AES-256-GCM) and stored locally on the desktop app. Only metadata is shown here.",
    }


# =====================================================
# GRAPH-BASED RECOMMENDATIONS
# =====================================================

@router.get("/graph/recommendations")
async def get_graph_recommendations(
    limit: int = Query(default=15, ge=1, le=50),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Get personalized recommendations based on knowledge graph analysis.
    Combines PageRank trending topics, learning path gaps, and cross-domain bridges.
    """
    user_id = _get_user_id(credentials)

    try:
        from services.graph_intelligence_service import graph_intelligence_service

        result = await graph_intelligence_service.get_recommendations(
            user_id=uuid_lib.UUID(user_id),
            db=db,
            limit=limit,
        )
        return result

    except Exception as e:
        logger.warning("graph_recommendations_failed", error=str(e), user_id=user_id)
        return {
            "recommendations": [],
            "total": 0,
            "categories": {
                "trending_topic": 0,
                "deepen_expertise": 0,
                "bridge_gap": 0,
                "explore_connection": 0,
            },
            "error": "Graph intelligence service unavailable",
        }


# =====================================================
# KNOWLEDGE DECAY MODELING
# =====================================================

@router.get("/knowledge/decay")
async def get_knowledge_decay(
    limit: int = Query(default=100, ge=1, le=500),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Get knowledge freshness analysis for all entities."""
    user_id = _get_user_id(credentials)
    try:
        from services.knowledge_decay_service import knowledge_decay_service
        return await knowledge_decay_service.get_decay_analysis(
            user_id=uuid_lib.UUID(user_id), db=db, limit=limit
        )
    except Exception as e:
        logger.warning("knowledge_decay_failed", error=str(e))
        return {"entities": [], "total": 0, "average_freshness": 0, "status_breakdown": {}, "overall_health": "unknown"}


@router.get("/knowledge/decay/at-risk")
async def get_at_risk_knowledge(
    threshold: float = Query(default=0.4, ge=0.0, le=1.0),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Get entities at risk of being forgotten."""
    user_id = _get_user_id(credentials)
    try:
        from services.knowledge_decay_service import knowledge_decay_service
        return await knowledge_decay_service.get_at_risk(
            user_id=uuid_lib.UUID(user_id), db=db, threshold=threshold
        )
    except Exception as e:
        logger.warning("at_risk_knowledge_failed", error=str(e))
        return {"at_risk_entities": [], "total_at_risk": 0, "refresh_suggestions": []}


# =====================================================
# SEMANTIC SIMILARITY CLUSTERING
# =====================================================

@router.get("/knowledge/clusters")
async def get_knowledge_clusters(
    eps: float = Query(default=0.35, ge=0.1, le=1.0),
    min_samples: int = Query(default=2, ge=2, le=10),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Get semantic clusters of related entities."""
    user_id = _get_user_id(credentials)
    try:
        from services.semantic_clustering_service import semantic_clustering_service
        return await semantic_clustering_service.get_clusters(
            user_id=uuid_lib.UUID(user_id), db=db, eps=eps, min_samples=min_samples
        )
    except Exception as e:
        logger.warning("semantic_clustering_failed", error=str(e))
        return {"clusters": [], "total_clusters": 0, "total_entities": 0}


# =====================================================
# COMMUNITY DETECTION
# =====================================================

@router.get("/intelligence/communities")
async def get_communities(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Detect communities in the entity co-occurrence graph."""
    user_id = _get_user_id(credentials)
    try:
        from services.graph_intelligence_service import graph_intelligence_service
        return await graph_intelligence_service.detect_communities(
            user_id=uuid_lib.UUID(user_id), db=db
        )
    except Exception as e:
        logger.warning("community_detection_failed", error=str(e))
        return {"communities": [], "total_communities": 0, "modularity_score": 0}


# =====================================================
# PREDICTIVE PRODUCTIVITY FORECASTING
# =====================================================

@router.get("/productivity/forecast")
async def get_productivity_forecast(
    forecast_days: int = Query(default=14, ge=7, le=30),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Get productivity predictions and weekly patterns."""
    user_id = _get_user_id(credentials)
    try:
        from services.predictive_service import predictive_service
        return await predictive_service.get_forecast(
            user_id=uuid_lib.UUID(user_id), db=db, forecast_days=forecast_days
        )
    except Exception as e:
        logger.warning("productivity_forecast_failed", error=str(e))
        return {
            "predictions": [], "weekly_pattern": [], "peak_hours": [],
            "trend_direction": "error", "forecast_summary": str(e),
        }


# =====================================================
# CUSTOM ENTITY TYPES
# =====================================================

BUILTIN_ENTITY_TYPES = [
    {"name": "person", "color": "#3b82f6", "icon": "👤", "builtin": True},
    {"name": "project", "color": "#22c55e", "icon": "📁", "builtin": True},
    {"name": "skill", "color": "#eab308", "icon": "⚡", "builtin": True},
    {"name": "concept", "color": "#8b5cf6", "icon": "💡", "builtin": True},
    {"name": "organization", "color": "#f97316", "icon": "🏢", "builtin": True},
    {"name": "tool", "color": "#06b6d4", "icon": "🔧", "builtin": True},
    {"name": "artifact", "color": "#ec4899", "icon": "📄", "builtin": True},
    {"name": "event", "color": "#14b8a6", "icon": "📅", "builtin": True},
    {"name": "interaction", "color": "#6366f1", "icon": "🤝", "builtin": True},
]


@router.get("/entities/types")
async def get_entity_types(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Get all entity types (built-in + custom)."""
    user_id = _get_user_id(credentials)

    # Get user preferences for custom types
    from models import User
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()

    custom_types = []
    if user and user.preferences:
        custom_types = user.preferences.get("custom_entity_types", [])

    return {
        "builtin_types": BUILTIN_ENTITY_TYPES,
        "custom_types": custom_types,
        "all_types": BUILTIN_ENTITY_TYPES + [dict(t, builtin=False) for t in custom_types],
    }


@router.post("/entities/types")
async def create_entity_type(
    request: dict,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Create a custom entity type."""
    user_id = _get_user_id(credentials)

    name = request.get("name", "").strip().lower()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")

    # Check not a builtin
    builtin_names = {t["name"] for t in BUILTIN_ENTITY_TYPES}
    if name in builtin_names:
        raise HTTPException(status_code=400, detail=f"'{name}' is a built-in type")

    new_type = {
        "name": name,
        "color": request.get("color", "#6b7280"),
        "icon": request.get("icon", "🏷️"),
        "parent_type": request.get("parent_type"),
    }

    from models import User
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    prefs = dict(user.preferences or {})
    custom_types = list(prefs.get("custom_entity_types", []))

    # Check duplicate
    if any(t["name"] == name for t in custom_types):
        raise HTTPException(status_code=400, detail=f"Custom type '{name}' already exists")

    custom_types.append(new_type)
    prefs["custom_entity_types"] = custom_types
    user.preferences = prefs
    await db.commit()

    return {"created": new_type, "total_custom_types": len(custom_types)}


@router.delete("/entities/types/{type_name}")
async def delete_entity_type(
    type_name: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Delete a custom entity type."""
    user_id = _get_user_id(credentials)

    builtin_names = {t["name"] for t in BUILTIN_ENTITY_TYPES}
    if type_name in builtin_names:
        raise HTTPException(status_code=400, detail="Cannot delete built-in types")

    from models import User
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    prefs = dict(user.preferences or {})
    custom_types = list(prefs.get("custom_entity_types", []))
    original_len = len(custom_types)
    custom_types = [t for t in custom_types if t["name"] != type_name]

    if len(custom_types) == original_len:
        raise HTTPException(status_code=404, detail=f"Custom type '{type_name}' not found")

    prefs["custom_entity_types"] = custom_types
    user.preferences = prefs
    await db.commit()

    return {"deleted": type_name, "remaining": len(custom_types)}
