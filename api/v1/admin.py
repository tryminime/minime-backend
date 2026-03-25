"""
Super Admin API endpoints.
Corporate management dashboard for user management, subscription tracking,
token usage, regional analytics, and system health.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
import uuid as uuid_lib

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from sqlalchemy import select, func, case, distinct
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from database.postgres import get_db
from models import User, Activity, Session, AuditLog, ContentItem
from auth.jwt_handler import security, decode_token, verify_token_type

logger = structlog.get_logger()
router = APIRouter(prefix="/admin", tags=["admin"])


# ─── Auth dependency ────────────────────────────────────────────────────────────

async def require_superadmin(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Verify the caller is a superadmin. Checks JWT first, then DB as fallback."""
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")

    payload = decode_token(credentials.credentials)
    if not payload or not verify_token_type(payload, "access"):
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    # Check JWT claim first (fast path)
    if payload.get("is_superadmin"):
        return user_id

    # Fallback: check DB (handles tokens issued before is_superadmin was added)
    result = await db.execute(
        select(User.is_superadmin).where(User.id == uuid_lib.UUID(user_id))
    )
    is_admin = result.scalar_one_or_none()
    if not is_admin:
        raise HTTPException(status_code=403, detail="Superadmin access required")

    return user_id


# ─── Response Models ────────────────────────────────────────────────────────────

class PlatformOverview(BaseModel):
    total_users: int = 0
    active_users_7d: int = 0
    total_activities: int = 0
    total_sessions: int = 0
    tier_distribution: dict = {}
    signup_trend: list = []
    status_distribution: dict = {}


class AdminUserItem(BaseModel):
    id: str
    email: str
    full_name: Optional[str] = None
    tier: str = "free"
    subscription_status: str = "active"
    is_superadmin: bool = False
    email_verified: bool = False
    activity_count: int = 0
    last_activity: Optional[str] = None
    created_at: Optional[str] = None


# ─── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/stats/overview")
async def admin_overview(
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
):
    """Platform-wide KPI overview for the admin dashboard."""
    # Total users
    total_q = await db.execute(select(func.count(User.id)).where(User.deleted_at.is_(None)))
    total_users = total_q.scalar() or 0

    # Active users (had an activity in last 7 days)
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    active_q = await db.execute(
        select(func.count(distinct(Activity.user_id))).where(Activity.occurred_at >= week_ago)
    )
    active_users_7d = active_q.scalar() or 0

    # Total activities
    act_q = await db.execute(select(func.count(Activity.id)))
    total_activities = act_q.scalar() or 0

    # Total sessions
    sess_q = await db.execute(select(func.count(Session.id)))
    total_sessions = sess_q.scalar() or 0

    # Tier distribution
    tier_q = await db.execute(
        select(User.tier, func.count(User.id))
        .where(User.deleted_at.is_(None))
        .group_by(User.tier)
    )
    tier_distribution = {row[0]: row[1] for row in tier_q.fetchall()}

    # Subscription status distribution
    status_q = await db.execute(
        select(User.subscription_status, func.count(User.id))
        .where(User.deleted_at.is_(None))
        .group_by(User.subscription_status)
    )
    status_distribution = {row[0]: row[1] for row in status_q.fetchall()}

    # Signup trend (last 30 days, grouped by day)
    month_ago = datetime.now(timezone.utc) - timedelta(days=30)
    signup_q = await db.execute(
        select(
            func.date_trunc('day', User.created_at).label('day'),
            func.count(User.id),
        )
        .where(User.created_at >= month_ago, User.deleted_at.is_(None))
        .group_by('day')
        .order_by('day')
    )
    signup_trend = [
        {"date": row[0].isoformat()[:10] if row[0] else "", "count": row[1]}
        for row in signup_q.fetchall()
    ]

    return {
        "total_users": total_users,
        "active_users_7d": active_users_7d,
        "total_activities": total_activities,
        "total_sessions": total_sessions,
        "tier_distribution": tier_distribution,
        "status_distribution": status_distribution,
        "signup_trend": signup_trend,
    }


@router.get("/users")
async def admin_list_users(
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
    search: Optional[str] = Query(None),
    tier: Optional[str] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    sort_by: str = Query("created_at"),
    sort_dir: str = Query("desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Paginated user list with search, filter, and sort."""
    query = select(User).where(User.deleted_at.is_(None))

    if search:
        query = query.where(
            User.email.ilike(f"%{search}%") | User.full_name.ilike(f"%{search}%")
        )
    if tier:
        query = query.where(User.tier == tier)
    if status_filter:
        query = query.where(User.subscription_status == status_filter)

    # Sort
    sort_col = getattr(User, sort_by, User.created_at)
    if sort_dir == "asc":
        query = query.order_by(sort_col.asc())
    else:
        query = query.order_by(sort_col.desc())

    # Count total
    count_q = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_q)
    total = total_result.scalar() or 0

    # Paginate
    query = query.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    users = result.scalars().all()

    # Build response with activity counts
    items = []
    for u in users:
        # Get activity count for this user
        act_count_q = await db.execute(
            select(func.count(Activity.id)).where(Activity.user_id == u.id)
        )
        act_count = act_count_q.scalar() or 0

        # Last activity date
        last_act_q = await db.execute(
            select(func.max(Activity.occurred_at)).where(Activity.user_id == u.id)
        )
        last_act = last_act_q.scalar()

        items.append({
            "id": str(u.id),
            "email": u.email,
            "full_name": u.full_name,
            "tier": u.tier,
            "subscription_status": u.subscription_status,
            "is_superadmin": bool(u.is_superadmin),
            "email_verified": bool(u.email_verified),
            "activity_count": act_count,
            "last_activity": last_act.isoformat() if last_act else None,
            "created_at": u.created_at.isoformat() if u.created_at else None,
        })

    return {
        "users": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
    }


@router.get("/users/{user_id}")
async def admin_get_user(
    user_id: str,
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed user info."""
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Activity stats
    act_q = await db.execute(
        select(func.count(Activity.id)).where(Activity.user_id == user.id)
    )
    activity_count = act_q.scalar() or 0

    # Session count
    sess_q = await db.execute(
        select(func.count(Session.id)).where(Session.user_id == user.id)
    )
    session_count = sess_q.scalar() or 0

    return {
        **user.to_dict(),
        "activity_count": activity_count,
        "session_count": session_count,
    }


@router.patch("/users/{user_id}")
async def admin_update_user(
    user_id: str,
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
    tier: Optional[str] = None,
    subscription_status: Optional[str] = None,
    is_superadmin: Optional[bool] = None,
):
    """Update user tier, status, or admin privileges."""
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    changes = {}
    if tier is not None:
        changes["tier"] = {"from": user.tier, "to": tier}
        user.tier = tier
    if subscription_status is not None:
        changes["subscription_status"] = {"from": user.subscription_status, "to": subscription_status}
        user.subscription_status = subscription_status
    if is_superadmin is not None:
        changes["is_superadmin"] = {"from": user.is_superadmin, "to": is_superadmin}
        user.is_superadmin = is_superadmin

    if changes:
        # Create audit log
        audit = AuditLog(
            user_id=uuid_lib.UUID(admin_id),
            action="admin_update_user",
            resource_type="user",
            resource_id=user.id,
            changes=changes,
        )
        db.add(audit)
        await db.commit()

    return {"message": "User updated", "changes": changes}


@router.delete("/users/{user_id}")
async def admin_delete_user(
    user_id: str,
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
):
    """Soft-delete a user."""
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.deleted_at = datetime.now(timezone.utc)

    audit = AuditLog(
        user_id=uuid_lib.UUID(admin_id),
        action="admin_delete_user",
        resource_type="user",
        resource_id=user.id,
        changes={"email": user.email},
    )
    db.add(audit)
    await db.commit()

    return {"message": "User deleted"}


@router.get("/subscriptions")
async def admin_subscriptions(
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
):
    """Subscription analytics: tier counts, MRR estimate, churn."""
    # Tier distribution
    tier_q = await db.execute(
        select(User.tier, func.count(User.id))
        .where(User.deleted_at.is_(None))
        .group_by(User.tier)
    )
    tier_distribution = {row[0]: row[1] for row in tier_q.fetchall()}

    # Status distribution
    status_q = await db.execute(
        select(User.subscription_status, func.count(User.id))
        .where(User.deleted_at.is_(None))
        .group_by(User.subscription_status)
    )
    status_distribution = {row[0]: row[1] for row in status_q.fetchall()}

    # MRR estimate (placeholder pricing)
    pricing = {"free": 0, "premium": 9.99, "enterprise": 49.99}
    mrr = sum(
        pricing.get(tier, 0) * count
        for tier, count in tier_distribution.items()
        if status_distribution.get("active", 0) > 0
    )

    # Total deleted users (churn)
    churn_q = await db.execute(
        select(func.count(User.id)).where(User.deleted_at.is_not(None))
    )
    churned_users = churn_q.scalar() or 0

    return {
        "tier_distribution": tier_distribution,
        "status_distribution": status_distribution,
        "mrr_estimate": round(mrr, 2),
        "churned_users": churned_users,
        "pricing_tiers": pricing,
    }


@router.get("/regions")
async def admin_regions(
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
):
    """User distribution by region (from session IP/user-agent data)."""
    # Extract IP-based region from sessions
    # Since we don't have GeoIP, group by IP prefix as a rough approximation
    ip_q = await db.execute(
        select(Session.ip_address, func.count(distinct(Session.user_id)))
        .where(Session.ip_address.is_not(None))
        .group_by(Session.ip_address)
        .order_by(func.count(distinct(Session.user_id)).desc())
        .limit(50)
    )
    ip_data = [{"ip": row[0], "user_count": row[1]} for row in ip_q.fetchall()]

    # User agent analysis (browser/OS)
    ua_q = await db.execute(
        select(Session.user_agent, func.count(Session.id))
        .where(Session.user_agent.is_not(None))
        .group_by(Session.user_agent)
        .order_by(func.count(Session.id).desc())
        .limit(20)
    )

    # Parse user agents into categories
    browser_counts: dict = {}
    for row in ua_q.fetchall():
        ua = row[0] or ""
        count = row[1]
        if "Chrome" in ua:
            browser_counts["Chrome"] = browser_counts.get("Chrome", 0) + count
        elif "Firefox" in ua:
            browser_counts["Firefox"] = browser_counts.get("Firefox", 0) + count
        elif "Safari" in ua:
            browser_counts["Safari"] = browser_counts.get("Safari", 0) + count
        elif "Edge" in ua:
            browser_counts["Edge"] = browser_counts.get("Edge", 0) + count
        else:
            browser_counts["Other"] = browser_counts.get("Other", 0) + count

    return {
        "ip_distribution": ip_data,
        "browser_distribution": browser_counts,
        "total_sessions_with_ip": len(ip_data),
    }


@router.get("/tokens")
async def admin_tokens(
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
):
    """AI token usage overview."""
    # Count content items (RAG-ingested docs)
    content_q = await db.execute(select(func.count(ContentItem.id)))
    total_content = content_q.scalar() or 0

    # Content per user
    content_per_user_q = await db.execute(
        select(User.email, func.count(ContentItem.id))
        .join(ContentItem, ContentItem.user_id == User.id)
        .group_by(User.email)
        .order_by(func.count(ContentItem.id).desc())
        .limit(20)
    )
    content_by_user = [
        {"email": row[0], "content_count": row[1]}
        for row in content_per_user_q.fetchall()
    ]

    # Activity volume per user (proxy for token usage)
    activity_per_user_q = await db.execute(
        select(User.email, func.count(Activity.id))
        .join(Activity, Activity.user_id == User.id)
        .group_by(User.email)
        .order_by(func.count(Activity.id).desc())
        .limit(20)
    )
    activity_by_user = [
        {"email": row[0], "activity_count": row[1]}
        for row in activity_per_user_q.fetchall()
    ]

    # Total activities as token proxy
    total_act_q = await db.execute(select(func.count(Activity.id)))
    total_activities = total_act_q.scalar() or 0

    return {
        "total_content_items": total_content,
        "total_activities": total_activities,
        "content_by_user": content_by_user,
        "activity_by_user": activity_by_user,
        "estimated_tokens": total_activities * 150 + total_content * 2000,  # rough estimate
    }


@router.get("/system/health")
async def admin_system_health(
    admin_id: str = Depends(require_superadmin),
    db: AsyncSession = Depends(get_db),
):
    """Check status of all backing services."""
    services = {}

    # PostgreSQL
    try:
        await db.execute(select(func.now()))
        services["postgres"] = {"status": "healthy", "latency_ms": 0}
    except Exception as e:
        services["postgres"] = {"status": "unhealthy", "error": str(e)}

    # Redis
    try:
        from database.redis_client import get_redis_client
        redis = get_redis_client()
        if redis:
            await redis.ping()
            services["redis"] = {"status": "healthy"}
        else:
            services["redis"] = {"status": "unavailable"}
    except Exception as e:
        services["redis"] = {"status": "unhealthy", "error": str(e)}

    # Neo4j
    try:
        from database.neo4j_client import get_neo4j_driver
        driver = get_neo4j_driver()
        if driver:
            async with driver.session() as session:
                await session.run("RETURN 1")
            services["neo4j"] = {"status": "healthy"}
        else:
            services["neo4j"] = {"status": "unavailable"}
    except Exception as e:
        services["neo4j"] = {"status": "unhealthy", "error": str(e)}

    # Qdrant
    try:
        from qdrant_client import QdrantClient
        client = QdrantClient(host="localhost", port=6333, timeout=3)
        client.get_collections()
        services["qdrant"] = {"status": "healthy"}
    except Exception as e:
        services["qdrant"] = {"status": "unhealthy", "error": str(e)}

    # DB stats
    try:
        user_count = await db.execute(select(func.count(User.id)))
        act_count = await db.execute(select(func.count(Activity.id)))
        services["db_stats"] = {
            "users": user_count.scalar() or 0,
            "activities": act_count.scalar() or 0,
        }
    except Exception:
        pass

    return {"services": services, "timestamp": datetime.now(timezone.utc).isoformat()}
