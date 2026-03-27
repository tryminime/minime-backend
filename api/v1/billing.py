"""
Billing API endpoints for MiniMe Platform.
Handles Stripe checkout, subscriptions, usage tracking, invoices, and customer portal.
"""

import os
import stripe
import structlog
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
import uuid as uuid_lib

from database.postgres import get_db
from models import User, Activity
from auth.jwt_handler import decode_token, verify_token_type

logger = structlog.get_logger()
router = APIRouter()
security = HTTPBearer()

# Initialize Stripe
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")

# Price IDs from env (default to test placeholders that trigger demo-mode checkout)
PRICE_MAP = {
    "pro": os.environ.get("STRIPE_PRICE_PRO_MONTHLY", "price_test_pro"),
    "enterprise": os.environ.get("STRIPE_PRICE_ENTERPRISE_MONTHLY", "price_test_enterprise"),
}

# Plan limits configuration
PLAN_LIMITS = {
    "free": {
        "activities_per_month": 100,
        "graph_nodes": 100,
        "api_calls_per_day": 50,
        "name": "Free",
        "price": 0,
        "features": [
            "100 activities/month", "Basic analytics", "7-day weekly digests",
            "Community support", "100 graph nodes",
        ],
    },
    "pro": {
        "activities_per_month": -1,
        "graph_nodes": 500,
        "api_calls_per_day": -1,
        "name": "Pro",
        "price": 19,
        "features": [
            "Unlimited activities", "Advanced analytics", "Real-time insights",
            "Skills tracking", "Knowledge graph (500 nodes)", "Email support",
        ],
    },
    "enterprise": {
        "activities_per_month": -1,
        "graph_nodes": -1,
        "api_calls_per_day": -1,
        "name": "Enterprise",
        "price": 99,
        "features": [
            "Everything in Pro", "Unlimited knowledge graph", "Custom integrations",
            "API access", "Priority support", "Team features", "Custom SLA",
        ],
    },
}


# ── Request Models ────────────────────────────────────

class CheckoutRequest(BaseModel):
    plan_type: str
    success_url: str
    cancel_url: str

class CancelRequest(BaseModel):
    at_period_end: bool = True

class UpdateSubscriptionRequest(BaseModel):
    new_plan_type: str

class PortalRequest(BaseModel):
    return_url: str


# ── Helpers ───────────────────────────────────────────

from api.v1.analytics import _get_user_id


async def _get_user(credentials: HTTPAuthorizationCredentials, db: AsyncSession) -> User:
    user_id = _get_user_id(credentials)
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


# Tier mapping: DB stores 'premium' but frontend uses 'pro'
DB_TO_FRONTEND_TIER = {"premium": "pro", "free": "free", "enterprise": "enterprise"}
FRONTEND_TO_DB_TIER = {"pro": "premium", "free": "free", "enterprise": "enterprise"}


def _warning(current: int, limit: int) -> Dict[str, Any]:
    if limit == -1:
        return {"exceeded": False, "warning": False, "percent_used": 0,
                "current": current, "limit": limit, "remaining": -1}
    remaining = max(0, limit - current)
    pct = (current / max(limit, 1)) * 100
    return {
        "exceeded": current > limit,
        "warning": pct > 70,
        "percent_used": round(pct, 1),
        "current": current,
        "limit": limit,
        "remaining": remaining,
    }


# ── Endpoints ─────────────────────────────────────────

@router.get("/subscription")
async def get_subscription(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Get current subscription status."""
    user = await _get_user(credentials, db)
    db_tier = user.tier or "free"
    plan_type = DB_TO_FRONTEND_TIER.get(db_tier, db_tier)
    plan_info = PLAN_LIMITS.get(plan_type, PLAN_LIMITS["free"])

    sub_data: Dict[str, Any] = {
        "id": 1,
        "user_id": str(user.id),
        "plan_type": plan_type,
        "status": user.subscription_status or "active",
        "cancel_at_period_end": False,
        "plan_details": {
            "name": plan_info["name"],
            "price": plan_info["price"],
            "features": plan_info["features"],
            "limits": {
                "activities_per_month": plan_info["activities_per_month"],
                "graph_nodes": plan_info["graph_nodes"],
                "api_calls_per_day": plan_info["api_calls_per_day"],
            },
        },
    }

    # Try to enrich with Stripe data
    prefs = user.preferences or {}
    stripe_sub_id = prefs.get("stripe_subscription_id")
    if stripe_sub_id and stripe.api_key:
        try:
            sub = stripe.Subscription.retrieve(stripe_sub_id)
            sub_data["stripe_subscription_id"] = stripe_sub_id
            sub_data["status"] = sub.status
            sub_data["cancel_at_period_end"] = sub.cancel_at_period_end
            sub_data["current_period_start"] = datetime.fromtimestamp(
                sub.current_period_start, tz=timezone.utc
            ).isoformat()
            sub_data["current_period_end"] = datetime.fromtimestamp(
                sub.current_period_end, tz=timezone.utc
            ).isoformat()
        except Exception as e:
            logger.warning("Stripe subscription fetch failed", error=str(e))

    return sub_data


@router.get("/usage")
async def get_usage_metrics(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Get usage metrics for the current billing period."""
    user = await _get_user(credentials, db)
    db_tier = user.tier or "free"
    plan_type = DB_TO_FRONTEND_TIER.get(db_tier, db_tier)
    plan_info = PLAN_LIMITS.get(plan_type, PLAN_LIMITS["free"])

    month_start = datetime.now(timezone.utc).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )

    act_result = await db.execute(
        select(func.count()).select_from(Activity).where(
            Activity.user_id == user.id,
            Activity.occurred_at >= month_start,
        )
    )
    act_count = act_result.scalar() or 0

    # Graph nodes count
    graph_nodes = 0
    try:
        from models import Entity
        nodes_result = await db.execute(
            select(func.count()).select_from(Entity).where(Entity.user_id == user.id)
        )
        graph_nodes = nodes_result.scalar() or 0
    except Exception:
        pass


    limits = {
        "activities_per_month": plan_info["activities_per_month"],
        "graph_nodes": plan_info["graph_nodes"],
    }

    return {
        "month": datetime.now(timezone.utc).strftime("%B %Y"),
        "plan_type": plan_type,
        "usage": {
            "activities_count": act_count,
            "graph_nodes_count": graph_nodes,
            "storage_bytes": act_count * 512,
        },
        "limits": limits,
        "warnings": {
            "activities": _warning(act_count, limits["activities_per_month"]),
            "graph_nodes": _warning(graph_nodes, limits["graph_nodes"]),
        },
    }


@router.get("/invoices")
async def get_invoices(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Get invoice history from Stripe."""
    user = await _get_user(credentials, db)
    prefs = user.preferences or {}
    customer_id = prefs.get("stripe_customer_id")

    invoices: List[Dict] = []
    if customer_id and stripe.api_key:
        try:
            stripe_invoices = stripe.Invoice.list(customer=customer_id, limit=10)
            invoices = [
                {
                    "id": inv.id,
                    "amount_paid": inv.amount_paid,
                    "currency": inv.currency,
                    "status": inv.status,
                    "created": datetime.fromtimestamp(inv.created, tz=timezone.utc).isoformat(),
                    "invoice_pdf": inv.invoice_pdf,
                }
                for inv in stripe_invoices.data
            ]
        except Exception as e:
            logger.warning("Invoice fetch failed", error=str(e))

    # Include demo invoices from preferences
    demo_invoices = prefs.get("demo_invoices", [])
    invoices.extend(demo_invoices)

    # Sort all invoices by created date descending
    invoices.sort(key=lambda x: x.get("created", ""), reverse=True)

    return {"invoices": invoices}


@router.get("/plans")
async def get_plans():
    """Get available pricing plans (public)."""
    plans = {}
    for plan_id, info in PLAN_LIMITS.items():
        plans[plan_id] = {
            "name": info["name"],
            "price": info["price"],
            "stripe_price_id": PRICE_MAP.get(plan_id),
            "limits": {
                "activities_per_month": info["activities_per_month"],
                "graph_nodes": info["graph_nodes"],
                "api_calls_per_day": info["api_calls_per_day"],
            },
            "features": info["features"],
        }
    return {"plans": plans}


@router.post("/checkout")
async def create_checkout_session(
    request: CheckoutRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Create a Stripe Checkout session, or demo-mode upgrade if test price IDs."""
    user = await _get_user(credentials, db)
    price_id = PRICE_MAP.get(request.plan_type)
    if not price_id:
        raise HTTPException(status_code=400, detail=f"Unknown plan: {request.plan_type}")

    # Demo mode: if price IDs are test placeholders, upgrade directly
    if price_id.startswith("price_test") or not stripe.api_key:
        db_tier = FRONTEND_TO_DB_TIER.get(request.plan_type, request.plan_type)
        user.tier = db_tier
        user.subscription_status = "active"
        # Store a demo invoice in preferences
        prefs = dict(user.preferences or {})
        demo_invoices = prefs.get("demo_invoices", [])
        plan_info = PLAN_LIMITS.get(request.plan_type, PLAN_LIMITS.get("pro", {}))
        demo_invoices.insert(0, {
            "id": f"inv_demo_{str(uuid_lib.uuid4())[:8]}",
            "amount_paid": (plan_info.get("price", 29)) * 100,  # cents
            "currency": "usd",
            "status": "paid",
            "created": datetime.now(timezone.utc).isoformat(),
            "invoice_pdf": None,
            "description": f"MiniMe {plan_info.get('name', request.plan_type.title())} Plan — Monthly",
        })
        prefs["demo_invoices"] = demo_invoices[:20]  # Keep last 20
        prefs["demo_subscription_start"] = datetime.now(timezone.utc).isoformat()
        user.preferences = prefs
        await db.commit()
        logger.info("Demo checkout: upgraded user", user_id=str(user.id), plan=request.plan_type)
        return {"checkout_url": request.success_url, "session_id": f"demo_{request.plan_type}"}

    # Real Stripe checkout
    prefs = dict(user.preferences or {})
    customer_id = prefs.get("stripe_customer_id")
    if not customer_id:
        customer = stripe.Customer.create(
            email=user.email,
            name=user.full_name or user.email,
            metadata={"user_id": str(user.id)},
        )
        customer_id = customer.id
        prefs["stripe_customer_id"] = customer_id
        user.preferences = prefs
        await db.commit()

    try:
        session = stripe.checkout.Session.create(
            customer=customer_id,
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="subscription",
            success_url=request.success_url,
            cancel_url=request.cancel_url,
            metadata={"user_id": str(user.id), "plan_type": request.plan_type},
        )
        return {"checkout_url": session.url, "session_id": session.id}
    except stripe.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/subscription/cancel")
async def cancel_subscription(
    request: CancelRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Cancel the current subscription."""
    user = await _get_user(credentials, db)
    prefs = user.preferences or {}
    stripe_sub_id = prefs.get("stripe_subscription_id")

    if stripe_sub_id and stripe.api_key:
        try:
            stripe.Subscription.modify(stripe_sub_id, cancel_at_period_end=request.at_period_end)
            return {"message": "Subscription will be canceled at the end of the billing period"}
        except stripe.StripeError as e:
            raise HTTPException(status_code=400, detail=str(e))

    user.tier = "free"
    user.subscription_status = "canceled"
    await db.commit()
    return {"message": "Subscription canceled"}


@router.post("/subscription/update")
async def update_subscription(
    request: UpdateSubscriptionRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Update subscription to a different plan."""
    user = await _get_user(credentials, db)
    new_price_id = PRICE_MAP.get(request.new_plan_type)
    if not new_price_id:
        raise HTTPException(status_code=400, detail=f"Unknown plan: {request.new_plan_type}")

    prefs = user.preferences or {}
    stripe_sub_id = prefs.get("stripe_subscription_id")

    if stripe_sub_id and stripe.api_key:
        try:
            sub = stripe.Subscription.retrieve(stripe_sub_id)
            stripe.Subscription.modify(
                stripe_sub_id,
                items=[{"id": sub["items"]["data"][0].id, "price": new_price_id}],
                proration_behavior="create_prorations",
            )
        except stripe.StripeError as e:
            raise HTTPException(status_code=400, detail=str(e))

    user.tier = FRONTEND_TO_DB_TIER.get(request.new_plan_type, request.new_plan_type)
    await db.commit()
    return {"message": f"Subscription updated to {request.new_plan_type}"}


@router.post("/portal")
async def create_portal_session(
    request: PortalRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """Create a Stripe Customer Portal session."""
    user = await _get_user(credentials, db)
    prefs = user.preferences or {}
    customer_id = prefs.get("stripe_customer_id")

    if not customer_id or not stripe.api_key:
        raise HTTPException(status_code=400, detail="No Stripe customer found")

    try:
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=request.return_url,
        )
        return {"portal_url": session.url}
    except stripe.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))
