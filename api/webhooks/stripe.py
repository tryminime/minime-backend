"""
Stripe Webhook Handler — Async SQLAlchemy version
Routes: POST /api/webhooks/stripe
"""
import os
import stripe
import structlog
from fastapi import APIRouter, Request, HTTPException, Depends
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from database.postgres import get_db
from models import User

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])
logger = structlog.get_logger()

WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

# Map Stripe price IDs → DB tier values
def _build_price_tier_map() -> dict:
    return {
        os.getenv("STRIPE_PRICE_PRO_MONTHLY", "__pro__"): "premium",
        os.getenv("STRIPE_PRICE_ENTERPRISE_MONTHLY", "__ent__"): "enterprise",
    }


async def _find_user_by_customer(db: AsyncSession, customer_id: str) -> User | None:
    """Locate user whose preferences JSON contains the given Stripe customer_id."""
    result = await db.execute(select(User))
    users = result.scalars().all()
    for u in users:
        prefs = u.preferences or {}
        if prefs.get("stripe_customer_id") == customer_id:
            return u
    return None


@router.post("/stripe")
async def stripe_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Handle Stripe webhook events — signature-verified, idempotent.
    https://stripe.com/docs/webhooks
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    etype = event["type"]
    obj = event["data"]["object"]
    logger.info("stripe_webhook_received", event_type=etype, event_id=event["id"])

    await _dispatch(db, etype, obj)
    return {"status": "ok", "event": etype}


async def _dispatch(db: AsyncSession, etype: str, obj: dict):
    """Route webhook events to handler functions."""
    price_tier_map = _build_price_tier_map()
    customer_id = obj.get("customer")
    user = await _find_user_by_customer(db, customer_id) if customer_id else None

    if etype == "checkout.session.completed":
        await _on_checkout_completed(db, obj, user)

    elif etype == "customer.subscription.created":
        await _on_subscription_created(db, obj, user, price_tier_map)

    elif etype == "customer.subscription.updated":
        await _on_subscription_updated(db, obj, user, price_tier_map)

    elif etype == "customer.subscription.deleted":
        await _on_subscription_deleted(db, obj, user)

    elif etype == "invoice.payment_succeeded":
        logger.info("payment_succeeded", customer=customer_id, user_id=str(user.id) if user else None)

    elif etype == "invoice.payment_failed":
        await _on_payment_failed(db, obj, user)

    else:
        logger.info("stripe_webhook_unhandled", event_type=etype)


async def _on_checkout_completed(db: AsyncSession, obj: dict, user: User | None):
    if not user:
        logger.warning("checkout_completed_no_user", customer=obj.get("customer"))
        return

    sub_id = obj.get("subscription")
    plan = obj.get("metadata", {}).get("plan_type", "pro")
    db_tier = "premium" if plan == "pro" else plan

    prefs = dict(user.preferences or {})
    prefs["stripe_subscription_id"] = sub_id
    user.preferences = prefs
    user.tier = db_tier
    user.subscription_status = "active"
    await db.commit()
    logger.info("checkout_completed", user_id=str(user.id), plan=plan, tier=db_tier)


async def _on_subscription_created(
    db: AsyncSession, obj: dict, user: User | None, price_tier_map: dict
):
    if not user:
        return
    price_id = ""
    items_data = obj.get("items", {}).get("data", [])
    if items_data:
        price_id = items_data[0].get("price", {}).get("id", "")

    db_tier = price_tier_map.get(price_id, "premium")
    user.tier = db_tier
    user.subscription_status = obj.get("status", "active")

    prefs = dict(user.preferences or {})
    prefs["stripe_subscription_id"] = obj.get("id")
    prefs["stripe_period_end"] = obj.get("current_period_end")
    user.preferences = prefs
    await db.commit()
    logger.info("subscription_created", user_id=str(user.id), tier=db_tier)


async def _on_subscription_updated(
    db: AsyncSession, obj: dict, user: User | None, price_tier_map: dict
):
    if not user:
        return
    items_data = obj.get("items", {}).get("data", [])
    price_id = items_data[0].get("price", {}).get("id", "") if items_data else ""
    db_tier = price_tier_map.get(price_id, user.tier or "premium")

    user.tier = db_tier
    user.subscription_status = obj.get("status", "active")
    prefs = dict(user.preferences or {})
    prefs["stripe_cancel_at_period_end"] = obj.get("cancel_at_period_end", False)
    prefs["stripe_period_end"] = obj.get("current_period_end")
    user.preferences = prefs
    await db.commit()
    logger.info("subscription_updated", user_id=str(user.id), tier=db_tier,
                cancel_at_period_end=obj.get("cancel_at_period_end"))


async def _on_subscription_deleted(db: AsyncSession, obj: dict, user: User | None):
    if not user:
        return
    user.tier = "free"
    user.subscription_status = "canceled"
    prefs = dict(user.preferences or {})
    prefs.pop("stripe_subscription_id", None)
    prefs.pop("stripe_cancel_at_period_end", None)
    user.preferences = prefs
    await db.commit()
    logger.info("subscription_deleted", user_id=str(user.id))


async def _on_payment_failed(db: AsyncSession, obj: dict, user: User | None):
    if not user:
        return
    user.subscription_status = "past_due"
    await db.commit()
    logger.warning("payment_failed", user_id=str(user.id))


@router.get("/stripe/test")
async def test_webhook_endpoint():
    """Health-check: verify the webhook endpoint is reachable."""
    return {
        "status": "webhook endpoint active",
        "webhook_secret_configured": bool(WEBHOOK_SECRET),
        "test_mode": (stripe.api_key or "").startswith("sk_test_"),
    }
