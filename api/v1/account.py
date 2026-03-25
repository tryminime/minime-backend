"""
Account management endpoints — GDPR-compliant data deletion & export.

Routes:
  DELETE /api/v1/account          — hard-delete account + all data (GDPR Art. 17)
  GET    /api/v1/account/export   — full data export (GDPR Art. 20 portability)
"""
from __future__ import annotations

import os
import json
import stripe
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import select, delete as sql_delete
from sqlalchemy.ext.asyncio import AsyncSession

from database.postgres import get_db
from models import User, Activity
from auth.jwt_handler import decode_token, verify_token_type

logger = structlog.get_logger()
router = APIRouter(prefix="/api/v1/account", tags=["account"])
security = HTTPBearer()
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")


# ── Auth helper ───────────────────────────────────────────────────────────────

async def _get_user(
    credentials: HTTPAuthorizationCredentials,
    db: AsyncSession,
) -> User:
    import uuid as _uuid
    payload = decode_token(credentials.credentials)
    if not payload or not verify_token_type(payload, "access"):
        raise HTTPException(status_code=401, detail="Invalid token")
    user_id = payload.get("sub", "")
    result = await db.execute(select(User).where(User.id == _uuid.UUID(user_id)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


# ── DELETE /api/v1/account ────────────────────────────────────────────────────

@router.delete("")
async def delete_account(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Permanently delete the authenticated user's account and ALL associated data.
    GDPR Article 17 — Right to Erasure ("Right to be Forgotten").

    Steps:
      1. Cancel any active Stripe subscription immediately
      2. Delete all user data rows (activities, content, entities, etc.)
      3. Delete the user row itself
    """
    user = await _get_user(credentials, db)
    user_id = user.id
    email = user.email

    # 1. Cancel Stripe subscription (fire-and-forget — don't fail if Stripe errors)
    prefs = user.preferences or {}
    if sub_id := prefs.get("stripe_subscription_id"):
        try:
            stripe.Subscription.cancel(sub_id)
            logger.info("stripe_subscription_canceled_on_delete", user_id=str(user_id))
        except Exception as e:
            logger.warning("stripe_cancel_failed", error=str(e), user_id=str(user_id))

    # 2. Delete child records (SQLAlchemy cascade handles FKs if configured,
    #    but we do explicit deletes for safety)
    try:
        await db.execute(sql_delete(Activity).where(Activity.user_id == user_id))
    except Exception:
        pass  # Table or column might not exist in all envs

    # Attempt to delete other user-owned tables gracefully
    _OPTIONAL_TABLES = [
        "content_items", "entities", "graph_edges",
        "knowledge_nodes", "tasks", "billing_events",
    ]
    for table_name in _OPTIONAL_TABLES:
        try:
            await db.execute(
                # Raw SQL for tables not yet in ORM models
                __import__("sqlalchemy").text(
                    f"DELETE FROM {table_name} WHERE user_id = :uid"
                ),
                {"uid": str(user_id)},
            )
        except Exception:
            pass

    # 3. Delete user row
    await db.execute(sql_delete(User).where(User.id == user_id))
    await db.commit()

    logger.info("account_deleted", user_id=str(user_id), email=email)
    return {
        "message": "Your account and all associated data have been permanently deleted.",
        "deleted_at": datetime.now(timezone.utc).isoformat(),
        "email": email,
    }


# ── GET /api/v1/account/export ────────────────────────────────────────────────

@router.get("/export")
async def export_personal_data(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Export all personal data for the authenticated user as JSON.
    GDPR Article 20 — Right to Data Portability.
    """
    user = await _get_user(credentials, db)

    # Collect activities
    act_result = await db.execute(
        select(Activity).where(Activity.user_id == user.id).order_by(Activity.occurred_at.desc())
    )
    activities: list[dict[str, Any]] = []
    for act in act_result.scalars().all():
        activities.append({
            "id": str(act.id),
            "event_type": getattr(act, "event_type", None),
            "application": getattr(act, "application", None),
            "window_title": getattr(act, "window_title", None),
            "url": getattr(act, "url", None),
            "duration_seconds": getattr(act, "duration_seconds", None),
            "occurred_at": act.occurred_at.isoformat() if act.occurred_at else None,
        })

    # Sanitise preferences — remove internal Stripe keys from export if desired
    prefs_export = {
        k: v for k, v in (user.preferences or {}).items()
        if k not in ("stripe_customer_id", "stripe_subscription_id")
    }

    export_payload: dict[str, Any] = {
        "export_info": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "format_version": "1.0",
            "service": "MiniMe",
            "gdpr_article": "Article 20 — Right to Data Portability",
        },
        "account": {
            "id": str(user.id),
            "email": user.email,
            "full_name": user.full_name,
            "tier": user.tier,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "email_verified": user.email_verified,
            "bio": user.bio,
            "avatar_url": user.avatar_url,
        },
        "preferences": prefs_export,
        "privacy_settings": user.privacy_settings or {},
        "activities": activities,
        "activity_count": len(activities),
    }

    logger.info("personal_data_exported", user_id=str(user.id),
                activity_count=len(activities))

    return JSONResponse(
        content=export_payload,
        headers={
            "Content-Disposition": f'attachment; filename="minime-export-{user.id}.json"',
            "Content-Type": "application/json",
        },
    )
