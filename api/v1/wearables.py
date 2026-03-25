"""
Wearable Device Integration API.

OAuth-based integrations for Fitbit, Apple Health (via HealthKit API),
and Oura Ring. Follows the same pattern as integrations.py (GitHub/Google/Notion).
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional, Dict, Any, List
from datetime import datetime
import uuid
import httpx
import structlog
import os

from database.postgres import get_db
from auth.jwt_handler import get_current_user as get_current_user_from_token

logger = structlog.get_logger()
router = APIRouter()


# =====================================================
# DATABASE HELPERS
# =====================================================

TABLE_INIT = """
CREATE TABLE IF NOT EXISTS wearable_integrations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    provider TEXT NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT,
    token_expires_at TIMESTAMPTZ,
    external_user_id TEXT,
    device_name TEXT,
    provider_metadata JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    last_synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, provider)
);
CREATE INDEX IF NOT EXISTS idx_wearable_user ON wearable_integrations(user_id);

CREATE TABLE IF NOT EXISTS wearable_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    provider TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    metric_value FLOAT NOT NULL,
    metric_unit TEXT,
    recorded_at TIMESTAMPTZ NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wearable_data_user ON wearable_data(user_id, provider, metric_type);
CREATE INDEX IF NOT EXISTS idx_wearable_data_time ON wearable_data(recorded_at);
"""


async def _ensure_tables(db: AsyncSession):
    """Create wearable tables if they don't exist."""
    try:
        await db.execute(text(TABLE_INIT))
        await db.commit()
    except Exception:
        await db.rollback()


# =====================================================
# PROVIDER CONFIGURATION
# =====================================================

PROVIDERS = {
    "fitbit": {
        "name": "Fitbit",
        "auth_url": "https://www.fitbit.com/oauth2/authorize",
        "token_url": "https://api.fitbit.com/oauth2/token",
        "api_base": "https://api.fitbit.com",
        "scopes": ["activity", "heartrate", "sleep", "weight"],
        "env_client_id": "FITBIT_CLIENT_ID",
        "env_client_secret": "FITBIT_CLIENT_SECRET",
    },
    "oura": {
        "name": "Oura Ring",
        "auth_url": "https://cloud.ouraring.com/oauth/authorize",
        "token_url": "https://api.ouraring.com/oauth/token",
        "api_base": "https://api.ouraring.com/v2",
        "scopes": ["daily", "heartrate", "session", "sleep"],
        "env_client_id": "OURA_CLIENT_ID",
        "env_client_secret": "OURA_CLIENT_SECRET",
    },
    "apple_health": {
        "name": "Apple Health",
        "auth_url": None,  # Apple Health uses HealthKit — no web OAuth
        "token_url": None,
        "api_base": None,
        "scopes": [],
        "env_client_id": "APPLE_HEALTH_CLIENT_ID",
        "env_client_secret": "APPLE_HEALTH_CLIENT_SECRET",
        "note": "Apple Health data is synced via the mobile app using HealthKit SDK. This endpoint stores the sync token from the mobile client.",
    },
}

REDIRECT_URI_BASE = os.getenv("WEARABLE_REDIRECT_URI", "http://localhost:3000/auth/wearable/callback")


# =====================================================
# REQUEST/RESPONSE MODELS
# =====================================================

class OAuthInitResponse(BaseModel):
    auth_url: str
    provider: str
    state: str


class OAuthCallbackRequest(BaseModel):
    code: str
    state: Optional[str] = None


class WearableStatusResponse(BaseModel):
    provider: str
    name: str
    connected: bool
    device_name: Optional[str] = None
    last_synced: Optional[str] = None
    is_active: bool = False


class WearableDataPoint(BaseModel):
    metric_type: str
    metric_value: float
    metric_unit: Optional[str] = None
    recorded_at: str


class WearableDataResponse(BaseModel):
    provider: str
    metrics: List[Dict[str, Any]]
    period_days: int
    total_data_points: int


class MobileHealthSyncRequest(BaseModel):
    """For Apple Health data pushed from the mobile app."""
    provider: str = "apple_health"
    device_name: Optional[str] = None
    data_points: List[WearableDataPoint]


# =====================================================
# OAUTH FLOW ENDPOINTS
# =====================================================

@router.get("/connect/{provider}", response_model=OAuthInitResponse)
async def initiate_wearable_oauth(
    provider: str,
    current_user=Depends(get_current_user_from_token),
):
    """
    Start OAuth flow for a wearable provider (Fitbit or Oura).
    Returns the authorization URL for the user to visit.
    """
    if provider not in PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")

    config = PROVIDERS[provider]

    if not config["auth_url"]:
        raise HTTPException(
            status_code=400,
            detail=f"{config['name']} does not use web OAuth. Use the mobile app to sync.",
        )

    client_id = os.getenv(config["env_client_id"])
    if not client_id:
        raise HTTPException(
            status_code=500,
            detail=f"{config['name']} is not configured. Set {config['env_client_id']} env var.",
        )

    user_id = current_user["id"] if isinstance(current_user, dict) else str(current_user.id)
    state = f"{provider}:{user_id}:{uuid.uuid4().hex[:8]}"
    redirect_uri = f"{REDIRECT_URI_BASE}/{provider}"

    scopes = " ".join(config["scopes"])

    auth_url = (
        f"{config['auth_url']}"
        f"?response_type=code"
        f"&client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&scope={scopes}"
        f"&state={state}"
    )

    logger.info("wearable_oauth_initiated", provider=provider, user_id=user_id)

    return OAuthInitResponse(auth_url=auth_url, provider=provider, state=state)


@router.post("/callback/{provider}")
async def wearable_oauth_callback(
    provider: str,
    request: OAuthCallbackRequest,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Handle OAuth callback — exchange code for access token.
    """
    if provider not in PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")

    config = PROVIDERS[provider]
    if not config["token_url"]:
        raise HTTPException(status_code=400, detail=f"{config['name']} does not support OAuth callbacks.")

    client_id = os.getenv(config["env_client_id"])
    client_secret = os.getenv(config["env_client_secret"])

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail=f"{config['name']} credentials not configured.")

    await _ensure_tables(db)

    user_id = current_user["id"] if isinstance(current_user, dict) else str(current_user.id)
    redirect_uri = f"{REDIRECT_URI_BASE}/{provider}"

    # Exchange code for token
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            config["token_url"],
            data={
                "grant_type": "authorization_code",
                "code": request.code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if token_response.status_code != 200:
        logger.error(
            "wearable_token_exchange_failed",
            provider=provider,
            status=token_response.status_code,
            body=token_response.text[:500],
        )
        raise HTTPException(status_code=502, detail=f"Token exchange failed with {config['name']}")

    token_data = token_response.json()

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = token_data.get("expires_in", 3600)
    external_user_id = token_data.get("user_id", "")

    token_expires = datetime.utcnow()
    if expires_in:
        from datetime import timedelta
        token_expires += timedelta(seconds=int(expires_in))

    # Upsert integration
    await db.execute(
        text("""
            INSERT INTO wearable_integrations
                (user_id, provider, access_token, refresh_token, token_expires_at,
                 external_user_id, is_active, updated_at)
            VALUES (:uid, :provider, :access, :refresh, :expires, :ext_uid, true, NOW())
            ON CONFLICT (user_id, provider) DO UPDATE SET
                access_token = :access,
                refresh_token = :refresh,
                token_expires_at = :expires,
                external_user_id = :ext_uid,
                is_active = true,
                updated_at = NOW()
        """),
        {
            "uid": user_id,
            "provider": provider,
            "access": access_token,
            "refresh": refresh_token,
            "expires": token_expires,
            "ext_uid": external_user_id,
        },
    )
    await db.commit()

    logger.info("wearable_connected", provider=provider, user_id=user_id)

    return {
        "connected": True,
        "provider": provider,
        "name": config["name"],
        "message": f"Successfully connected {config['name']}",
    }


# =====================================================
# STATUS & MANAGEMENT
# =====================================================


@router.get("/status", response_model=List[WearableStatusResponse])
async def get_all_wearable_status(
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Get connection status of all wearable providers."""
    await _ensure_tables(db)

    user_id = current_user["id"] if isinstance(current_user, dict) else str(current_user.id)

    result = await db.execute(
        text("""
            SELECT provider, device_name, last_synced_at, is_active
            FROM wearable_integrations
            WHERE user_id = :uid
        """),
        {"uid": user_id},
    )

    connected = {}
    for row in result.fetchall():
        connected[row[0]] = {
            "device_name": row[1],
            "last_synced": row[2].isoformat() if row[2] else None,
            "is_active": row[3],
        }

    statuses = []
    for provider_key, config in PROVIDERS.items():
        conn = connected.get(provider_key, {})
        statuses.append(WearableStatusResponse(
            provider=provider_key,
            name=config["name"],
            connected=provider_key in connected,
            device_name=conn.get("device_name"),
            last_synced=conn.get("last_synced"),
            is_active=conn.get("is_active", False),
        ))

    return statuses


@router.delete("/disconnect/{provider}")
async def disconnect_wearable(
    provider: str,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Disconnect a wearable provider and optionally delete synced data."""
    await _ensure_tables(db)

    user_id = current_user["id"] if isinstance(current_user, dict) else str(current_user.id)

    result = await db.execute(
        text("DELETE FROM wearable_integrations WHERE user_id = :uid AND provider = :p"),
        {"uid": user_id, "p": provider},
    )
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"No {provider} connection found")

    logger.info("wearable_disconnected", provider=provider, user_id=user_id)

    return {"message": f"Disconnected {provider}", "provider": provider}


# =====================================================
# DATA ENDPOINTS
# =====================================================


@router.get("/data", response_model=WearableDataResponse)
async def get_wearable_data(
    provider: Optional[str] = None,
    metric_type: Optional[str] = None,
    days: int = Query(7, ge=1, le=90),
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Get wearable data (steps, heart rate, sleep, etc.) for the current user.
    Optionally filter by provider and metric type.
    """
    await _ensure_tables(db)

    user_id = current_user["id"] if isinstance(current_user, dict) else str(current_user.id)

    from datetime import timedelta
    cutoff = datetime.utcnow() - timedelta(days=days)

    query = """
        SELECT metric_type, metric_value, metric_unit, recorded_at, provider, metadata
        FROM wearable_data
        WHERE user_id = :uid AND recorded_at >= :cutoff
    """
    params: Dict[str, Any] = {"uid": user_id, "cutoff": cutoff}

    if provider:
        query += " AND provider = :provider"
        params["provider"] = provider
    if metric_type:
        query += " AND metric_type = :metric"
        params["metric"] = metric_type

    query += " ORDER BY recorded_at DESC LIMIT 1000"

    result = await db.execute(text(query), params)
    rows = result.fetchall()

    metrics = []
    for row in rows:
        metrics.append({
            "metric_type": row[0],
            "value": row[1],
            "unit": row[2],
            "recorded_at": row[3].isoformat() if row[3] else "",
            "provider": row[4],
            "metadata": row[5] or {},
        })

    return WearableDataResponse(
        provider=provider or "all",
        metrics=metrics,
        period_days=days,
        total_data_points=len(metrics),
    )


@router.post("/sync/mobile")
async def sync_mobile_health_data(
    sync_data: MobileHealthSyncRequest,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Receive health data pushed from the mobile app (Apple Health / Google Fit).
    The mobile client collects data via native SDK and posts it here.
    """
    await _ensure_tables(db)

    user_id = current_user["id"] if isinstance(current_user, dict) else str(current_user.id)

    inserted = 0
    for dp in sync_data.data_points:
        await db.execute(
            text("""
                INSERT INTO wearable_data (user_id, provider, metric_type, metric_value, metric_unit, recorded_at)
                VALUES (:uid, :provider, :type, :value, :unit, :recorded)
            """),
            {
                "uid": user_id,
                "provider": sync_data.provider,
                "type": dp.metric_type,
                "value": dp.metric_value,
                "unit": dp.metric_unit,
                "recorded": dp.recorded_at,
            },
        )
        inserted += 1

    await db.commit()

    # Update last_synced
    await db.execute(
        text("""
            INSERT INTO wearable_integrations (user_id, provider, access_token, last_synced_at, device_name, is_active)
            VALUES (:uid, :provider, 'mobile-sync', NOW(), :device, true)
            ON CONFLICT (user_id, provider) DO UPDATE SET
                last_synced_at = NOW(),
                device_name = COALESCE(:device, wearable_integrations.device_name)
        """),
        {"uid": user_id, "provider": sync_data.provider, "device": sync_data.device_name},
    )
    await db.commit()

    logger.info("wearable_mobile_sync", provider=sync_data.provider, user_id=user_id, count=inserted)

    return {
        "synced": inserted,
        "provider": sync_data.provider,
        "message": f"Synced {inserted} data points from {sync_data.provider}",
    }
