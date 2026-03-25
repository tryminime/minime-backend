"""
Cloud Backup API — Phase 2

Endpoints:
  GET  /api/v1/sync/status
  POST /api/v1/sync/backup

  POST   /api/v1/sync/gdrive/connect
  GET    /api/v1/sync/gdrive/callback
  POST   /api/v1/sync/gdrive/upload
  POST   /api/v1/sync/gdrive/download
  GET    /api/v1/sync/gdrive/status
  DELETE /api/v1/sync/gdrive/disconnect

  POST   /api/v1/sync/onedrive/connect
  GET    /api/v1/sync/onedrive/callback
  POST   /api/v1/sync/onedrive/upload
  POST   /api/v1/sync/onedrive/download
  GET    /api/v1/sync/onedrive/status
  DELETE /api/v1/sync/onedrive/disconnect

Token storage: in-memory dict (keyed by user_id).
Production: replace with encrypted DB columns.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, JSONResponse
from pydantic import BaseModel
import structlog

from services.cloud_sync import (
    GoogleDriveSync, OneDriveSync,
    encrypt_payload, decrypt_payload,
    build_manifest, dedup_snapshots, merge_snapshots,
    key_fingerprint,
    get_gdrive_sync, get_onedrive_sync,
    MANIFEST_FILENAME,
)
from auth.jwt_handler import get_current_user
from database.postgres import get_db
from models import User
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from middleware.feature_gate import FeatureGate

logger = structlog.get_logger()
router = APIRouter(prefix="/api/v1/sync", tags=["Cloud Sync"])

# Frontend base URL — redirect OAuth callbacks here, not to the backend host
_FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

# ── Auth helper ────────────────────────────────────────────────────────────────

async def _require_user(
    token_data: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Return the full User ORM object for the authenticated request.
    get_current_user validates the JWT and returns a dict with 'id'.
    We then load the User from DB so FeatureGate can read user.tier.
    """
    user_id = token_data.get("id") or token_data.get("sub")
    if not user_id:
        raise HTTPException(401, "Invalid token payload")
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user or user.deleted_at is not None:
        raise HTTPException(401, "User not found or inactive")
    return user


# ── In-memory token store (replace with encrypted DB in production) ───────────
# Structure: { user_id: { "gdrive": {...}, "onedrive": {...} } }
_token_store: Dict[str, Dict[str, Any]] = {}

# OAuth state → user_id mapping (short-lived)
_oauth_states: Dict[str, str] = {}

# ── Sync metadata ──────────────────────────────────────────────────────────────
_sync_meta: Dict[str, Dict[str, Any]] = {}


def _get_sync_key() -> bytes:
    """Get or generate a per-instance AES-256 key.

    Production: derive from user password + stored salt, or use OS keychain.
    For now: stable key derived from a secret env var.
    """
    secret = os.getenv("SYNC_ENCRYPTION_SECRET", "minime-dev-sync-secret-change-in-prod")
    salt = b"minime_sync_v1"
    return hashlib.pbkdf2_hmac("sha256", secret.encode(), salt, 100_000, dklen=32)


def _snapshot_filename() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"minime_snapshot_{ts}.enc"


# ══════════════════════════════════════════════════════════════════════════════
# Combined status
# ══════════════════════════════════════════════════════════════════════════════
def _get_tokens(user_id: str, provider: str) -> Optional[dict]:
    return _token_store.get(user_id, {}).get(provider)


def _set_tokens(user_id: str, provider: str, tokens: dict):
    if user_id not in _token_store:
        _token_store[user_id] = {}
    _token_store[user_id][provider] = tokens


def _clear_tokens(user_id: str, provider: str):
    if user_id in _token_store and provider in _token_store[user_id]:
        del _token_store[user_id][provider]

@router.get("/status")
async def get_combined_sync_status(user: User = Depends(_require_user)):
    """Return connection + last-sync status for all providers. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    gdrive_tokens = _get_tokens(user_id, "gdrive")
    onedrive_tokens = _get_tokens(user_id, "onedrive")
    meta = _sync_meta.get(user_id, {})

    gdrive_user: Optional[dict] = None
    if gdrive_tokens:
        try:
            gdrive_user = get_gdrive_sync().get_user_info(gdrive_tokens)
        except Exception:
            gdrive_user = None

    onedrive_user: Optional[dict] = None
    if onedrive_tokens:
        try:
            onedrive_user = get_onedrive_sync().get_user_info(onedrive_tokens)
        except Exception:
            onedrive_user = None

    key = _get_sync_key()
    return {
        "encryption": {
            "algorithm": "AES-256-GCM",
            "key_fingerprint": key_fingerprint(key),
        },
        "providers": {
            "gdrive": {
                "connected": gdrive_tokens is not None,
                "account": gdrive_user,
                "last_sync": meta.get("gdrive_last_sync"),
                "last_snapshot_size": meta.get("gdrive_last_size"),
                "snapshot_count": meta.get("gdrive_snapshot_count", 0),
            },
            "onedrive": {
                "connected": onedrive_tokens is not None,
                "account": onedrive_user,
                "last_sync": meta.get("onedrive_last_sync"),
                "last_snapshot_size": meta.get("onedrive_last_size"),
                "snapshot_count": meta.get("onedrive_snapshot_count", 0),
            },
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# Auto-backup (both connected providers)
# ══════════════════════════════════════════════════════════════════════════════

class BackupRequest(BaseModel):
    data: list[dict] = []   # list of activity records to back up
    incremental: bool = True   # dedup before uploading


@router.post("/backup")
async def backup_now(req: BackupRequest, user: User = Depends(_require_user)):
    """Encrypt data and upload to all connected providers. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    key = _get_sync_key()
    records = req.data or []
    results = {}

    for provider in ("gdrive", "onedrive"):
        tokens = _get_tokens(user_id, provider)
        if not tokens:
            results[provider] = {"skipped": True, "reason": "not connected"}
            continue
        try:
            # Build / fetch manifest for dedup
            manifest_bytes: Optional[bytes] = None
            try:
                if provider == "gdrive":
                    manifest_bytes = get_gdrive_sync().download(tokens, MANIFEST_FILENAME)
                else:
                    manifest_bytes = get_onedrive_sync().download(tokens, MANIFEST_FILENAME)
            except Exception:
                pass

            old_manifest = json.loads(manifest_bytes.decode()) if manifest_bytes else {}
            to_upload = dedup_snapshots(old_manifest, records) if req.incremental else records

            # Encrypt snapshot
            payload_json = json.dumps(to_upload, default=str).encode()
            encrypted = encrypt_payload(payload_json, key)
            filename = _snapshot_filename()

            # Upload snapshot + updated manifest
            new_manifest = build_manifest(records)  # manifest always reflects full state
            manifest_enc = encrypt_payload(json.dumps(new_manifest).encode(), key)

            if provider == "gdrive":
                svc = get_gdrive_sync()
                svc.upload(tokens, encrypted, filename)
                svc.upload(tokens, manifest_enc, MANIFEST_FILENAME)
                snapshots = svc.list_snapshots(tokens)
            else:
                svc = get_onedrive_sync()
                svc.upload(tokens, encrypted, filename)
                svc.upload(tokens, manifest_enc, MANIFEST_FILENAME)
                snapshots = svc.list_snapshots(tokens)

            meta = _sync_meta.setdefault(user_id, {})
            meta[f"{provider}_last_sync"] = datetime.now(timezone.utc).isoformat()
            meta[f"{provider}_last_size"] = len(encrypted)
            meta[f"{provider}_snapshot_count"] = len(snapshots)

            results[provider] = {
                "success": True,
                "filename": filename,
                "records_uploaded": len(to_upload),
                "encrypted_size_bytes": len(encrypted),
            }
            logger.info("backup_complete", provider=provider, records=len(to_upload))
        except Exception as e:
            logger.error("backup_failed", provider=provider, error=str(e))
            results[provider] = {"success": False, "error": str(e)}

    return {"results": results, "backed_up_at": datetime.now(timezone.utc).isoformat()}


# ══════════════════════════════════════════════════════════════════════════════
# Google Drive endpoints
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/gdrive/connect")
async def gdrive_connect(user: User = Depends(_require_user)):
    """Initiate Google Drive OAuth2 flow. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    state = secrets.token_urlsafe(16)
    _oauth_states[state] = user_id
    try:
        auth_url = get_gdrive_sync().get_auth_url(state=state)
        return {"auth_url": auth_url, "provider": "gdrive"}
    except Exception as e:
        raise HTTPException(502, f"Could not build Google OAuth URL: {e}")


@router.get("/gdrive/callback")
async def gdrive_callback(code: str = Query(...), state: str = Query("")):
    """Handle Google OAuth2 callback. Stores tokens and redirects to settings."""
    # Callbacks are browser redirects — no Bearer token available.
    # user_id was seeded into _oauth_states during the authenticated /connect call.
    user_id = _oauth_states.pop(state, "")
    if not user_id:
        return RedirectResponse(url=f"{_FRONTEND_URL}/dashboard/settings?tab=sync&error=invalid_oauth_state")
    try:
        tokens = get_gdrive_sync().exchange_code(code, state=state)
        _set_tokens(user_id, "gdrive", tokens)
        logger.info("gdrive_connected", user_id=user_id)
    except Exception as e:
        import traceback, urllib.parse
        tb = traceback.format_exc()
        logger.error("gdrive_callback_failed", error=str(e), traceback=tb)
        err_msg = urllib.parse.quote(str(e)[:120], safe="")
        return RedirectResponse(url=f"{_FRONTEND_URL}/dashboard/settings?tab=sync&error=gdrive_auth_failed&detail={err_msg}")
    return RedirectResponse(url=f"{_FRONTEND_URL}/dashboard/settings?tab=sync&connected=gdrive")


@router.get("/gdrive/status")
async def gdrive_status(user: User = Depends(_require_user)):
    """Google Drive connection status. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    tokens = _get_tokens(user_id, "gdrive")
    if not tokens:
        return {"connected": False}
    try:
        info = get_gdrive_sync().get_user_info(tokens)
        snapshots = get_gdrive_sync().list_snapshots(tokens)
        meta = _sync_meta.get(user_id, {})
        return {
            "connected": True,
            "account": info,
            "last_sync": meta.get("gdrive_last_sync"),
            "snapshot_count": len(snapshots),
            "snapshots": snapshots[:5],  # latest 5
        }
    except Exception as e:
        return {"connected": False, "error": str(e)}


class UploadRequest(BaseModel):
    data: list[dict]
    incremental: bool = True


@router.post("/gdrive/upload")
async def gdrive_upload(req: UploadRequest, user: User = Depends(_require_user)):
    """Encrypt and upload data to Google Drive. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    tokens = _get_tokens(user_id, "gdrive")
    if not tokens:
        raise HTTPException(401, "Google Drive not connected")

    key = _get_sync_key()
    svc = get_gdrive_sync()
    try:
        # Dedup
        manifest_bytes = svc.download(tokens, MANIFEST_FILENAME)
        old_manifest = json.loads(manifest_bytes.decode()) if manifest_bytes else {}
        to_upload = dedup_snapshots(old_manifest, req.data) if req.incremental else req.data

        payload_json = json.dumps(to_upload, default=str).encode()
        encrypted = encrypt_payload(payload_json, key)
        filename = _snapshot_filename()
        svc.upload(tokens, encrypted, filename)

        # Update manifest
        new_manifest = build_manifest(req.data)
        svc.upload(tokens, encrypt_payload(json.dumps(new_manifest).encode(), key), MANIFEST_FILENAME)

        meta = _sync_meta.setdefault(user_id, {})
        meta["gdrive_last_sync"] = datetime.now(timezone.utc).isoformat()
        meta["gdrive_last_size"] = len(encrypted)
        return {"success": True, "filename": filename, "records_uploaded": len(to_upload),
                "encrypted_size_bytes": len(encrypted)}
    except Exception as e:
        logger.error("gdrive_upload_error", error=str(e))
        raise HTTPException(500, str(e))


class DownloadRequest(BaseModel):
    filename: Optional[str] = None   # None = latest


@router.post("/gdrive/download")
async def gdrive_download(req: DownloadRequest, user: User = Depends(_require_user)):
    """Download and decrypt latest snapshot from Google Drive. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    tokens = _get_tokens(user_id, "gdrive")
    if not tokens:
        raise HTTPException(401, "Google Drive not connected")

    key = _get_sync_key()
    svc = get_gdrive_sync()
    try:
        # Find latest snapshot
        if req.filename:
            filename = req.filename
        else:
            snapshots = svc.list_snapshots(tokens)
            snap_files = [s for s in snapshots if s.get("name", "").startswith("minime_snapshot_")]
            if not snap_files:
                return {"records": [], "message": "No snapshots found"}
            filename = snap_files[0]["name"]

        encrypted = svc.download(tokens, filename)
        if encrypted is None:
            raise HTTPException(404, f"File {filename} not found in Google Drive")

        decrypted = decrypt_payload(encrypted, key)
        records = json.loads(decrypted.decode())
        return {"records": records, "filename": filename, "count": len(records)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("gdrive_download_error", error=str(e))
        raise HTTPException(500, str(e))


@router.delete("/gdrive/disconnect")
async def gdrive_disconnect(user: User = Depends(_require_user)):
    """Revoke Google Drive connection."""
    # Note: disconnecting doesn't require cloud_sync gate — user should always be able to unlink
    user_id = str(user.id)
    _clear_tokens(user_id, "gdrive")
    return {"disconnected": True, "provider": "gdrive"}


# ══════════════════════════════════════════════════════════════════════════════
# OneDrive endpoints (mirrors Google Drive structure)
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/onedrive/connect")
async def onedrive_connect(user: User = Depends(_require_user)):
    """Initiate OneDrive OAuth2 flow. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    state = secrets.token_urlsafe(16)
    _oauth_states[state] = user_id
    try:
        auth_url = get_onedrive_sync().get_auth_url(state=state)
        return {"auth_url": auth_url, "provider": "onedrive"}
    except Exception as e:
        raise HTTPException(502, f"Could not build Microsoft OAuth URL: {e}")


@router.get("/onedrive/callback")
async def onedrive_callback(code: str = Query(...), state: str = Query(""),
                             session_state: str = Query("")):
    """Handle Microsoft OAuth2 callback."""
    # Callbacks are browser redirects — no Bearer token available.
    # user_id was seeded into _oauth_states during the authenticated /connect call.
    user_id = _oauth_states.pop(state, "")
    if not user_id:
        return RedirectResponse(url=f"{_FRONTEND_URL}/dashboard/settings?tab=sync&error=invalid_oauth_state")
    try:
        tokens = get_onedrive_sync().exchange_code(code)
        _set_tokens(user_id, "onedrive", tokens)
        logger.info("onedrive_connected", user_id=user_id)
    except Exception as e:
        logger.error("onedrive_callback_failed", error=str(e))
        return RedirectResponse(url=f"{_FRONTEND_URL}/dashboard/settings?tab=sync&error=onedrive_auth_failed")
    return RedirectResponse(url=f"{_FRONTEND_URL}/dashboard/settings?tab=sync&connected=onedrive")


@router.get("/onedrive/status")
async def onedrive_status(user: User = Depends(_require_user)):
    """OneDrive connection status. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    tokens = _get_tokens(user_id, "onedrive")
    if not tokens:
        return {"connected": False}
    try:
        info = get_onedrive_sync().get_user_info(tokens)
        snapshots = get_onedrive_sync().list_snapshots(tokens)
        meta = _sync_meta.get(user_id, {})
        return {
            "connected": True,
            "account": info,
            "last_sync": meta.get("onedrive_last_sync"),
            "snapshot_count": len(snapshots),
            "snapshots": snapshots[:5],
        }
    except Exception as e:
        return {"connected": False, "error": str(e)}


@router.post("/onedrive/upload")
async def onedrive_upload(req: UploadRequest, user: User = Depends(_require_user)):
    """Encrypt and upload data to OneDrive. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    tokens = _get_tokens(user_id, "onedrive")
    if not tokens:
        raise HTTPException(401, "OneDrive not connected")

    key = _get_sync_key()
    svc = get_onedrive_sync()
    try:
        manifest_bytes = svc.download(tokens, MANIFEST_FILENAME)
        old_manifest = json.loads(manifest_bytes.decode()) if manifest_bytes else {}
        to_upload = dedup_snapshots(old_manifest, req.data) if req.incremental else req.data

        payload_json = json.dumps(to_upload, default=str).encode()
        encrypted = encrypt_payload(payload_json, key)
        filename = _snapshot_filename()
        svc.upload(tokens, encrypted, filename)

        new_manifest = build_manifest(req.data)
        svc.upload(tokens, encrypt_payload(json.dumps(new_manifest).encode(), key), MANIFEST_FILENAME)

        meta = _sync_meta.setdefault(user_id, {})
        meta["onedrive_last_sync"] = datetime.now(timezone.utc).isoformat()
        meta["onedrive_last_size"] = len(encrypted)
        return {"success": True, "filename": filename, "records_uploaded": len(to_upload),
                "encrypted_size_bytes": len(encrypted)}
    except Exception as e:
        logger.error("onedrive_upload_error", error=str(e))
        raise HTTPException(500, str(e))


@router.post("/onedrive/download")
async def onedrive_download(req: DownloadRequest, user: User = Depends(_require_user)):
    """Download and decrypt latest snapshot from OneDrive. Requires Pro/Enterprise."""
    FeatureGate(user).require("cloud_sync")
    user_id = str(user.id)
    tokens = _get_tokens(user_id, "onedrive")
    if not tokens:
        raise HTTPException(401, "OneDrive not connected")

    key = _get_sync_key()
    svc = get_onedrive_sync()
    try:
        if req.filename:
            filename = req.filename
        else:
            snapshots = svc.list_snapshots(tokens)
            snap_files = [s for s in snapshots if s.get("name", "").startswith("minime_snapshot_")]
            if not snap_files:
                return {"records": [], "message": "No snapshots found"}
            filename = snap_files[0]["name"]

        encrypted = svc.download(tokens, filename)
        if encrypted is None:
            raise HTTPException(404, f"File {filename} not found in OneDrive")

        decrypted = decrypt_payload(encrypted, key)
        records = json.loads(decrypted.decode())
        return {"records": records, "filename": filename, "count": len(records)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.delete("/onedrive/disconnect")
async def onedrive_disconnect(user: User = Depends(_require_user)):
    """Revoke OneDrive connection."""
    # Disconnecting doesn't require the cloud_sync gate — always allow unlinking
    user_id = str(user.id)
    _clear_tokens(user_id, "onedrive")
    return {"disconnected": True, "provider": "onedrive"}
