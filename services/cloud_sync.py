"""
Cloud Sync Service — Phase 2

Provides:
  • AES-256-GCM helpers (encrypt_payload / decrypt_payload)
  • GoogleDriveSync  — OAuth2 + upload/download via Google Drive API
  • OneDriveSync     — MSAL + upload/download via Microsoft Graph API
  • dedup_snapshots  — content-hash delta to avoid redundant uploads

Credentials are stored in the database (encrypted).  OAuth flow is
initiated from the API layer; tokens and state are passed in here.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

logger = structlog.get_logger()

# Stores in-progress OAuth flows keyed by state, so the PKCE code_verifier
# generated inside the Flow during get_auth_url() is preserved for exchange_code().
_gdrive_pending_flows: dict = {}

# ── Manifest filename stored in each cloud provider ───────────────────────────
MANIFEST_FILENAME = "minime_manifest.json"
SNAPSHOT_FILENAME = "minime_snapshot_{ts}.enc"


# ═══════════════════════════════════════════════════════════════════════════════
# AES-256-GCM Encryption helpers
# ═══════════════════════════════════════════════════════════════════════════════

def encrypt_payload(data: bytes, key: bytes) -> bytes:
    """Encrypt *data* with AES-256-GCM using *key* (32 bytes).

    Wire format:  | 12-byte nonce | ciphertext | 16-byte GCM tag |
    Returns raw bytes (not base64).
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    if len(key) != 32:
        raise ValueError("key must be exactly 32 bytes for AES-256-GCM")
    nonce = os.urandom(12)
    aesgcm = AESGCM(key)
    ct = aesgcm.encrypt(nonce, data, None)   # includes tag appended by cryptography
    return nonce + ct


def decrypt_payload(ciphertext: bytes, key: bytes) -> bytes:
    """Decrypt data produced by *encrypt_payload*."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    if len(ciphertext) < 28:   # 12 nonce + 16 tag minimum
        raise ValueError("ciphertext too short")
    nonce, ct = ciphertext[:12], ciphertext[12:]
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ct, None)


def derive_key_from_password(password: str, salt: bytes) -> bytes:
    """Derive a 32-byte key from a password using PBKDF2-HMAC-SHA256."""
    import hashlib
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000, dklen=32)


def key_fingerprint(key: bytes) -> str:
    """Return last 8 hex chars of SHA-256(key) for display."""
    return hashlib.sha256(key).hexdigest()[-8:].upper()


# ═══════════════════════════════════════════════════════════════════════════════
# Dedup helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _record_hash(record: dict) -> str:
    """Stable hash for a data record.  Only content fields (not sync metadata)."""
    stable = {k: v for k, v in sorted(record.items()) if k not in ("sync_hash",)}
    raw = json.dumps(stable, sort_keys=True, default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def build_manifest(records: list[dict]) -> dict:
    """Build a content-hash manifest for a list of data records."""
    return {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "count": len(records),
        "hashes": {str(r.get("id", i)): _record_hash(r) for i, r in enumerate(records)},
    }


def dedup_snapshots(old_manifest: dict, records: list[dict]) -> list[dict]:
    """Return only records that differ from *old_manifest*.

    If no old manifest exists (first sync), returns all records.
    """
    if not old_manifest or "hashes" not in old_manifest:
        return records
    old_hashes = old_manifest["hashes"]
    changed = []
    for r in records:
        rid = str(r.get("id", ""))
        if old_hashes.get(rid) != _record_hash(r):
            changed.append(r)
    logger.info("dedup_result", total=len(records), changed=len(changed))
    return changed


def merge_snapshots(local: list[dict], remote: list[dict]) -> list[dict]:
    """Merge remote snapshot into local data.  Local wins on conflict
    (same id, local updated_at >= remote updated_at).
    """
    local_map = {str(r.get("id", "")): r for r in local}
    for r in remote:
        rid = str(r.get("id", ""))
        if rid not in local_map:
            local_map[rid] = r
        else:
            local_ts = local_map[rid].get("updated_at", "")
            remote_ts = r.get("updated_at", "")
            if remote_ts > local_ts:   # remote is newer — take it
                local_map[rid] = r
    return list(local_map.values())


# ═══════════════════════════════════════════════════════════════════════════════
# Google Drive Sync
# ═══════════════════════════════════════════════════════════════════════════════

GDRIVE_FOLDER_NAME = "MiniMe Backups"
GDRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.file",   # only files created by this app
]


class GoogleDriveSync:
    """Manages encrypted backups in a private Google Drive folder."""

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

    # ── OAuth2 flow ──────────────────────────────────────────────────────────

    def get_auth_url(self, state: str = "") -> str:
        """Return Google OAuth2 authorization URL."""
        import os as _os
        from google_auth_oauthlib.flow import Flow   # type: ignore
        _os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")
        flow = self._make_flow()
        auth_url, _ = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
            state=state,
        )
        # Store the flow so exchange_code can reuse it (preserves PKCE verifier)
        if state:
            _gdrive_pending_flows[state] = flow
        return auth_url

    def exchange_code(self, code: str, state: str = "") -> dict:
        """Exchange authorization code for tokens. Returns token dict."""
        import os as _os
        from google_auth_oauthlib.flow import Flow   # type: ignore
        _os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")
        # Reuse the stored flow so the PKCE code_verifier is preserved.
        # Fallback to a fresh flow if the state wasn't found (shouldn't happen).
        flow = _gdrive_pending_flows.pop(state, None) if state else None
        if flow is None:
            flow = self._make_flow()
        flow.fetch_token(code=code)
        creds = flow.credentials
        return {
            "access_token": creds.token,
            "refresh_token": creds.refresh_token,
            "token_uri": creds.token_uri,
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "scopes": list(creds.scopes or []),
            "expiry": creds.expiry.isoformat() if creds.expiry else None,
        }

    def _make_flow(self):
        from google_auth_oauthlib.flow import Flow   # type: ignore
        client_config = {
            "web": {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uris": [self.redirect_uri],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }
        return Flow.from_client_config(client_config, scopes=GDRIVE_SCOPES,
                                       redirect_uri=self.redirect_uri)

    def _build_service(self, token_dict: dict):
        """Build a Google Drive service from stored token dict."""
        import google.oauth2.credentials as gc   # type: ignore
        from googleapiclient.discovery import build   # type: ignore
        creds = gc.Credentials(
            token=token_dict["access_token"],
            refresh_token=token_dict.get("refresh_token"),
            token_uri=token_dict.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=token_dict.get("client_id", self.client_id),
            client_secret=token_dict.get("client_secret", self.client_secret),
            scopes=token_dict.get("scopes", GDRIVE_SCOPES),
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    # ── Folder helpers ───────────────────────────────────────────────────────

    def _get_or_create_folder(self, service) -> str:
        """Return the Drive folder ID for MiniMe backups."""
        query = (
            f"name='{GDRIVE_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder'"
            " and trashed=false"
        )
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        if files:
            return files[0]["id"]
        # Create it
        meta = {
            "name": GDRIVE_FOLDER_NAME,
            "mimeType": "application/vnd.google-apps.folder",
        }
        folder = service.files().create(body=meta, fields="id").execute()
        return folder["id"]

    # ── Upload / Download ────────────────────────────────────────────────────

    def upload(self, token_dict: dict, payload: bytes, filename: str) -> dict:
        """Upload encrypted *payload* as *filename* to the MiniMe folder."""
        from googleapiclient.http import MediaIoBaseUpload   # type: ignore

        service = self._build_service(token_dict)
        folder_id = self._get_or_create_folder(service)

        media = MediaIoBaseUpload(io.BytesIO(payload), mimetype="application/octet-stream")
        meta = {"name": filename, "parents": [folder_id]}

        # Check if file already exists (update vs create)
        q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        existing = service.files().list(q=q, fields="files(id)").execute().get("files", [])

        if existing:
            file_id = existing[0]["id"]
            result = service.files().update(fileId=file_id, media_body=media).execute()
        else:
            result = service.files().create(body=meta, media_body=media, fields="id,name,size").execute()

        logger.info("gdrive_upload_complete", filename=filename, file_id=result.get("id"))
        return result

    def download(self, token_dict: dict, filename: str) -> Optional[bytes]:
        """Download *filename* from the MiniMe folder. Returns None if not found."""
        from googleapiclient.http import MediaIoBaseDownload   # type: ignore

        service = self._build_service(token_dict)
        folder_id = self._get_or_create_folder(service)

        q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        files = service.files().list(q=q, fields="files(id,name,size,modifiedTime)").execute().get("files", [])
        if not files:
            return None

        file_id = files[0]["id"]
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        logger.info("gdrive_download_complete", filename=filename)
        return buf.getvalue()

    def list_snapshots(self, token_dict: dict) -> list[dict]:
        """List all encrypted snapshot files in the MiniMe folder."""
        service = self._build_service(token_dict)
        folder_id = self._get_or_create_folder(service)
        q = f"'{folder_id}' in parents and trashed=false"
        files = service.files().list(
            q=q,
            fields="files(id,name,size,modifiedTime)",
            orderBy="modifiedTime desc",
        ).execute().get("files", [])
        return files

    def get_user_info(self, token_dict: dict) -> dict:
        """Return Google account email/name."""
        from googleapiclient.discovery import build   # type: ignore
        import google.oauth2.credentials as gc   # type: ignore
        creds = gc.Credentials(
            token=token_dict["access_token"],
            refresh_token=token_dict.get("refresh_token"),
            token_uri=token_dict.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=token_dict.get("client_id", self.client_id),
            client_secret=token_dict.get("client_secret", self.client_secret),
        )
        svc = build("oauth2", "v2", credentials=creds, cache_discovery=False)
        info = svc.userinfo().get().execute()
        return {"email": info.get("email", ""), "name": info.get("name", "")}


# ═══════════════════════════════════════════════════════════════════════════════
# OneDrive Sync (Microsoft Graph)
# ═══════════════════════════════════════════════════════════════════════════════

ONEDRIVE_FOLDER = "MiniMe Backups"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
ONEDRIVE_SCOPES = ["Files.ReadWrite", "User.Read"]  # offline_access is reserved — MSAL adds it automatically



class OneDriveSync:
    """Manages encrypted backups in a private OneDrive folder via Microsoft Graph."""

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str,
                 tenant: str = "common"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.tenant = tenant

    # ── OAuth2 / MSAL flow ───────────────────────────────────────────────────

    def get_auth_url(self, state: str = "") -> str:
        """Return Microsoft OAuth2 authorization URL."""
        import msal   # type: ignore
        app = self._make_app()
        result = app.get_authorization_request_url(
            scopes=ONEDRIVE_SCOPES,
            redirect_uri=self.redirect_uri,
            state=state,
        )
        return result

    def exchange_code(self, code: str) -> dict:
        """Exchange authorization code for tokens."""
        import msal   # type: ignore
        app = self._make_app()
        result = app.acquire_token_by_authorization_code(
            code=code,
            scopes=ONEDRIVE_SCOPES,
            redirect_uri=self.redirect_uri,
        )
        if "error" in result:
            raise ValueError(f"MSAL error: {result.get('error_description', result['error'])}")
        return result

    def _make_app(self):
        import msal   # type: ignore
        return msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant}",
            client_credential=self.client_secret,
        )

    def _refresh_token_if_needed(self, token_dict: dict) -> dict:
        """Refresh access token using refresh_token if expired."""
        import msal   # type: ignore
        if "refresh_token" not in token_dict:
            return token_dict
        app = self._make_app()
        result = app.acquire_token_by_refresh_token(
            token_dict["refresh_token"],
            scopes=ONEDRIVE_SCOPES,
        )
        if "access_token" in result:
            token_dict.update(result)
        return token_dict

    def _headers(self, token_dict: dict) -> dict:
        return {"Authorization": f"Bearer {token_dict['access_token']}"}

    # ── Folder helpers ───────────────────────────────────────────────────────

    def _ensure_folder(self, token_dict: dict) -> str:
        """Get or create the MiniMe Backups folder. Returns DriveItem ID."""
        import httpx   # type: ignore
        headers = self._headers(token_dict)

        # Try to get existing folder
        url = f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}"
        r = httpx.get(url, headers=headers)
        if r.status_code == 200:
            return r.json()["id"]

        # Create it
        create_url = f"{GRAPH_BASE}/me/drive/root/children"
        body = {"name": ONEDRIVE_FOLDER, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"}
        r = httpx.post(create_url, headers=headers, json=body)
        r.raise_for_status()
        return r.json()["id"]

    # ── Upload / Download ────────────────────────────────────────────────────

    def upload(self, token_dict: dict, payload: bytes, filename: str) -> dict:
        """Upload encrypted *payload* to OneDrive MiniMe folder."""
        import httpx   # type: ignore
        token_dict = self._refresh_token_if_needed(token_dict)
        headers = self._headers(token_dict)
        url = f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}/{filename}:/content"
        r = httpx.put(url, headers={**headers, "Content-Type": "application/octet-stream"},
                      content=payload, timeout=60.0)
        r.raise_for_status()
        logger.info("onedrive_upload_complete", filename=filename)
        return r.json()

    def download(self, token_dict: dict, filename: str) -> Optional[bytes]:
        """Download *filename* from the MiniMe folder."""
        import httpx   # type: ignore
        token_dict = self._refresh_token_if_needed(token_dict)
        headers = self._headers(token_dict)

        # Get download URL
        meta_url = f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}/{filename}"
        r = httpx.get(meta_url, headers=headers)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        download_url = r.json().get("@microsoft.graph.downloadUrl")
        if not download_url:
            return None
        data_r = httpx.get(download_url, timeout=60.0)
        data_r.raise_for_status()
        logger.info("onedrive_download_complete", filename=filename)
        return data_r.content

    def list_snapshots(self, token_dict: dict) -> list[dict]:
        """List snapshot files in the MiniMe folder."""
        import httpx   # type: ignore
        token_dict = self._refresh_token_if_needed(token_dict)
        headers = self._headers(token_dict)
        url = f"{GRAPH_BASE}/me/drive/root:/{ONEDRIVE_FOLDER}:/children"
        r = httpx.get(url, headers=headers)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        items = r.json().get("value", [])
        return [{"name": i["name"], "size": i.get("size", 0),
                 "modifiedTime": i.get("lastModifiedDateTime", "")} for i in items]

    def get_user_info(self, token_dict: dict) -> dict:
        """Return Microsoft account email/name."""
        import httpx   # type: ignore
        token_dict = self._refresh_token_if_needed(token_dict)
        r = httpx.get(f"{GRAPH_BASE}/me", headers=self._headers(token_dict))
        r.raise_for_status()
        data = r.json()
        return {
            "email": data.get("mail") or data.get("userPrincipalName", ""),
            "name": data.get("displayName", ""),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Factory helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _load_env():
    """Load .env from project root into os.environ (idempotent, safe to call multiple times)."""
    try:
        from dotenv import load_dotenv  # type: ignore
        import pathlib
        # Walk up from this file to find the project root .env
        env_file = pathlib.Path(__file__).parent.parent.parent / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=False)  # override=False: don't clobber already-set vars
    except ImportError:
        pass   # dotenv not installed — rely on env vars set externally


def get_gdrive_sync() -> GoogleDriveSync:
    _load_env()
    # Use GOOGLE_DRIVE_* vars (Drive-specific redirect URI: localhost:8000/...gdrive/callback)
    # Falls back to GOOGLE_CLIENT_* for backward compat.
    client_id = (
        os.getenv("GOOGLE_DRIVE_CLIENT_ID")
        or os.getenv("GOOGLE_CLIENT_ID", "")
    )
    client_secret = (
        os.getenv("GOOGLE_DRIVE_CLIENT_SECRET")
        or os.getenv("GOOGLE_CLIENT_SECRET", "")
    )
    redirect_uri = (
        os.getenv("GOOGLE_DRIVE_REDIRECT_URI")
        or "http://localhost:8000/api/v1/sync/gdrive/callback"
    )
    return GoogleDriveSync(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )


def get_onedrive_sync() -> OneDriveSync:
    _load_env()
    from config import settings
    return OneDriveSync(
        client_id=getattr(settings, "MICROSOFT_CLIENT_ID", os.getenv("MICROSOFT_CLIENT_ID", "")),
        client_secret=getattr(settings, "MICROSOFT_CLIENT_SECRET", os.getenv("MICROSOFT_CLIENT_SECRET", "")),
        redirect_uri=getattr(settings, "MICROSOFT_REDIRECT_URI",
                             os.getenv("MICROSOFT_REDIRECT_URI", "http://localhost:8000/api/v1/sync/onedrive/callback")),
    )

