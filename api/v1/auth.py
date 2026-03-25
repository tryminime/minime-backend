"""
Authentication API endpoints.
Handles user registration, login, logout, token refresh.
"""

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from datetime import datetime, timedelta, timezone
import uuid as uuid_lib
import structlog
from collections import defaultdict
import time

from database.postgres import get_db
from auth.jwt_handler import (
    create_access_token,
    create_refresh_token,
    create_long_lived_refresh_token,
    decode_token,
    verify_token_type,
    get_user_id_from_token
)
from auth.password import hash_password, verify_password, validate_password_strength

logger = structlog.get_logger()
router = APIRouter()
security = HTTPBearer()


# =====================================================
# RATE LIMITING (in-memory, per-IP)
# =====================================================

_rate_limit_store: dict[str, list[float]] = defaultdict(list)
_CLEANUP_INTERVAL = 300  # seconds between cleanups
_last_cleanup = time.monotonic()


def _cleanup_rate_store():
    """Remove expired entries to prevent memory leak."""
    global _last_cleanup
    now = time.monotonic()
    if now - _last_cleanup < _CLEANUP_INTERVAL:
        return
    _last_cleanup = now
    cutoff = now - 120  # keep last 2 minutes
    expired_keys = [k for k, v in _rate_limit_store.items() if not v or v[-1] < cutoff]
    for k in expired_keys:
        del _rate_limit_store[k]


def _check_rate_limit(request: Request, action: str, max_attempts: int, window_seconds: int = 60):
    """
    Check rate limit for a given action + client IP.
    Raises 429 if limit exceeded.
    """
    _cleanup_rate_store()
    client_ip = request.client.host if request.client else "unknown"
    key = f"{action}:{client_ip}"
    now = time.monotonic()
    cutoff = now - window_seconds

    # Remove old timestamps
    timestamps = _rate_limit_store[key]
    _rate_limit_store[key] = [t for t in timestamps if t > cutoff]

    if len(_rate_limit_store[key]) >= max_attempts:
        logger.warning("Rate limit exceeded", action=action, ip=client_ip)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many {action} attempts. Please try again later.",
        )

    _rate_limit_store[key].append(now)


# =====================================================
# REQUEST/RESPONSE MODELS
# =====================================================

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    full_name: Optional[str] = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    remember_device: bool = False
    device_name: Optional[str] = None         # e.g. "Work Laptop", "Home PC"
    device_fingerprint: Optional[str] = None  # hash of machine-id / browser fingerprint


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    remember_device: bool = False    # echo back so frontend knows which storage to use
    expires_in_days: int = 7         # 7 or 90 depending on remember_device
    has_cloud_backup: bool = False   # True if user has synced data in the cloud
    last_synced_at: Optional[str] = None  # ISO timestamp of most recent completed sync


class RefreshRequest(BaseModel):
    refresh_token: str


class UserResponse(BaseModel):
    id: str
    email: str
    username: Optional[str] = None
    full_name: Optional[str] = None
    avatar_url: Optional[str] = None
    role: str = "user"
    tier: str = "free"
    subscription_status: str = "free"
    created_at: str
    email_verified: bool


# =====================================================
# ENDPOINTS
# =====================================================

@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(request: RegisterRequest, raw_request: Request, db: AsyncSession = Depends(get_db)):
    _check_rate_limit(raw_request, "register", max_attempts=5)
    """
    Register a new user account.
    
    - **email**: User's email address
    - **password**: Strong password (min 8 chars, uppercase, lowercase, digit, special char)
    - **full_name**: Optional full name
    
    Returns access and refresh tokens on success.
    """
    from sqlalchemy import select
    from models import User
    from datetime import datetime
    
    # Validate password strength
    is_valid, error_message = validate_password_strength(request.password)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_message
        )
    
    # Check if user already exists
    result = await db.execute(select(User).where(User.email == request.email))
    existing_user = result.scalar_one_or_none()
    
    if existing_user:
        # Allow re-registration if account was deleted
        if hasattr(existing_user, 'deleted_at') and existing_user.deleted_at is not None:
            await db.delete(existing_user)
            await db.flush()
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
    
    # Hash password
    password_hash = hash_password(request.password)
    
    # Create user in database
    new_user = User(
        email=request.email,
        password_hash=password_hash,
        full_name=request.full_name,
        tier="free",
        subscription_status="active",
        email_verified=False,
        preferences={},
        privacy_settings={"track_desktop": True, "track_web": True, "track_mobile": True}
    )
    
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)
    
    user_id = str(new_user.id)
    
    # Generate tokens
    access_token = create_access_token({"sub": user_id, "email": request.email})
    refresh_token = create_refresh_token({"sub": user_id})
    
    # Persist refresh token in sessions table
    from models import Session as SessionModel
    from config import settings
    new_session = SessionModel(
        user_id=new_user.id,
        refresh_token=refresh_token,
        revoked=False,
        expires_at=datetime.now(timezone.utc) + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS),
    )
    db.add(new_session)
    await db.commit()
    
    logger.info("User registered", user_id=user_id, email=request.email)
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token
    )


@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest, raw_request: Request, db: AsyncSession = Depends(get_db)):
    _check_rate_limit(raw_request, "login", max_attempts=5)
    """
    Login with email and password.
    
    - **email**: User's email address
    - **password**: User's password
    
    Returns access and refresh tokens on success.
    """
    from sqlalchemy import select
    from models import User
    
    # Fetch user from database
    result = await db.execute(select(User).where(User.email == request.email))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )
    
    # Verify password
    if not verify_password(request.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )
    
    # Block deleted accounts
    if hasattr(user, 'deleted_at') and user.deleted_at is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This account has been deleted. Please register again to enjoy our services."
        )
    
    user_id = str(user.id)
    
    # Generate tokens — use long-lived refresh token if remember_device is set
    access_token = create_access_token({
        "sub": user_id,
        "email": request.email,
        "is_superadmin": bool(getattr(user, 'is_superadmin', False)),
    })
    
    from config import settings
    if request.remember_device:
        refresh_token = create_long_lived_refresh_token({"sub": user_id})
        token_expiry_days = settings.JWT_REMEMBER_DEVICE_EXPIRE_DAYS
    else:
        refresh_token = create_refresh_token({"sub": user_id})
        token_expiry_days = settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS
    
    # Persist refresh token in sessions table with device metadata
    from models import Session as SessionModel
    new_session = SessionModel(
        user_id=user.id,
        refresh_token=refresh_token,
        revoked=False,
        remember_device=request.remember_device,
        device_name=request.device_name,
        device_info={
            "fingerprint": request.device_fingerprint,
            "user_agent": None,  # set from HTTP header below if available
        },
        expires_at=datetime.now(timezone.utc) + timedelta(days=token_expiry_days),
    )
    db.add(new_session)
    await db.commit()
    
    logger.info("User logged in", user_id=user_id, email=request.email,
                remember_device=request.remember_device, device_name=request.device_name)
    
    # Write token to shared file so the desktop tracker picks it up automatically.
    _save_local_token(access_token, refresh_token)
    
    # Check if user has cloud backup data (for cross-device restore)
    has_cloud_backup = False
    last_synced_at_str = None
    if (user.tier or "free") in ("pro", "premium", "enterprise"):
        try:
            from sqlalchemy import text as sa_text
            sync_check = await db.execute(
                sa_text("SELECT MAX(completed_at) FROM sync_history WHERE user_id = :uid AND status = 'completed'"),
                {"uid": user.id},
            )
            last_sync = sync_check.scalar()
            if last_sync:
                has_cloud_backup = True
                last_synced_at_str = last_sync.isoformat()
        except Exception:
            pass  # Table might not exist yet

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        remember_device=request.remember_device,
        expires_in_days=token_expiry_days,
        has_cloud_backup=has_cloud_backup,
        last_synced_at=last_synced_at_str,
    )


def _save_local_token(access_token: str, refresh_token: str) -> None:
    """Write tokens to ~/.config/minime/token for the desktop tracker to use."""
    import os
    import json
    try:
        config_dir = os.path.expanduser("~/.config/minime")
        os.makedirs(config_dir, exist_ok=True)
        token_path = os.path.join(config_dir, "token")
        
        data = {
            "access_token": access_token,
            "refresh_token": refresh_token
        }
        
        with open(token_path, "w") as f:
            json.dump(data, f)
        os.chmod(token_path, 0o600)  # owner read/write only
    except Exception as e:
        logger.warning("Could not save local token file", error=str(e))


@router.post("/refresh", response_model=TokenResponse)
async def refresh_access_token(request: RefreshRequest, raw_request: Request, db: AsyncSession = Depends(get_db)):
    _check_rate_limit(raw_request, "refresh", max_attempts=10)
    """
    Refresh an access token using a valid refresh token.
    
    - **refresh_token**: Valid refresh token
    
    Returns new access and refresh tokens (token rotation).
    """
    from sqlalchemy import select
    from models import Session as SessionModel
    from config import settings

    # Decode and validate refresh token
    payload = decode_token(request.refresh_token)
    
    if not payload or not verify_token_type(payload, "refresh"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )
    
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload"
        )
    
    # Verify refresh token exists in sessions table and is not revoked
    result = await db.execute(
        select(SessionModel).where(
            SessionModel.refresh_token == request.refresh_token,
            SessionModel.user_id == uuid_lib.UUID(user_id),
        )
    )
    session = result.scalar_one_or_none()

    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token not found"
        )

    if session.revoked:
        # Potential token replay attack — revoke ALL sessions for this user
        from sqlalchemy import update
        await db.execute(
            update(SessionModel)
            .where(SessionModel.user_id == uuid_lib.UUID(user_id))
            .values(revoked=True)
        )
        await db.commit()
        logger.warning("Refresh token replay detected, all sessions revoked", user_id=user_id)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has been revoked"
        )

    if session.expires_at and session.expires_at < datetime.now(timezone.utc):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token has expired"
        )

    # Revoke old refresh token (token rotation)
    old_remember_device = session.remember_device  # preserve long-lived status on rotation
    session.revoked = True
    
    # Generate new tokens — preserve long-lived if this was a remember-device session
    from config import settings
    access_token = create_access_token({"sub": user_id})
    if old_remember_device:
        new_refresh_token = create_long_lived_refresh_token({"sub": user_id})
        token_expiry_days = settings.JWT_REMEMBER_DEVICE_EXPIRE_DAYS
    else:
        new_refresh_token = create_refresh_token({"sub": user_id})
        token_expiry_days = settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS

    # Persist new refresh token (carry over device metadata)
    new_session = SessionModel(
        user_id=uuid_lib.UUID(user_id),
        refresh_token=new_refresh_token,
        revoked=False,
        remember_device=old_remember_device,
        device_name=session.device_name,
        device_info=session.device_info,
        expires_at=datetime.now(timezone.utc) + timedelta(days=token_expiry_days),
    )
    db.add(new_session)
    await db.commit()
    
    logger.info("Token refreshed", user_id=user_id, remember_device=old_remember_device)
    
    # Also update the local token file for the desktop tracker
    _save_local_token(access_token, new_refresh_token)
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=new_refresh_token,
        remember_device=old_remember_device,
        expires_in_days=token_expiry_days,
    )



class OAuthExchangeRequest(BaseModel):
    provider: str
    email: EmailStr
    full_name: Optional[str] = None


@router.post("/oauth-exchange", response_model=TokenResponse)
async def oauth_exchange(request: OAuthExchangeRequest, db: AsyncSession = Depends(get_db)):
    """
    Exchange OAuth provider info for MiniMe JWT tokens.

    Called by the frontend after a successful OAuth callback.
    Finds the user by email or creates a new account (passwordless OAuth user).
    Returns access and refresh tokens.
    """
    from sqlalchemy import select
    from models import User, Session as SessionModel
    from config import settings

    # Find existing user by email
    result = await db.execute(select(User).where(User.email == request.email))
    user = result.scalar_one_or_none()

    if user:
        # Block deleted accounts
        if hasattr(user, 'deleted_at') and user.deleted_at is not None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This account has been deleted"
            )
    else:
        # Auto-create account for new OAuth users (no password needed)
        user = User(
            email=request.email,
            password_hash="",  # OAuth-only user, no password
            full_name=request.full_name,
            tier="free",
            subscription_status="active",
            email_verified=True,  # OAuth emails are pre-verified
            preferences={"auth_provider": request.provider},
            privacy_settings={"track_desktop": True, "track_web": True, "track_mobile": True},
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        logger.info("OAuth user created", user_id=str(user.id), provider=request.provider)

    user_id = str(user.id)

    # Generate tokens
    access_token = create_access_token({"sub": user_id, "email": request.email})
    refresh_token = create_refresh_token({"sub": user_id})

    # Persist refresh token in sessions table
    new_session = SessionModel(
        user_id=user.id,
        refresh_token=refresh_token,
        revoked=False,
        expires_at=datetime.now(timezone.utc) + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS),
    )
    db.add(new_session)
    await db.commit()

    logger.info("OAuth exchange completed", user_id=user_id, provider=request.provider)

    # Check if user has cloud backup data (for cross-device restore)
    has_cloud_backup = False
    last_synced_at_str = None
    if (user.tier or "free") in ("pro", "premium", "enterprise"):
        try:
            from sqlalchemy import text as sa_text
            sync_check = await db.execute(
                sa_text("SELECT MAX(completed_at) FROM sync_history WHERE user_id = :uid AND status = 'completed'"),
                {"uid": user.id},
            )
            last_sync = sync_check.scalar()
            if last_sync:
                has_cloud_backup = True
                last_synced_at_str = last_sync.isoformat()
        except Exception:
            pass

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        has_cloud_backup=has_cloud_backup,
        last_synced_at=last_synced_at_str,
    )


@router.post("/logout")
async def logout(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Logout and revoke all refresh tokens for this user.
    
    Requires: Authorization header with Bearer token
    """
    token = credentials.credentials
    user_id = get_user_id_from_token(token)
    
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    
    # Revoke all active refresh tokens for this user
    from sqlalchemy import update
    from models import Session as SessionModel
    await db.execute(
        update(SessionModel)
        .where(
            SessionModel.user_id == uuid_lib.UUID(user_id),
            SessionModel.revoked == False,
        )
        .values(revoked=True)
    )
    await db.commit()
    
    logger.info("User logged out, all sessions revoked", user_id=user_id)
    
    return {"message": "Successfully logged out"}


@router.get("/me", response_model=UserResponse)
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current authenticated user profile.
    
    Requires: Authorization header with Bearer token
    """
    token = credentials.credentials
    payload = decode_token(token)
    
    if not payload or not verify_token_type(payload, "access"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token"
        )
    
    user_id = payload.get("sub")
    
    from sqlalchemy import select
    from models import User
    import uuid as uuid_lib
    
    # Fetch user from database
    result = await db.execute(select(User).where(User.id == uuid_lib.UUID(user_id)))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Block deleted accounts
    if hasattr(user, 'deleted_at') and user.deleted_at is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This account has been deleted"
        )
    
    return UserResponse(
        id=str(user.id),
        email=user.email,
        username=user.full_name or user.email.split('@')[0],
        full_name=user.full_name,
        avatar_url=user.avatar_url,
        role="user",
        tier=user.tier or "free",
        subscription_status=user.subscription_status or "free",
        created_at=user.created_at.isoformat() if user.created_at else None,
        email_verified=user.email_verified
    )


# =====================================================
# DEVICE MANAGEMENT ENDPOINTS
# =====================================================

@router.get("/devices")
async def list_trusted_devices(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    List all active (non-revoked) sessions/devices for the current user.
    Useful for the Settings > Security page to show and revoke trusted devices.
    """
    from sqlalchemy import select
    from models import Session as SessionModel

    token = credentials.credentials
    user_id = get_user_id_from_token(token)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    result = await db.execute(
        select(SessionModel).where(
            SessionModel.user_id == uuid_lib.UUID(user_id),
            SessionModel.revoked == False,
            SessionModel.expires_at > datetime.now(timezone.utc),
        )
    )
    sessions = result.scalars().all()

    devices = []
    for s in sessions:
        devices.append({
            "session_id": str(s.id),
            "device_name": s.device_name or "Unknown device",
            "remember_device": s.remember_device,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "expires_at": s.expires_at.isoformat() if s.expires_at else None,
            "device_info": s.device_info or {},
        })

    return {"devices": devices, "count": len(devices)}


@router.delete("/devices/{session_id}")
async def revoke_device(
    session_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Revoke a specific device/session by session ID.
    Only the owning user can revoke their own sessions.
    """
    from sqlalchemy import select
    from models import Session as SessionModel

    token = credentials.credentials
    user_id = get_user_id_from_token(token)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    try:
        session_uuid = uuid_lib.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid session ID")

    result = await db.execute(
        select(SessionModel).where(
            SessionModel.id == session_uuid,
            SessionModel.user_id == uuid_lib.UUID(user_id),
        )
    )
    session = result.scalar_one_or_none()

    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")

    session.revoked = True
    await db.commit()

    logger.info("Device session revoked", user_id=user_id, session_id=session_id)
    return {"message": "Device session revoked", "session_id": session_id}


@router.post("/verify-local")
async def verify_local_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """
    Offline-capable token validation — checks JWT signature and expiry only.
    No database call. Used by desktop app on startup to confirm auth without internet.
    
    Returns 200 with user info if valid, 401 if expired/invalid.
    """
    token = credentials.credentials
    payload = decode_token(token)

    if not payload or not verify_token_type(payload, "access"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    return {
        "valid": True,
        "user_id": payload.get("sub"),
        "email": payload.get("email"),
        "is_superadmin": payload.get("is_superadmin", False),
        "expires_at": payload.get("exp"),
    }
