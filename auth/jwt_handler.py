"""
JWT token handling for authentication.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
import uuid as uuid_lib
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import structlog

from config import settings

logger = structlog.get_logger()

# HTTP Bearer security scheme
security = HTTPBearer(auto_error=False)


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a JWT access token.
    
    Args:
        data: Payload data to encode in token
        expires_delta: Custom expiration time (defaults to settings)
        
    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "jti": str(uuid_lib.uuid4()),
        "type": "access"
    })
    
    encoded_jwt = jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM
    )
    
    return encoded_jwt


def create_refresh_token(data: Dict[str, Any]) -> str:
    """
    Create a JWT refresh token (longer expiration — 7 days default).
    
    Args:
        data: Payload data to encode in token
        
    Returns:
        Encoded JWT refresh token
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS)
    
    to_encode.update({
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "jti": str(uuid_lib.uuid4()),
        "type": "refresh",
        "long_lived": False,
    })
    
    encoded_jwt = jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM
    )
    
    return encoded_jwt


def create_long_lived_refresh_token(data: Dict[str, Any]) -> str:
    """
    Create a long-lived JWT refresh token for "remember this device" (90 days).
    
    Args:
        data: Payload data to encode in token
        
    Returns:
        Encoded long-lived JWT refresh token
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=settings.JWT_REMEMBER_DEVICE_EXPIRE_DAYS)
    
    to_encode.update({
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "jti": str(uuid_lib.uuid4()),
        "type": "refresh",
        "long_lived": True,  # flag so refresh endpoint can preserve long lifetime
    })
    
    encoded_jwt = jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM
    )
    
    return encoded_jwt


def decode_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Decode and validate a JWT token.
    
    Args:
        token: JWT token to decode
        
    Returns:
        Decoded payload or None if invalid
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )
        return payload
    except JWTError as e:
        logger.warning("JWT decode error", error=str(e))
        return None


def verify_token_type(payload: Dict[str, Any], expected_type: str) -> bool:
    """
    Verify that a token payload has the expected type.
    
    Args:
        payload: Decoded token payload
        expected_type: Expected token type ("access" or "refresh")
        
    Returns:
        True if type matches, False otherwise
    """
    return payload.get("type") == expected_type


def get_user_id_from_token(token: str) -> Optional[str]:
    """
    Extract user ID from a token.
    
    Args:
        token: JWT token
        
    Returns:
        User ID or None if invalid
    """
    payload = decode_token(token)
    if payload is None:
        return None
    
    return payload.get("sub")


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """
    Get current authenticated user from JWT token.
    
    Args:
        credentials: HTTP Bearer credentials
        
    Returns:
        User data dictionary
        
    Raises:
        HTTPException: If authentication fails
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    token = credentials.credentials
    payload = decode_token(token)
    
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not verify_token_type(payload, "access"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_id = payload.get("sub")
    return {
        "id": user_id,
        "user_id": user_id,  # backwards compat
        "sub": user_id,      # raw JWT claim
        "email": payload.get("email"),
        "username": payload.get("username"),
        "is_superadmin": payload.get("is_superadmin", False),
    }


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Optional[Dict[str, Any]]:
    """
    Optional authentication - returns None if no token provided.
    
    Args:
        credentials: HTTP Bearer credentials (optional)
        
    Returns:
        User data dictionary or None
    """
    if not credentials:
        return None
    
    try:
        return await get_current_user(credentials)
    except HTTPException:
        return None


async def get_dev_user() -> Dict[str, Any]:
    """
    Get development user (bypasses auth in dev mode).
    
    Returns:
        Development user data
        
    Raises:
        HTTPException: If not in development mode
    """
    if settings.ENVIRONMENT == "development":
        logger.info("Using development authentication bypass")
        return {
            "id": "dev-user-123",
            "user_id": "dev-user-123",
            "sub": "dev-user-123",
            "email": "dev@example.com",
            "username": "DevUser"
        }
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated (dev mode only in development)",
        headers={"WWW-Authenticate": "Bearer"},
    )
