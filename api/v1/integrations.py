"""
OAuth Integration API Router
Handles OAuth flows for GitHub, Google Calendar, and Notion integrations
WITH DATABASE PERSISTENCE
"""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError
import httpx
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import uuid
import structlog

from config import settings
from database.postgres import get_db
from models.integration_models import Integration

logger = structlog.get_logger()

router = APIRouter(prefix="/api/integrations", tags=["integrations"])

# =====================================================
# DATABASE HELPERS
# =====================================================

async def ensure_table_exists(db: AsyncSession):
    """
    Ensure integrations table exists.
    Creates it automatically if missing.
    """
    from sqlalchemy import text
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS integrations (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL,
        provider VARCHAR(50) NOT NULL,
        access_token TEXT NOT NULL,
        refresh_token TEXT,
        token_expires_at TIMESTAMP,
        username VARCHAR(255),
        email VARCHAR(255),
        external_id VARCHAR(255),
        provider_metadata JSONB DEFAULT '{}',
        connected BOOLEAN DEFAULT TRUE,
        last_synced_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        CONSTRAINT unique_user_provider UNIQUE(user_id, provider)
    );
    
    CREATE INDEX IF NOT EXISTS idx_integrations_user_provider ON integrations(user_id, provider);
    CREATE INDEX IF NOT EXISTS idx_integrations_user_id ON integrations(user_id);
    CREATE INDEX IF NOT EXISTS idx_integrations_provider ON integrations(provider);
    CREATE INDEX IF NOT EXISTS idx_integrations_connected ON integrations(connected);
    """
    
    try:
        await db.execute(text(create_table_sql))
        await db.commit()
        logger.info("Integrations table ensured")
    except Exception as e:
        logger.debug("Table creation check", error=str(e))


async def save_integration(
    db: AsyncSession,
    user_id: uuid.UUID,
    provider: str,
    access_token: str,
    refresh_token: Optional[str],
    username: Optional[str],
    email: Optional[str],
    external_id: Optional[str] = None,
    token_expires_at: Optional[datetime] = None,
    provider_metadata: Dict[str, Any] = None
) -> Integration:
    """Save or update integration in database."""
    
    # Ensure table exists
    await ensure_table_exists(db)
    
    # Check if integration already exists
    result = await db.execute(
        select(Integration).where(
            Integration.user_id == user_id,
            Integration.provider == provider
        )
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        # Update existing integration
        existing.access_token = access_token
        existing.refresh_token = refresh_token
        existing.username = username
        existing.email = email
        existing.external_id = external_id
        existing.token_expires_at = token_expires_at
        existing.provider_metadata = provider_metadata or {}
        existing.connected = True
        existing.updated_at = datetime.utcnow()
        
        await db.commit()
        await db.refresh(existing)
        logger.info("Updated integration", provider=provider, user_id=str(user_id))
        return existing
    else:
        # Create new integration
        integration = Integration(
            user_id=user_id,
            provider=provider,
            access_token=access_token,
            refresh_token=refresh_token,
            username=username,
            email=email,
            external_id=external_id,
            token_expires_at=token_expires_at,
            provider_metadata=provider_metadata or {},
            connected=True
        )
        
        db.add(integration)
        await db.commit()
        await db.refresh(integration)
        logger.info("Created integration", provider=provider, user_id=str(user_id))
        return integration


async def get_integration_status(
    db: AsyncSession,
    user_id: uuid.UUID,
    provider: str
) -> Optional[Integration]:
    """Get integration status from database."""
    result = await db.execute(
        select(Integration).where(
            Integration.user_id == user_id,
            Integration.provider == provider
        )
    )
    return result.scalar_one_or_none()


async def remove_integration(
    db: AsyncSession,
    user_id: uuid.UUID,
    provider: str
):
    """Remove integration from database."""
    await db.execute(
        delete(Integration).where(
            Integration.user_id == user_id,
            Integration.provider == provider
        )
    )
    await db.commit()
    logger.info("Removed integration", provider=provider, user_id=str(user_id))


# =====================================================
# REQUEST/RESPONSE MODELS
# =====================================================

class OAuthInitiateResponse(BaseModel):
    auth_url: str

class OAuthCallbackRequest(BaseModel):
    code: str
    user_id: Optional[str] = None  # For now, optional. Should be required with auth

class IntegrationAuthResponse(BaseModel):
    connected: bool
    provider: str
    username: Optional[str] = None
    email: Optional[str] = None
    access_token: Optional[str] = None
    expires_at: Optional[str] = None

class IntegrationStatusResponse(BaseModel):
    connected: bool
    username: Optional[str] = None
    email: Optional[str] = None
    last_synced: Optional[str] = None
    error: Optional[str] = None

# =====================================================
# GITHUB OAUTH ENDPOINTS
# =====================================================

@router.post("/github/oauth/initiate", response_model=OAuthInitiateResponse)
async def initiate_github_oauth():
    """
    Initiate GitHub OAuth flow.
    Returns authorization URL for user to visit.
    """
    try:
        state = secrets.token_urlsafe(32)
        
        # Build GitHub authorization URL
        auth_url = (
            f"https://github.com/login/oauth/authorize"
            f"?client_id={settings.GITHUB_CLIENT_ID}"
            f"&redirect_uri={settings.GITHUB_REDIRECT_URI}"
            f"&state={state}"
            f"&scope=user:email read:user"
        )
        
        logger.info("GitHub OAuth initiated", state=state)
        
        return OAuthInitiateResponse(auth_url=auth_url)
    
    except Exception as e:
        logger.error("Failed to initiate GitHub OAuth", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate GitHub OAuth: {str(e)}"
        )


@router.post("/github/oauth/callback", response_model=IntegrationAuthResponse)
async def github_oauth_callback(
    request: OAuthCallbackRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Handle GitHub OAuth callback.
    Exchanges authorization code for access token and saves to database.
    """
    try:
        # Exchange code for access token
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                "https://github.com/login/oauth/access_token",
                headers={"Accept": "application/json"},
                data={
                    "client_id": settings.GITHUB_CLIENT_ID,
                    "client_secret": settings.GITHUB_CLIENT_SECRET,
                    "code": request.code,
                    "redirect_uri": settings.GITHUB_REDIRECT_URI,
                }
            )
            
            if token_response.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Failed to exchange code for token"
                )
            
            token_data = token_response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No access token received"
                )
            
            # Fetch user info from GitHub
            user_response = await client.get(
                "https://api.github.com/user",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }
            )
            
            if user_response.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Failed to fetch user info"
                )
            
            user_data = user_response.json()
            
            # Use provided user_id or create test UUID
            user_id = uuid.UUID(request.user_id) if request.user_id else uuid.uuid4()
            
            # Save to database
            integration = await save_integration(
                db=db,
                user_id=user_id,
                provider="github",
                access_token=access_token,
                refresh_token=None,
                username=user_data.get("login"),
                email=user_data.get("email"),
                external_id=str(user_data.get("id")),
                provider_metadata={"scopes": token_data.get("scope", "").split(",")}
            )
            
            logger.info("GitHub OAuth completed", username=user_data.get("login"))
            
            return IntegrationAuthResponse(
                connected=True,
                provider="github",
                username=integration.username,
                email=integration.email,
                access_token=access_token
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("GitHub OAuth callback failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"OAuth callback failed: {str(e)}"
        )


@router.get("/github/status", response_model=IntegrationStatusResponse)
async def get_github_status(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Get GitHub integration status for current user.
    """
    try:
        # Use provided user_id or test UUID
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        
        integration = await get_integration_status(db, uid, "github")
        
        if integration and integration.connected:
            return IntegrationStatusResponse(
                connected=True,
                username=integration.username,
                email=integration.email,
                last_synced=integration.last_synced_at.isoformat() if integration.last_synced_at else None
            )
        else:
            return IntegrationStatusResponse(connected=False)
    
    except Exception as e:
        logger.error("Failed to get GitHub status", error=str(e))
        return IntegrationStatusResponse(connected=False, error=str(e))


@router.delete("/github/disconnect")
async def disconnect_github(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Disconnect GitHub integration.
    """
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        await remove_integration(db, uid, "github")
        logger.info("GitHub disconnected")
        return {"success": True, "message": "GitHub disconnected"}
    except Exception as e:
        logger.error("Failed to disconnect GitHub", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# =====================================================
# GOOGLE CALENDAR OAUTH ENDPOINTS
# =====================================================

@router.post("/google/oauth/initiate", response_model=OAuthInitiateResponse)
async def initiate_google_oauth():
    """
    Initiate Google Calendar OAuth flow.
    Returns authorization URL for user to visit.
    """
    try:
        state = secrets.token_urlsafe(32)
        
        # Build Google authorization URL
        auth_url = (
            f"https://accounts.google.com/o/oauth2/v2/auth"
            f"?client_id={settings.GOOGLE_CLIENT_ID}"
            f"&redirect_uri={settings.GOOGLE_REDIRECT_URI}"
            f"&response_type=code"
            f"&scope=https://www.googleapis.com/auth/calendar.readonly https://www.googleapis.com/auth/userinfo.email"
            f"&state={state}"
            f"&access_type=offline"
            f"&prompt=consent"
        )
        
        logger.info("Google OAuth initiated", state=state)
        
        return OAuthInitiateResponse(auth_url=auth_url)
    
    except Exception as e:
        logger.error("Failed to initiate Google OAuth", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate Google OAuth: {str(e)}"
        )


@router.post("/google/oauth/callback", response_model=IntegrationAuthResponse)
async def google_oauth_callback(
    request: OAuthCallbackRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Handle Google OAuth callback.
    Exchanges authorization code for access token and saves to database.
    """
    try:
        # Exchange code for access token
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": settings.GOOGLE_CLIENT_ID,
                    "client_secret": settings.GOOGLE_CLIENT_SECRET,
                    "code": request.code,
                    "redirect_uri": settings.GOOGLE_REDIRECT_URI,
                    "grant_type": "authorization_code"
                }
            )
            
            if token_response.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to exchange code for token: {token_response.text}"
                )
            
            token_data = token_response.json()
            access_token = token_data.get("access_token")
            refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in")
            
            if not access_token:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No access token received"
                )
            
            # Calculate token expiry
            token_expires_at = None
            if expires_in:
                token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            # Fetch user info from Google
            user_response = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            if user_response.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Failed to fetch user info"
                )
            
            user_data = user_response.json()
            
            # Use provided user_id or create test UUID
            user_id = uuid.UUID(request.user_id) if request.user_id else uuid.uuid4()
            
            # Save to database
            integration = await save_integration(
                db=db,
                user_id=user_id,
                provider="google",
                access_token=access_token,
                refresh_token=refresh_token,
                username=user_data.get("name"),
                email=user_data.get("email"),
                external_id=user_data.get("id"),
                token_expires_at=token_expires_at,
                provider_metadata={"scopes": token_data.get("scope", "").split()}
            )
            
            logger.info("Google OAuth completed", email=user_data.get("email"))
            
            return IntegrationAuthResponse(
                connected=True,
                provider="google",
                username=integration.username,
                email=integration.email,
                access_token=access_token,
                expires_at=token_expires_at.isoformat() if token_expires_at else None
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Google OAuth callback failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"OAuth callback failed: {str(e)}"
        )


@router.get("/google/status", response_model=IntegrationStatusResponse)
async def get_google_status(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Get Google Calendar integration status for current user.
    """
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        
        integration = await get_integration_status(db, uid, "google")
        
        if integration and integration.connected:
            return IntegrationStatusResponse(
                connected=True,
                username=integration.username,
                email=integration.email,
                last_synced=integration.last_synced_at.isoformat() if integration.last_synced_at else None
            )
        else:
            return IntegrationStatusResponse(connected=False)
    
    except Exception as e:
        logger.error("Failed to get Google status", error=str(e))
        return IntegrationStatusResponse(connected=False, error=str(e))


@router.delete("/google/disconnect")
async def disconnect_google(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Disconnect Google Calendar integration.
    """
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        await remove_integration(db, uid, "google")
        logger.info("Google disconnected")
        return {"success": True, "message": "Google Calendar disconnected"}
    except Exception as e:
        logger.error("Failed to disconnect Google", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# =====================================================
# NOTION OAUTH ENDPOINTS
# =====================================================

@router.post("/notion/oauth/initiate", response_model=OAuthInitiateResponse)
async def initiate_notion_oauth():
    """
    Initiate Notion OAuth flow.
    Returns authorization URL for user to visit.
    """
    try:
        state = secrets.token_urlsafe(32)
        
        # Build Notion authorization URL
        auth_url = (
            f"https://api.notion.com/v1/oauth/authorize"
            f"?client_id={settings.NOTION_CLIENT_ID}"
            f"&redirect_uri={settings.NOTION_REDIRECT_URI}"
            f"&response_type=code"
            f"&owner=user"
            f"&state={state}"
        )
        
        logger.info("Notion OAuth initiated", state=state)
        
        return OAuthInitiateResponse(auth_url=auth_url)
    
    except Exception as e:
        logger.error("Failed to initiate Notion OAuth", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate Notion OAuth: {str(e)}"
        )


@router.post("/notion/oauth/callback", response_model=IntegrationAuthResponse)
async def notion_oauth_callback(
    request: OAuthCallbackRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Handle Notion OAuth callback.
    Exchanges authorization code for access token and saves to database.
    """
    try:
        # Exchange code for access token
        async with httpx.AsyncClient() as client:
            # Notion requires Basic Auth with client_id:client_secret
            import base64
            auth_string = f"{settings.NOTION_CLIENT_ID}:{settings.NOTION_CLIENT_SECRET}"
            auth_bytes = auth_string.encode('utf-8')
            auth_b64 = base64.b64encode(auth_bytes).decode('utf-8')
            
            token_response = await client.post(
                "https://api.notion.com/v1/oauth/token",
                headers={
                    "Authorization": f"Basic {auth_b64}",
                    "Content-Type": "application/json"
                },
                json={
                    "grant_type": "authorization_code",
                    "code": request.code,
                    "redirect_uri": settings.NOTION_REDIRECT_URI
                }
            )
            
            if token_response.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to exchange code for token: {token_response.text}"
                )
            
            token_data = token_response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No access token received"
                )
            
            # Extract workspace and owner info
            workspace_name = token_data.get("workspace_name", "Unknown")
            workspace_id = token_data.get("workspace_id")
            owner_info = token_data.get("owner", {})
            bot_id = token_data.get("bot_id")
            
            # Use provided user_id or create test UUID
            user_id = uuid.UUID(request.user_id) if request.user_id else uuid.uuid4()
            
            # Save to database
            integration = await save_integration(
                db=db,
                user_id=user_id,
                provider="notion",
                access_token=access_token,
                refresh_token=None,  # Notion doesn't use refresh tokens
                username=workspace_name,
                email=owner_info.get("user", {}).get("person", {}).get("email"),
                external_id=workspace_id,
                provider_metadata={
                    "bot_id": bot_id,
                    "workspace_id": workspace_id,
                    "owner": owner_info
                }
            )
            
            logger.info("Notion OAuth completed", workspace=workspace_name)
            
            return IntegrationAuthResponse(
                connected=True,
                provider="notion",
                username=integration.username,
                email=integration.email,
                access_token=access_token
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Notion OAuth callback failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"OAuth callback failed: {str(e)}"
        )


@router.get("/notion/status", response_model=IntegrationStatusResponse)
async def get_notion_status(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Get Notion integration status for current user.
    """
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        
        integration = await get_integration_status(db, uid, "notion")
        
        if integration and integration.connected:
            return IntegrationStatusResponse(
                connected=True,
                username=integration.username,
                email=integration.email,
                last_synced=integration.last_synced_at.isoformat() if integration.last_synced_at else None
            )
        else:
            return IntegrationStatusResponse(connected=False)
    
    except Exception as e:
        logger.error("Failed to get Notion status", error=str(e))
        return IntegrationStatusResponse(connected=False, error=str(e))


@router.delete("/notion/disconnect")
async def disconnect_notion(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Disconnect Notion integration.
    """
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        await remove_integration(db, uid, "notion")
        logger.info("Notion disconnected")
        return {"success": True, "message": "Notion disconnected"}
    except Exception as e:
        logger.error("Failed to disconnect Notion", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# =====================================================
# SLACK OAUTH ENDPOINTS
# =====================================================

@router.post("/slack/oauth/initiate", response_model=OAuthInitiateResponse)
async def initiate_slack_oauth():
    """
    Initiate Slack OAuth flow.
    Returns authorization URL for user to visit.
    """
    try:
        state = secrets.token_urlsafe(32)

        auth_url = (
            f"https://slack.com/oauth/v2/authorize"
            f"?client_id={getattr(settings, 'SLACK_CLIENT_ID', '')}"
            f"&redirect_uri={getattr(settings, 'SLACK_REDIRECT_URI', '')}"
            f"&scope=channels:read,chat:write,users:read,users:read.email"
            f"&state={state}"
        )

        logger.info("Slack OAuth initiated", state=state)
        return OAuthInitiateResponse(auth_url=auth_url)

    except Exception as e:
        logger.error("Failed to initiate Slack OAuth", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate Slack OAuth: {str(e)}"
        )


@router.post("/slack/oauth/callback", response_model=IntegrationAuthResponse)
async def slack_oauth_callback(
    request: OAuthCallbackRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Handle Slack OAuth callback.
    Exchanges authorization code for access token and saves to database.
    """
    try:
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                "https://slack.com/api/oauth.v2.access",
                data={
                    "client_id": getattr(settings, 'SLACK_CLIENT_ID', ''),
                    "client_secret": getattr(settings, 'SLACK_CLIENT_SECRET', ''),
                    "code": request.code,
                    "redirect_uri": getattr(settings, 'SLACK_REDIRECT_URI', ''),
                }
            )

            token_data = token_response.json()
            if not token_data.get("ok"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Slack OAuth failed: {token_data.get('error', 'unknown')}"
                )

            access_token = token_data.get("access_token")
            team_name = token_data.get("team", {}).get("name", "Unknown")
            team_id = token_data.get("team", {}).get("id")
            user_info = token_data.get("authed_user", {})

            user_id = uuid.UUID(request.user_id) if request.user_id else uuid.uuid4()

            integration = await save_integration(
                db=db,
                user_id=user_id,
                provider="slack",
                access_token=access_token,
                refresh_token=None,
                username=team_name,
                email=None,
                external_id=team_id,
                provider_metadata={
                    "team_id": team_id,
                    "authed_user": user_info,
                    "scope": token_data.get("scope", ""),
                }
            )

            logger.info("Slack OAuth completed", team=team_name)

            return IntegrationAuthResponse(
                connected=True,
                provider="slack",
                username=integration.username,
                email=integration.email,
                access_token=access_token
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Slack OAuth callback failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"OAuth callback failed: {str(e)}"
        )


@router.get("/slack/status", response_model=IntegrationStatusResponse)
async def get_slack_status(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get Slack integration status for current user."""
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        integration = await get_integration_status(db, uid, "slack")

        if integration and integration.connected:
            return IntegrationStatusResponse(
                connected=True,
                username=integration.username,
                email=integration.email,
                last_synced=integration.last_synced_at.isoformat() if integration.last_synced_at else None
            )
        else:
            return IntegrationStatusResponse(connected=False)

    except Exception as e:
        logger.error("Failed to get Slack status", error=str(e))
        return IntegrationStatusResponse(connected=False, error=str(e))


@router.delete("/slack/disconnect")
async def disconnect_slack(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Disconnect Slack integration."""
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        await remove_integration(db, uid, "slack")
        logger.info("Slack disconnected")
        return {"success": True, "message": "Slack disconnected"}
    except Exception as e:
        logger.error("Failed to disconnect Slack", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# =====================================================
# JIRA OAUTH ENDPOINTS
# =====================================================

@router.post("/jira/oauth/initiate", response_model=OAuthInitiateResponse)
async def initiate_jira_oauth():
    """
    Initiate Jira OAuth 2.0 flow.
    Returns authorization URL for user to visit.
    """
    try:
        state = secrets.token_urlsafe(32)

        auth_url = (
            f"https://auth.atlassian.com/authorize"
            f"?audience=api.atlassian.com"
            f"&client_id={getattr(settings, 'JIRA_CLIENT_ID', '')}"
            f"&scope=read%3Ajira-work%20read%3Ajira-user%20write%3Ajira-work"
            f"&redirect_uri={getattr(settings, 'JIRA_REDIRECT_URI', '')}"
            f"&state={state}"
            f"&response_type=code"
            f"&prompt=consent"
        )

        logger.info("Jira OAuth initiated", state=state)
        return OAuthInitiateResponse(auth_url=auth_url)

    except Exception as e:
        logger.error("Failed to initiate Jira OAuth", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate Jira OAuth: {str(e)}"
        )


@router.post("/jira/oauth/callback", response_model=IntegrationAuthResponse)
async def jira_oauth_callback(
    request: OAuthCallbackRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Handle Jira OAuth callback.
    Exchanges authorization code for access token and saves to database.
    """
    try:
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                "https://auth.atlassian.com/oauth/token",
                json={
                    "grant_type": "authorization_code",
                    "client_id": getattr(settings, 'JIRA_CLIENT_ID', ''),
                    "client_secret": getattr(settings, 'JIRA_CLIENT_SECRET', ''),
                    "code": request.code,
                    "redirect_uri": getattr(settings, 'JIRA_REDIRECT_URI', ''),
                }
            )

            if token_response.status_code != 200:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to exchange code for token: {token_response.text}"
                )

            token_data = token_response.json()
            access_token = token_data.get("access_token")
            refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in")

            if not access_token:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No access token received"
                )

            token_expires_at = None
            if expires_in:
                token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

            # Get accessible resources (Jira sites)
            resources_response = await client.get(
                "https://api.atlassian.com/oauth/token/accessible-resources",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            resources = resources_response.json() if resources_response.status_code == 200 else []
            site_name = resources[0].get("name", "Unknown") if resources else "Unknown"
            cloud_id = resources[0].get("id", "") if resources else ""

            # Get user info
            if cloud_id:
                me_response = await client.get(
                    f"https://api.atlassian.com/ex/jira/{cloud_id}/rest/api/3/myself",
                    headers={"Authorization": f"Bearer {access_token}"}
                )
                user_data = me_response.json() if me_response.status_code == 200 else {}
            else:
                user_data = {}

            user_id = uuid.UUID(request.user_id) if request.user_id else uuid.uuid4()

            integration = await save_integration(
                db=db,
                user_id=user_id,
                provider="jira",
                access_token=access_token,
                refresh_token=refresh_token,
                username=user_data.get("displayName", site_name),
                email=user_data.get("emailAddress"),
                external_id=cloud_id,
                token_expires_at=token_expires_at,
                provider_metadata={
                    "cloud_id": cloud_id,
                    "site_name": site_name,
                    "resources": resources[:3],
                }
            )

            logger.info("Jira OAuth completed", site=site_name)

            return IntegrationAuthResponse(
                connected=True,
                provider="jira",
                username=integration.username,
                email=integration.email,
                access_token=access_token,
                expires_at=token_expires_at.isoformat() if token_expires_at else None
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Jira OAuth callback failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"OAuth callback failed: {str(e)}"
        )


@router.get("/jira/status", response_model=IntegrationStatusResponse)
async def get_jira_status(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get Jira integration status for current user."""
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        integration = await get_integration_status(db, uid, "jira")

        if integration and integration.connected:
            return IntegrationStatusResponse(
                connected=True,
                username=integration.username,
                email=integration.email,
                last_synced=integration.last_synced_at.isoformat() if integration.last_synced_at else None
            )
        else:
            return IntegrationStatusResponse(connected=False)

    except Exception as e:
        logger.error("Failed to get Jira status", error=str(e))
        return IntegrationStatusResponse(connected=False, error=str(e))


@router.delete("/jira/disconnect")
async def disconnect_jira(
    user_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Disconnect Jira integration."""
    try:
        uid = uuid.UUID(user_id) if user_id else uuid.uuid4()
        await remove_integration(db, uid, "jira")
        logger.info("Jira disconnected")
        return {"success": True, "message": "Jira disconnected"}
    except Exception as e:
        logger.error("Failed to disconnect Jira", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
