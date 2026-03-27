"""
FastAPI application entry point for MiniMe.
Configures middleware, routes, and application lifecycle.
"""

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
import time
import uuid
import structlog

from config import settings
from database.postgres import init_db, close_db
from database.neo4j_client import init_neo4j, close_neo4j
from database.redis_client import init_redis, close_redis
from database.qdrant_client import init_qdrant, close_qdrant


# Configure structured logging
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    Initializes and closes database connections.
    All inits are non-fatal with timeouts so the app always starts
    and binds to the port (critical for Render deployment).
    """
    import asyncio
    logger.info("Starting MiniMe API", environment=settings.ENVIRONMENT)
    
    # Initialize PostgreSQL (with timeout to prevent hanging on unreachable hosts)
    try:
        await asyncio.wait_for(init_db(), timeout=10.0)
        logger.info("PostgreSQL initialized")
        
        # Ensure all ORM tables exist (idempotent — skips existing tables)
        try:
            from database.postgres import engine, Base
            import models  # noqa — registers User, Activity, Session, etc.
            import models.analytics_models  # noqa
            import models.integration_models  # noqa
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables verified/created")
        except Exception as e:
            logger.warning("Table creation failed (non-fatal)", error=str(e))
            
    except asyncio.TimeoutError:
        logger.warning("PostgreSQL initialization timed out (non-fatal)")
    except Exception as e:
        logger.warning("PostgreSQL initialization failed (non-fatal)", error=str(e))
        
    # Initialize Neo4j (optional — graph explorer won't work without it)
    try:
        await asyncio.wait_for(init_neo4j(), timeout=10.0)
        logger.info("Neo4j initialized")
    except asyncio.TimeoutError:
        logger.warning("Neo4j initialization timed out (non-fatal)")
    except Exception as e:
        logger.warning("Neo4j initialization failed (non-fatal)", error=str(e))
        
    # Initialize Redis (optional — caching/queues degraded without it)
    try:
        await asyncio.wait_for(init_redis(), timeout=10.0)
        logger.info("Redis initialized")
    except asyncio.TimeoutError:
        logger.warning("Redis initialization timed out (non-fatal)")
    except Exception as e:
        logger.warning("Redis initialization failed (non-fatal)", error=str(e))
        
    # Initialize Qdrant (optional — vector search won't work without it)
    try:
        await asyncio.wait_for(init_qdrant(), timeout=10.0)
        logger.info("Qdrant initialized")
    except asyncio.TimeoutError:
        logger.warning("Qdrant initialization timed out (non-fatal)")
    except Exception as e:
        logger.warning("Qdrant initialization failed (non-fatal)", error=str(e))
    
    # Start sync scheduler (Phase 3c — after all DBs are ready)
    try:
        from services.sync_scheduler import start_scheduler
        await asyncio.wait_for(start_scheduler(), timeout=5.0)
        logger.info("Sync scheduler started")
    except Exception as e:
        logger.warning("Sync scheduler start failed (non-fatal)", error=str(e))

    yield
    
    # Cleanup on shutdown
    logger.info("Shutting down MiniMe API")
    # Stop sync scheduler
    try:
        from services.sync_scheduler import stop_scheduler
        await stop_scheduler()
    except Exception:
        pass
    # Close cloud DB clients
    try:
        from database.cloud_db_clients import close_all_cloud_clients
        await close_all_cloud_clients()
    except Exception:
        pass
    try:
        await close_db()
    except Exception:
        pass
    try:
        await close_neo4j()
    except Exception:
        pass
    try:
        await close_redis()
    except Exception:
        pass
    try:
        await close_qdrant()
    except Exception:
        pass
    logger.info("All connections closed")


# Create FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="AI-Powered Activity Intelligence & Knowledge Management Platform",
    lifespan=lifespan,
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
)


# =====================================================
# MIDDLEWARE CONFIGURATION
# =====================================================

# CORS Middleware - MUST be first to handle preflight OPTIONS requests
cors_origins = settings.cors_origins_list
logger.info(f"CORS origins configured: {cors_origins}")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods including OPTIONS
    allow_headers=["*"],  # Allow all headers
    expose_headers=["*"],  # Expose all headers to the browser
)

# GZip Compression
app.add_middleware(GZipMiddleware, minimum_size=1000)


# Request ID and timing middleware
@app.middleware("http")
async def add_request_id_and_timing(request: Request, call_next):
    """Add unique request ID and measure response time."""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time"] = f"{process_time:.4f}"
    
    logger.info(
        "Request completed",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        process_time=f"{process_time:.4f}s"
    )
    
    return response


# =====================================================
# EXCEPTION HANDLERS
# =====================================================

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors with detailed messages."""
    logger.warning(
        "Validation error",
        request_id=getattr(request.state, "request_id", "unknown"),
        errors=exc.errors()
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "detail": exc.errors(),
            "message": "Validation error occurred"
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(
        "Unexpected error",
        request_id=getattr(request.state, "request_id", "unknown"),
        error=str(exc),
        exc_info=True
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Internal server error",
            "message": str(exc) if settings.DEBUG else "An unexpected error occurred"
        }
    )


# =====================================================
# ROUTERS
# =====================================================

# Import and include OAuth Integrations router
from api.v1 import integrations, waitlist
app.include_router(integrations.router)

# Include Waitlist router
app.include_router(waitlist.router, prefix="/api/v1")

# =====================================================
# HEALTH CHECK
# =====================================================

@app.get("/health", tags=["system"])  
async def health_check():
    """System health check."""
    return {
        "status": "healthy",
        "service": "minime",
        "version": settings.VERSION
    }


@app.get("/ready", tags=["Health"])
async def readiness_check():
    """
    Readiness check - verifies all dependencies are available.
    Used by Kubernetes/orchestrators to determine if app can receive traffic.
    """
    checks = {
        "postgres": False,
        "neo4j": False,
        "redis": False,
        "qdrant": False
    }
    
    # Check PostgreSQL connectivity
    try:
        from database.postgres import get_db
        from sqlalchemy import text
        async for db in get_db():
            await db.execute(text("SELECT 1"))
            checks["postgres"] = True
            break
    except Exception:
        checks["postgres"] = False

    # Check Neo4j (optional — skip if not configured)
    try:
        from config.settings import settings as _settings
        if _settings.NEO4J_URI:
            from database.neo4j_client import get_neo4j_driver
            driver = get_neo4j_driver()
            if driver:
                driver.verify_connectivity()
                checks["neo4j"] = True
            else:
                checks["neo4j"] = True  # Not configured = skip
        else:
            checks["neo4j"] = True  # Not configured = skip
    except Exception:
        checks["neo4j"] = False

    # Check Redis (optional — skip if not configured)
    try:
        import redis as _redis
        from config.settings import settings as _rs
        if _rs.REDIS_URL:
            r = _redis.from_url(_rs.REDIS_URL, socket_connect_timeout=2)
            r.ping()
            checks["redis"] = True
        else:
            checks["redis"] = True  # Not configured = skip
    except Exception:
        checks["redis"] = False

    # Check Qdrant (optional — skip if not configured)
    try:
        from config.settings import settings as _qs
        if _qs.QDRANT_URL:
            import httpx
            resp = httpx.get(f"{_qs.QDRANT_URL}/healthz", timeout=2.0)
            checks["qdrant"] = resp.status_code == 200
        else:
            checks["qdrant"] = True  # Not configured = skip
    except Exception:
        checks["qdrant"] = False
    
    all_healthy = all(checks.values())
    
    return {
        "status": "ready" if all_healthy else "not_ready",
        "checks": checks
    }


# =====================================================
# API ROUTES
# =====================================================

# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """API root endpoint."""
    return {
        "message": "Welcome to MiniMe API",
        "version": settings.VERSION,
        "docs": "/docs",
        "health": "/health",
        "api": settings.API_V1_PREFIX
    }


# Import all API route routers
from api.v1 import (
    auth,
    users,
    activities,
    entities,
    graph,
    analytics,
    realtime,
    activity_ingestion,  # Batch ingestion API
    enrichment_api,  # Entity extraction & enrichment
)
from websocket import stream_endpoint  # WebSocket streaming

# Import new settings and AI chat routers
from api import settings as settings_api, ai_chat

# Include all routers
app.include_router(auth.router, prefix=f"{settings.API_V1_PREFIX}/auth", tags=["Authentication"])
app.include_router(users.router, prefix=f"{settings.API_V1_PREFIX}/users", tags=["Users"])
app.include_router(activities.router, prefix=f"{settings.API_V1_PREFIX}/activities", tags=["Activities"])
app.include_router(activity_ingestion.router, prefix=settings.API_V1_PREFIX, tags=["Activity Ingestion"])  # NEW
app.include_router(entities.router, prefix=f"{settings.API_V1_PREFIX}/entities", tags=["Entities"])
app.include_router(enrichment_api.router, prefix=f"{settings.API_V1_PREFIX}/enrichment", tags=["Enrichment"])
app.include_router(graph.router, prefix=f"{settings.API_V1_PREFIX}/graph", tags=["Knowledge Graph"])
app.include_router(analytics.router, prefix=f"{settings.API_V1_PREFIX}/analytics", tags=["Analytics"])
app.include_router(realtime.router, prefix=f"{settings.API_V1_PREFIX}/realtime", tags=["Real-time Updates"])

# Include newSettings and AI Chat routers
app.include_router(settings_api.settings_router, tags=["Settings"])
app.include_router(ai_chat.ai_router, tags=["AI Chat"])

# Include WebSocket streaming
app.include_router(stream_endpoint.router, prefix=settings.API_V1_PREFIX, tags=["WebSocket"])

# Include Screenshots API
from api.v1 import screenshots
app.include_router(screenshots.router, prefix=f"{settings.API_V1_PREFIX}/screenshots", tags=["Screenshots"])

# Include Wearables API
from api.v1 import wearables
app.include_router(wearables.router, prefix=f"{settings.API_V1_PREFIX}/wearables", tags=["Wearables"])

# Include Billing API
from api.v1 import billing
app.include_router(billing.router, prefix=f"{settings.API_V1_PREFIX}/billing", tags=["Billing"])

# Phase 3: Content Intelligence
from api.v1 import content_ingestion, documents as documents_api
app.include_router(content_ingestion.router, tags=["Content Intelligence"])
app.include_router(documents_api.router, tags=["Documents"])

# Phase 2: Cloud Sync (Google Drive + OneDrive)
from api.v1 import cloud_backup
app.include_router(cloud_backup.router, tags=["Cloud Sync"])

# Phase 3b/3c: Cloud Sync Service (local → cloud push + scheduler)
from api.v1 import sync as sync_api
app.include_router(sync_api.router, tags=["Cloud Sync"])

# Phase 4b: Encrypted export/import (.mmexport)
from api.v1 import export as export_api
app.include_router(export_api.router, tags=["Data Export"])

# Phase 4: Account management (GDPR delete + export)
from api.v1 import account as account_api
app.include_router(account_api.router, tags=["Account"])

# Super Admin panel
from api.v1 import admin as admin_api
app.include_router(admin_api.router, prefix=settings.API_V1_PREFIX, tags=["Admin"])




# =====================================================
# WEBSOCKET  /ws  — Dashboard real-time channel
# =====================================================

from fastapi import WebSocket, WebSocketDisconnect
import asyncio
import json
from typing import Set

# Simple in-process connection registry
_ws_clients: Set[WebSocket] = set()


async def broadcast_ws_message(payload: dict) -> None:
    """Broadcast a JSON message to all connected dashboard clients."""
    if not _ws_clients:
        return
    dead: Set[WebSocket] = set()
    for ws in list(_ws_clients):
        try:
            await ws.send_text(json.dumps(payload))
        except Exception:
            dead.add(ws)
    _ws_clients.difference_update(dead)


@app.websocket("/ws")
async def ws_dashboard(websocket: WebSocket):
    """
    Lightweight WebSocket channel for dashboard real-time updates.

    The frontend useWebSocket hook connects here and listens for:
      - {"type": "activity:new"}     → invalidate productivity queries
      - {"type": "metrics:updated"}  → invalidate productivity + collaboration
      - {"type": "graph:updated"}    → invalidate graph + collaboration network
      - {"type": "ping"}             → heartbeat (client ignores)
    """
    await websocket.accept()
    _ws_clients.add(websocket)
    logger.info("WebSocket client connected", total=len(_ws_clients))

    try:
        # Send initial connection confirmation
        await websocket.send_text(json.dumps({"type": "connected", "message": "MiniMe real-time channel ready"}))

        # Keep alive with periodic pings
        while True:
            # Wait up to 30s for an incoming message, then send a ping
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                # Echo back pong for any "ping" messages
                try:
                    data = json.loads(msg)
                    if data.get("type") == "ping":
                        await websocket.send_text(json.dumps({"type": "pong"}))
                except Exception:
                    pass
            except asyncio.TimeoutError:
                # Send heartbeat ping
                await websocket.send_text(json.dumps({"type": "ping"}))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug("WebSocket error", error=str(e))
    finally:
        _ws_clients.discard(websocket)
        logger.info("WebSocket client disconnected", total=len(_ws_clients))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )
