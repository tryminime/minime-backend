"""
Lazy-initialized cloud database clients for background sync.

These clients connect to the cloud counterparts of local databases:
  - Supabase (PostgreSQL)  ← local PostgreSQL
  - Upstash (Redis)        ← local Redis
  - Neo4j AuraDB           ← local Neo4j
  - Qdrant Cloud           ← local Qdrant

Clients are created on first use and cached.  If credentials are missing
the factory raises ``CloudSyncNotConfigured`` so callers can skip gracefully.
"""

from __future__ import annotations

import structlog
from typing import Optional

from config import settings

logger = structlog.get_logger()


class CloudSyncNotConfigured(Exception):
    """Raised when a cloud DB's credentials are missing / empty."""


# ── Singleton caches ──────────────────────────────────────────────────────────

_cloud_pg_engine = None
_cloud_redis = None
_cloud_neo4j_driver = None
_cloud_qdrant = None


# ── Supabase PostgreSQL ───────────────────────────────────────────────────────

async def get_cloud_pg_engine():
    """Return an asyncpg engine connected to Supabase PostgreSQL."""
    global _cloud_pg_engine
    if _cloud_pg_engine is not None:
        return _cloud_pg_engine

    url = settings.SUPABASE_DB_URL
    if not url:
        raise CloudSyncNotConfigured("SUPABASE_DB_URL not set")

    from sqlalchemy.ext.asyncio import create_async_engine
    db_url = url.replace("postgresql://", "postgresql+asyncpg://")
    _cloud_pg_engine = create_async_engine(
        db_url,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )
    logger.info("cloud_pg_engine_initialized")
    return _cloud_pg_engine


# ── Upstash Redis ─────────────────────────────────────────────────────────────

async def get_cloud_redis():
    """Return an async Redis client connected to Upstash (TLS)."""
    global _cloud_redis
    if _cloud_redis is not None:
        return _cloud_redis

    url = settings.UPSTASH_REDIS_URL
    if not url:
        raise CloudSyncNotConfigured("UPSTASH_REDIS_URL not set")

    import redis.asyncio as aioredis
    _cloud_redis = await aioredis.from_url(
        url,
        encoding="utf-8",
        decode_responses=True,
        max_connections=5,
    )
    await _cloud_redis.ping()
    logger.info("cloud_redis_initialized")
    return _cloud_redis


# ── Neo4j AuraDB ──────────────────────────────────────────────────────────────

async def get_cloud_neo4j():
    """Return an async Neo4j driver connected to AuraDB.
    
    Resets cached driver on connectivity failure so retries work
    after AuraDB wakes from paused state.
    """
    global _cloud_neo4j_driver
    if _cloud_neo4j_driver is not None:
        try:
            await _cloud_neo4j_driver.verify_connectivity()
            return _cloud_neo4j_driver
        except Exception:
            # Driver is stale (AuraDB may have been paused/restarted)
            try:
                await _cloud_neo4j_driver.close()
            except Exception:
                pass
            _cloud_neo4j_driver = None

    uri = settings.CLOUD_NEO4J_URI
    user = settings.CLOUD_NEO4J_USERNAME
    pwd = settings.CLOUD_NEO4J_PASSWORD
    if not uri or not pwd:
        raise CloudSyncNotConfigured("CLOUD_NEO4J_URI / CLOUD_NEO4J_PASSWORD not set")

    from neo4j import AsyncGraphDatabase
    _cloud_neo4j_driver = AsyncGraphDatabase.driver(
        uri,
        auth=(user, pwd),
        max_connection_pool_size=10,
        connection_timeout=30,
    )
    await _cloud_neo4j_driver.verify_connectivity()
    logger.info("cloud_neo4j_initialized")
    return _cloud_neo4j_driver


# ── Qdrant Cloud ──────────────────────────────────────────────────────────────

async def get_cloud_qdrant():
    """Return an async Qdrant client connected to Qdrant Cloud."""
    global _cloud_qdrant
    if _cloud_qdrant is not None:
        return _cloud_qdrant

    url = settings.CLOUD_QDRANT_URL
    api_key = settings.CLOUD_QDRANT_API_KEY
    if not url or not api_key:
        raise CloudSyncNotConfigured("CLOUD_QDRANT_URL / CLOUD_QDRANT_API_KEY not set")

    from qdrant_client import AsyncQdrantClient
    _cloud_qdrant = AsyncQdrantClient(url=url, api_key=api_key)
    logger.info("cloud_qdrant_initialized")
    return _cloud_qdrant


# ── Teardown ──────────────────────────────────────────────────────────────────

async def close_all_cloud_clients():
    """Dispose all cached cloud clients (call on app shutdown)."""
    global _cloud_pg_engine, _cloud_redis, _cloud_neo4j_driver, _cloud_qdrant

    if _cloud_pg_engine:
        await _cloud_pg_engine.dispose()
        _cloud_pg_engine = None

    if _cloud_redis:
        await _cloud_redis.close()
        _cloud_redis = None

    if _cloud_neo4j_driver:
        await _cloud_neo4j_driver.close()
        _cloud_neo4j_driver = None

    if _cloud_qdrant:
        await _cloud_qdrant.close()
        _cloud_qdrant = None

    logger.info("cloud_clients_closed")
