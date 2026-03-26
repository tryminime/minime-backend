"""
PostgreSQL database client and session management.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
import structlog

from config import settings

logger = structlog.get_logger()

# Lazy engine initialization — don't create at import time
_engine = None
_SessionLocal = None


def _get_engine():
    """Get or create the sync SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.DATABASE_URL,
            echo=settings.DEBUG,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
        )
    return _engine


def _get_session_factory():
    """Get or create session factory."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=_get_engine()
        )
    return _SessionLocal


# Backward compat: module-level SessionLocal accessed by some code
# Initialized lazily on first get_db() call
SessionLocal = None


def get_db() -> Session:
    """
    FastAPI dependency for database sessions.
    
    Yields:
        SQLAlchemy database session
    """
    global SessionLocal
    if SessionLocal is None:
        SessionLocal = _get_session_factory()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database connection pool."""
    _get_engine()  # Force engine creation
    logger.info("PostgreSQL (sync) connection pool initialized")


def close_db():
    """Close database connections."""
    if _engine is not None:
        _engine.dispose()
    logger.info("PostgreSQL (sync) connection pool closed")
