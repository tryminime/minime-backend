"""
PostgreSQL database client and session management.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
import structlog

from config import settings

logger = structlog.get_logger()

# Create synchronous engine for FastAPI dependency
engine = create_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db() -> Session:
    """
    FastAPI dependency for database sessions.
    
    Yields:
        SQLAlchemy database session
        
    Example:
        @app.get("/users")
        def get_users(db: Session = Depends(get_db)):
            return db.query(User).all()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database connection pool."""
    logger.info("PostgreSQL connection pool initialized")


def close_db():
    """Close database connections."""
    engine.dispose()
    logger.info("PostgreSQL connection pool closed")
