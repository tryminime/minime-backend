"""
PostgreSQL database connection and session management.
"""

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool
import structlog

from config import settings

logger = structlog.get_logger()

# Create async engine
engine = None
async_session_factory = None
SessionLocal = None  # Alias for async_session_factory

# Base class for SQLAlchemy models
Base = declarative_base()


async def init_db():
    """Initialize database connection pool."""
    global engine, async_session_factory
    
    # Convert postgresql:// to postgresql+asyncpg://
    db_url = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
    
    engine = create_async_engine(
        db_url,
        echo=settings.DEBUG,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )
    
    async_session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    # Alias for backward compatibility
    global SessionLocal
    SessionLocal = async_session_factory
    
    logger.info("PostgreSQL connection pool initialized")


async def close_db():
    """Close database connections."""
    global engine
    if engine:
        await engine.dispose()
        logger.info("PostgreSQL connections closed")


async def get_db() -> AsyncSession:
    """
    Dependency for FastAPI routes to get database session.
    
    Usage:
        @app.get("/users")
        async def get_users(db: AsyncSession = Depends(get_db)):
            ...
    """
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
