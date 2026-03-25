"""
Database initialization script.
Creates all tables in the database.
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.postgres import Base, engine, init_db
from models import User, Session, Activity, Entity, AuditLog
import structlog

logger = structlog.get_logger()


async def create_tables():
    """Create all database tables."""
    try:
        # Initialize database connection
        await init_db()
        
        # Create all tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        logger.info("All database tables created successfully")
        
    except Exception as e:
        logger.error("Failed to create tables", error=str(e))
        raise


if __name__ == "__main__":
    asyncio.run(create_tables())
