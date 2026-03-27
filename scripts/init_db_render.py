#!/usr/bin/env python3
"""
Database initialization script for Render deployment.
Uses SQLAlchemy's create_all() which is idempotent (creates tables only if they don't exist).
This is more reliable than Alembic for initial deployment since it doesn't depend on
migration history state.
"""

import os
import sys
import time

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def init_db():
    """Create all database tables using SQLAlchemy models."""
    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine
    
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        print("ERROR: DATABASE_URL not set", flush=True)
        sys.exit(1)
    
    # SQLAlchemy async requires +asyncpg driver
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    async_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
    
    print(f"Connecting to database...", flush=True)
    
    async def run():
        # Import all models to register them with Base.metadata
        from database.postgres import Base
        import models  # noqa — registers User, Activity, Session, etc.
        import models.analytics_models  # noqa
        import models.integration_models  # noqa
        
        engine = create_async_engine(async_url, echo=False)
        
        async with engine.begin() as conn:
            # create_all is idempotent — skips existing tables
            await conn.run_sync(Base.metadata.create_all)
        
        await engine.dispose()
        print("All tables created/verified.", flush=True)
    
    asyncio.run(run())


if __name__ == "__main__":
    retries = 5
    for attempt in range(retries):
        try:
            init_db()
            break
        except Exception as e:
            print(f"DB init attempt {attempt + 1}/{retries} failed: {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(3)
            else:
                print("All DB init attempts failed, starting server anyway...", flush=True)
