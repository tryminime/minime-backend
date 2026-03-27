#!/usr/bin/env python3
"""
Database initialization script for Render deployment.
Drops and recreates the sessions table (which was originally created with
an incomplete schema by Alembic 001), then runs create_all() for all other tables.
"""

import os
import sys
import time

# Add /app (parent of scripts/) to path so backend modules are importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def init_db():
    """Create all database tables with correct schemas."""
    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text
    
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        print("ERROR: DATABASE_URL not set", flush=True)
        sys.exit(1)
    
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    async_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
    
    print("Connecting to database...", flush=True)
    
    async def run():
        from database.postgres import Base
        import models  # noqa
        import models.analytics_models  # noqa
        import models.integration_models  # noqa
        
        engine = create_async_engine(async_url, echo=False)
        
        async with engine.begin() as conn:
            # Drop sessions table — it was created by Alembic 001 with only 6
            # columns, but the ORM model has 10+. create_all() can't add missing
            # columns to existing tables.
            try:
                await conn.execute(text("DROP TABLE IF EXISTS sessions CASCADE"))
                print("Dropped stale sessions table.", flush=True)
            except Exception:
                pass
            
            # create_all — idempotent for all OTHER tables, creates sessions fresh
            await conn.run_sync(Base.metadata.create_all)
            
            # Ensure is_superadmin on users
            try:
                await conn.execute(text(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_superadmin BOOLEAN DEFAULT false NOT NULL"
                ))
            except Exception:
                pass
        
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
