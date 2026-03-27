#!/usr/bin/env python3
"""
Database initialization script for Render deployment.
Uses SQLAlchemy's create_all() which is idempotent (creates tables only if they don't exist).
Then runs ALTER TABLE to add any missing columns to existing tables.
"""

import os
import sys
import time

# Add /app (parent of scripts/) to path so backend modules are importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def init_db():
    """Create all database tables and ensure columns are up to date."""
    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text
    
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        print("ERROR: DATABASE_URL not set", flush=True)
        sys.exit(1)
    
    # SQLAlchemy async requires +asyncpg driver
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    async_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
    
    print("Connecting to database...", flush=True)
    
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
            
            # create_all() does NOT add missing columns to existing tables.
            # Run ALTER TABLE for any columns added after initial table creation.
            alter_statements = [
                "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS device_name VARCHAR(255)",
                "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS remember_device BOOLEAN DEFAULT false NOT NULL",
                "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS ip_address VARCHAR(45)",
                "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS user_agent TEXT",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_superadmin BOOLEAN DEFAULT false NOT NULL",
            ]
            for stmt in alter_statements:
                try:
                    await conn.execute(text(stmt))
                except Exception:
                    pass  # column already exists or other non-fatal issue
        
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
