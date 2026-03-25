#!/usr/bin/env python3
"""
Initialize database tables for MiniMe backend
Creates all tables defined in SQLAlchemy models
"""
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.postgres import Base, engine

def init_db():
    """Create all database tables"""
    try:
        print("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        print("✓ Database tables created successfully")
        return True
    except Exception as e:
        print(f"✗ Database initialization failed: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    success = init_db()
    sys.exit(0 if success else 1)
