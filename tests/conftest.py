"""
Pytest configuration and shared fixtures.
"""

import pytest
import asyncio
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from fastapi.testclient import TestClient

from main import app
from database.postgres import Base


# =====================================================
# ASYNC EVENT LOOP
# =====================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the entire test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# =====================================================
# TEST DATABASE
# =====================================================

@pytest.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Create a test database session.
    Each test gets a fresh database session that is rolled back after the test.
    """
    # Use in-memory SQLite for testing
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False
    )
    
    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Create session factory
    async_session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    async with async_session_factory() as session:
        yield session
        await session.rollback()
    
    # Cleanup
    await engine.dispose()


# =====================================================
# TEST CLIENT
# =====================================================

@pytest.fixture(scope="function")
def client():
    """Create a test client for making API requests."""
    return TestClient(app)


# =====================================================
# MOCK DATA
# =====================================================

@pytest.fixture
def mock_user_data():
    """Mock user data for testing."""
    return {
        "email": "test@example.com",
        "password": "Test123!@#",
        "full_name": "Test User"
    }


@pytest.fixture
def mock_activity_data():
    """Mock activity event data for testing."""
    return {
        "event_type": "browser",
        "source": "chrome",
        "application": "Google Chrome",
        "title": "MiniMe Documentation",
        "url": "https://docs.minime.ai",
        "domain": "docs.minime.ai",
        "duration_seconds": 300
    }


@pytest.fixture
def mock_entity_data():
    """Mock entity data for testing."""
    return {
        "entity_type": "person",
        "name": "John Doe",
        "confidence": 0.95
    }
