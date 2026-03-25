"""
Run database migrations for OAuth integrations
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from database.postgres import engine
from models.integration_models import Integration


async def run_migration():
    """Create integrations table."""
    print("Creating integrations table...")
    
    # Read migration SQL
    migration_path = Path(__file__).parent / "005_integrations_table.sql"
    with open(migration_path) as f:
        migration_sql = f.read()
    
    # Execute migration
    async with engine.begin() as conn:
        await conn.execute(text(migration_sql))
    
    print("✅ Migration completed successfully!")
    print("   - Created 'integrations' table")
    print("   - Added indexes")


if __name__ == "__main__":
    asyncio.run(run_migration())
