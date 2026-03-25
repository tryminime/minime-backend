#!/usr/bin/env python3
"""
Initialize Neo4j Graph Schema
Run this script after starting Neo4j for the first time.
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from config.neo4j_config import get_neo4j_config
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def initialize_schema():
    """Initialize Neo4j schema with constraints and indexes."""
    
    logger.info("Initializing Neo4j graph schema...")
    
    # Get Neo4j config
    try:
        config = get_neo4j_config()
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {e}")
        logger.error("Make sure Neo4j is running (docker-compose up neo4j)")
        return False
    
    # Verify connectivity
    if not config.verify_connectivity():
        logger.error("Cannot connect to Neo4j. Check connection settings.")
        return False
    
    # Execute schema file
    schema_file = os.path.join(
        os.path.dirname(__file__),
        '..',
        '..',
        'migrations',
        '003_neo4j_schema.cypher'
    )
    
    if not os.path.exists(schema_file):
        logger.error(f"Schema file not found: {schema_file}")
        return False
    
    logger.info(f"Executing schema from: {schema_file}")
    
    try:
        result = config.execute_schema_file(schema_file)
        
        logger.info(f"✅ Schema initialization complete!")
        logger.info(f"   Total statements: {result['total_statements']}")
        logger.info(f"   Executed: {result['executed']}")
        logger.info(f"   Skipped (already exists): {result['skipped']}")
        
        if result['errors']:
            logger.warning(f"   Errors: {len(result['errors'])}")
            for error in result['errors'][:5]:  # Show first 5 errors
                logger.warning(f"      - {error['statement'][:50]}... : {error['error']}")
        
        # Verify constraints and indexes
        logger.info("\nVerifying schema...")
        _verify_schema(config)
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to execute schema: {e}")
        return False
    finally:
        config.close()


def _verify_schema(config):
    """Verify that constraints and indexes were created."""
    
    with config.get_session() as session:
        # Count constraints
        result = session.run("SHOW CONSTRAINTS")
        constraints = list(result)
        logger.info(f"   Constraints created: {len(constraints)}")
        
        # Count indexes
        result = session.run("SHOW INDEXES")
        indexes = list(result)
        logger.info(f"   Indexes created: {len(indexes)}")
        
        # Expected counts
        expected_constraints = 8  # One per node type
        expected_indexes = 30+    # Multiple per type
        
        if len(constraints) >= expected_constraints:
            logger.info(f"   ✅ Constraints OK ({len(constraints)} >= {expected_constraints})")
        else:
            logger.warning(f"   ⚠️  Expected at least {expected_constraints} constraints, found {len(constraints)}")
        
        if len(indexes) >= expected_indexes:
            logger.info(f"   ✅ Indexes OK ({len(indexes)} >= {expected_indexes})")
        else:
            logger.warning(f"   ⚠️  Expected at least {expected_indexes} indexes, found {len(indexes)}")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("MiniMe Knowledge Graph - Schema Initialization")
    logger.info("=" * 60)
    
    success = initialize_schema()
    
    if success:
        logger.info("\n✅ Schema initialization successful!")
        logger.info("You can now use the knowledge graph services.")
        sys.exit(0)
    else:
        logger.error("\n❌ Schema initialization failed!")
        logger.error("Check the error messages above and try again.")
        sys.exit(1)
