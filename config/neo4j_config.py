"""
Neo4j Configuration for MiniMe Knowledge Graph
Handles connection pooling, session management, and graph client setup.
"""

from neo4j import GraphDatabase, Session
from typing import Optional
import logging
from config import settings

logger = logging.getLogger(__name__)


class Neo4jConfig:
    """Neo4j configuration and connection management."""
    
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "neo4j",
        max_connection_lifetime: int = 3600,
        max_connection_pool_size: int = 50,
        connection_acquisition_timeout: int = 60
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        
        # Create driver with connection pooling
        self.driver = GraphDatabase.driver(
            uri,
            auth=(username, password),
            max_connection_lifetime=max_connection_lifetime,
            max_connection_pool_size=max_connection_pool_size,
            connection_acquisition_timeout=connection_acquisition_timeout,
            encrypted=False  # Set to True for production with SSL
        )
        
        logger.info(f"Neo4j driver initialized: {uri}")
    
    def get_session(self, database: Optional[str] = None) -> Session:
        """
        Get a new Neo4j session.
        
        Args:
            database: Database name (defaults to configured database)
            
        Returns:
            Neo4j session
        """
        return self.driver.session(database=database or self.database)
    
    def verify_connectivity(self) -> bool:
        """
        Verify connection to Neo4j.
        
        Returns:
            True if connected, False otherwise
        """
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 AS num")
                record = result.single()
                if record and record["num"] == 1:
                    logger.info("Neo4j connectivity verified")
                    return True
            return False
        except Exception as e:
            logger.error(f"Neo4j connectivity check failed: {e}")
            return False
    
    def close(self):
        """Close the driver and all connections."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j driver closed")
    
    def execute_schema_file(self, filepath: str) -> dict:
        """
        Execute a Cypher schema file (DDL).
        
        Args:
            filepath: Path to .cypher file
            
        Returns:
            Execution summary
        """
        try:
            with open(filepath, 'r') as f:
                cypher_statements = f.read()
            
            # Split on semicolons and execute each statement
            statements = [s.strip() for s in cypher_statements.split(';') if s.strip()]
            
            executed = 0
            skipped = 0
            errors = []
            
            with self.driver.session() as session:
                for statement in statements:
                    # Skip comments
                    if statement.startswith('--') or statement.startswith('//'):
                        continue
                    
                    try:
                        session.run(statement)
                        executed += 1
                        logger.debug(f"Executed: {statement[:50]}...")
                    except Exception as e:
                        # Some statements may fail if already exist (IF NOT EXISTS)
                        error_msg = str(e)
                        if 'already exists' in error_msg.lower():
                            skipped += 1
                        else:
                            errors.append({
                                "statement": statement[:100],
                                "error": error_msg
                            })
                            logger.warning(f"Failed to execute statement: {e}")
            
            summary = {
                "total_statements": len(statements),
                "executed": executed,
                "skipped": skipped,
                "errors": errors
            }
            
            logger.info(f"Schema execution complete: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to execute schema file: {e}")
            raise


# Global Neo4j instance
neo4j_config: Optional[Neo4jConfig] = None


def get_neo4j_config() -> Neo4jConfig:
    """
    Get or create global Neo4j configuration.
    
    Returns:
        Neo4jConfig instance
    """
    global neo4j_config
    
    if neo4j_config is None:
        neo4j_config = Neo4jConfig(
            uri=settings.NEO4J_URI,
            username=settings.NEO4J_USERNAME,
            password=settings.NEO4J_PASSWORD,
            database=settings.NEO4J_DATABASE,
        )
    
    return neo4j_config


def get_neo4j_session() -> Session:
    """
    Get a new Neo4j session from global config.
    
    Returns:
        Neo4j session
    """
    config = get_neo4j_config()
    return config.get_session()


def get_neo4j_driver():
    """
    Get the Neo4j driver from global config.
    
    Returns:
        Neo4j driver instance
    """
    config = get_neo4j_config()
    return config.driver


def close_neo4j():
    """Close global Neo4j driver."""
    global neo4j_config
    if neo4j_config:
        neo4j_config.close()
        neo4j_config = None
