"""
Neo4j graph database client wrapper.
"""

from neo4j import AsyncGraphDatabase, AsyncDriver
from typing import Optional, Dict, List, Any
import structlog

from config import settings

logger = structlog.get_logger()

# Global driver instance
driver: Optional[AsyncDriver] = None


async def init_neo4j():
    """Initialize Neo4j driver."""
    global driver
    
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
        max_connection_pool_size=50,
        connection_timeout=30
    )
    
    # Verify connectivity
    await driver.verify_connectivity()
    logger.info("Neo4j driver initialized")


async def close_neo4j():
    """Close Neo4j driver."""
    global driver
    if driver:
        await driver.close()
        logger.info("Neo4j driver closed")


def get_neo4j_driver() -> AsyncDriver:
    """Get Neo4j driver instance."""
    if driver is None:
        raise RuntimeError("Neo4j driver not initialized")
    return driver


async def execute_query(query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a read query and return results.
    
    Args:
        query: Cypher query string
        parameters: Query parameters
        
    Returns:
        List of result records as dictionaries
    """
    async with driver.session() as session:
        result = await session.run(query, parameters or {})
        records = await result.data()
        return records


async def execute_write(query: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Execute a write query (CREATE, MERGE, DELETE, etc.).
    
    Args:
        query: Cypher query string
        parameters: Query parameters
        
    Returns:
        Query summary and counters
    """
    async with driver.session() as session:
        result = await session.run(query, parameters or {})
        summary = await result.consume()
        return {
            "counters": summary.counters,
            "query_type": summary.query_type,
            "notifications": summary.notifications
        }


async def create_node(
    user_id: str,
    node_type: str,
    properties: Dict[str, Any]
) -> str:
    """
    Create a node in the knowledge graph.
    
    Args:
        user_id: User ID for multi-tenancy
        node_type: Node label (Person, Project, Skill, etc.)
        properties: Node properties
        
    Returns:
        Created node ID
    """
    import uuid
    node_id = str(uuid.uuid4())
    
    query = f"""
    CREATE (n:{node_type} {{id: $id, user_id: $user_id}})
    SET n += $properties
    RETURN n.id as id
    """
    
    result = await execute_query(
        query,
        {
            "id": node_id,
            "user_id": user_id,
            "properties": properties
        }
    )
    
    return result[0]["id"] if result else node_id


async def create_relationship(
    from_id: str,
    to_id: str,
    relationship_type: str,
    properties: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Create a relationship between two nodes.
    
    Args:
        from_id: Source node ID
        to_id: Target node ID
        relationship_type: Relationship type (e.g., COLLABORATES_WITH)
        properties: Relationship properties
        
    Returns:
        Success status
    """
    query = f"""
    MATCH (a {{id: $from_id}}), (b {{id: $to_id}})
    MERGE (a)-[r:{relationship_type}]->(b)
    SET r += $properties
    RETURN r
    """
    
    result = await execute_query(
        query,
        {
            "from_id": from_id,
            "to_id": to_id,
            "properties": properties or {}
        }
    )
    
    return len(result) > 0


async def find_nodes(
    user_id: str,
    node_type: str,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Find nodes matching criteria.
    
    Args:
        user_id: User ID for filtering
        node_type: Node label to search
        filters: Additional property filters
        limit: Maximum results to return
        
    Returns:
        List of matching nodes
    """
    where_clauses = ["n.user_id = $user_id"]
    params = {"user_id": user_id, "limit": limit}
    
    if filters:
        for key, value in filters.items():
            param_name = f"filter_{key}"
            where_clauses.append(f"n.{key} = ${param_name}")
            params[param_name] = value
    
    where = " AND ".join(where_clauses)
    
    query = f"""
    MATCH (n:{node_type})
    WHERE {where}
    RETURN n
    LIMIT $limit
    """
    
    result = await execute_query(query, params)
    return [record["n"] for record in result]
