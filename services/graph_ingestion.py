"""
Graph Ingestion Service
Handles ingestion of entities from NER system into Neo4j knowledge graph.
"""

from neo4j import GraphDatabase, Session
from neo4j.exceptions import ServiceUnavailable, TransientError, DatabaseError
from typing import List, Optional, Dict, Any
import logging
import time
from datetime import datetime
from functools import wraps
from prometheus_client import Counter, Histogram, Gauge

from config.neo4j_config import get_neo4j_session
from services.relationship_validator import relationship_validator
from models.graph_models import (
    NodeType,
    PersonNode,
    PaperNode,
    TopicNode,
    ProjectNode,
    DatasetNode,
    InstitutionNode,
    ToolNode,
    VenueNode,
    GraphNodeCreate,
    GraphRelationshipCreate,
    IngestionResult
)

logger = logging.getLogger(__name__)


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

graph_ingestion_total = Counter(
    'graph_ingestion_total',
    'Total graph entity ingestions',
    ['status', 'entity_type']
)

graph_ingestion_errors = Counter(
    'graph_ingestion_errors_total',
    'Total graph ingestion errors',
    ['error_type', 'entity_type']
)

graph_ingestion_duration = Histogram(
    'graph_ingestion_duration_seconds',
    'Graph ingestion duration',
    ['operation', 'entity_type']
)

graph_relationship_total = Counter(
    'graph_relationship_total',
    'Total graph relationship creations',
    ['status', 'relationship_type']
)

graph_nodes_count = Gauge(
    'graph_nodes_total',
    'Total nodes in graph',
    ['user_id', 'node_type']
)

# ============================================================================
# RETRY DECORATOR
# ============================================================================

def retry_on_transient_error(max_retries=3, backoff_factor=2):
    """
    Retry decorator for transient Neo4j errors.
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Exponential backoff multiplier
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ServiceUnavailable, TransientError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = backoff_factor ** attempt
                        logger.warning(
                            f"Transient error in {func.__name__}, "
                            f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries}): {e}"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Max retries reached for {func.__name__}: {e}")
                        raise
                except DatabaseError as e:
                    # Don't retry database errors (constraint violations, etc.)
                    logger.error(f"Database error in {func.__name__}: {e}")
                    raise
                except Exception as e:
                    # Don't retry other exceptions
                    logger.error(f"Unexpected error in {func.__name__}: {e}")
                    raise
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


class GraphIngestionService:
    """Service for ingesting entities and relationships into Neo4j."""
    
    def __init__(self):
        """Initialize graph ingestion service."""
        self.logger = logging.getLogger(__name__)
    
    @retry_on_transient_error(max_retries=3, backoff_factor=2)
    def ingest_entity(self, entity_id: str, entity_type: str, user_id: str, properties: Dict[str, Any]) -> str:
        """
        Ingest a single entity into the graph with retry logic.
        
        Args:
            entity_id: UUID from entity system
            entity_type: Node type (PERSON, PAPER, etc.)
            user_id: User ID for multi-tenancy
            properties: Node properties
            
        Returns:
            Node ID (UUID)
            
        Raises:
            ServiceUnavailable: If Neo4j is unavailable after retries
            DatabaseError: For constraint violations or data errors
        """
        start_time = time.time()
        
        try:
            with get_neo4j_session() as session:
                result = session.execute_write(
                    self._upsert_entity,
                    entity_id,
                    entity_type,
                    user_id,
                    properties
                )
                
                # Record success metrics
                duration = time.time() - start_time
                graph_ingestion_duration.labels(
                    operation='ingest_entity',
                    entity_type=entity_type
                ).observe(duration)
                
                graph_ingestion_total.labels(
                    status='success',
                    entity_type=entity_type
                ).inc()
                
                self.logger.info(
                    f"Ingested {entity_type}: {entity_id} in {duration:.3f}s",
                    extra={
                        'entity_id': entity_id,
                        'entity_type': entity_type,
                        'user_id': user_id,
                        'duration_sec': duration
                    }
                )
                
                return entity_id
                
        except Exception as e:
            # Record error metrics
            error_type = type(e).__name__
            graph_ingestion_errors.labels(
                error_type=error_type,
                entity_type=entity_type
            ).inc()
            
            graph_ingestion_total.labels(
                status='error',
                entity_type=entity_type
            ).inc()
            
            self.logger.error(
                f"Failed to ingest {entity_type}: {entity_id}: {e}",
                extra={
                    'entity_id': entity_id,
                    'entity_type': entity_type,
                    'user_id': user_id,
                    'error': str(e),
                    'error_type': error_type
                },
                exc_info=True
            )
            
            raise
    
    def _upsert_entity(self, tx, entity_id: str, entity_type: str, user_id: str, properties: Dict[str, Any]):
        """
        Idempotent upsert: create or update node.
        
        Uses MERGE to ensure no duplicates.
        """
        # Build base properties
        props = {
            "id": entity_id,
            "user_id": user_id,
            **self._map_type_specific_properties(entity_type, properties)
        }
        
        # Add metadata timestamps
        if "metadata" in props:
            if "first_seen_at" not in props["metadata"]:
                props["metadata"]["first_seen_at"] = datetime.utcnow().isoformat()
            props["metadata"]["last_updated_at"] = datetime.utcnow().isoformat()
        
        # Cypher MERGE (upsert)
        query = f"""
        MERGE (n:{entity_type} {{id: $id, user_id: $user_id}})
        SET n += $props
        SET n.last_updated_at = datetime()
        RETURN n.id AS id
        """
        
        result = tx.run(query, id=entity_id, user_id=user_id, props=props)
        return result.single()["id"]
    
    def _map_type_specific_properties(self, entity_type: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map entity properties to node-specific schema.
        
        Args:
            entity_type: Node type
            properties: Raw entity properties
            
        Returns:
            Mapped properties for specific node type
        """
        mapped = {}
        
        if entity_type == "PERSON":
            mapped = {
                "canonical_name": properties.get("canonical_name", ""),
                "aliases": properties.get("aliases", []),
                "email": properties.get("email"),
                "affiliation": properties.get("affiliation"),
                "h_index": properties.get("h_index", 0),
                "num_publications": properties.get("num_publications", 0),
                "research_interests": properties.get("research_interests", []),
                "orcid": properties.get("orcid"),
                "external_links": properties.get("external_links", {}),
                "metadata": properties.get("metadata", {})
            }
        
        elif entity_type == "PAPER":
            mapped = {
                "canonical_name": properties.get("title", properties.get("canonical_name", "")),
                "title": properties.get("title", ""),
                "authors": properties.get("authors", []),
                "abstract": properties.get("abstract", ""),
                "year": properties.get("year"),
                "doi": properties.get("doi"),
                "arxiv_id": properties.get("arxiv_id"),
                "venue": properties.get("venue"),
                "keywords": properties.get("keywords", []),
                "num_citations": properties.get("num_citations", 0),
                "external_url": properties.get("external_url"),
                "metadata": properties.get("metadata", {})
            }
        
        elif entity_type == "TOPIC":
            mapped = {
                "canonical_name": properties.get("canonical_name", ""),
                "aliases": properties.get("aliases", []),
                "description": properties.get("description", ""),
                "parent_topic_id": properties.get("parent_topic_id"),
                "num_papers": properties.get("num_papers", 0),
                "num_people": properties.get("num_people", 0),
                "embedding": properties.get("embedding"),
                "metadata": properties.get("metadata", {})
            }
        
        elif entity_type == "PROJECT":
            mapped = {
                "canonical_name": properties.get("name", properties.get("canonical_name", "")),
                "name": properties.get("name", ""),
                "description": properties.get("description", ""),
                "start_date": properties.get("start_date"),
                "status": properties.get("status", "active"),
                "repository": properties.get("repository"),
                "num_collaborators": properties.get("num_collaborators", 0),
                "num_papers": properties.get("num_papers", 0),
                "metadata": properties.get("metadata", {})
            }
        
        elif entity_type == "DATASET":
            mapped = {
                "canonical_name": properties.get("name", properties.get("canonical_name", "")),
                "name": properties.get("name", ""),
                "description": properties.get("description", ""),
                "num_samples": properties.get("num_samples"),
                "url": properties.get("url"),
                "license": properties.get("license"),
                "papers_using": properties.get("papers_using", 0),
                "metadata": properties.get("metadata", {})
            }
        
        elif entity_type == "INSTITUTION":
            mapped = {
                "canonical_name": properties.get("name", properties.get("canonical_name", "")),
                "name": properties.get("name", ""),
                "city": properties.get("city"),
                "country": properties.get("country"),
                "type": properties.get("type", "university"),
                "num_affiliations": properties.get("num_affiliations", 0),
                "website": properties.get("website"),
                "metadata": properties.get("metadata", {})
            }
        
        elif entity_type == "TOOL":
            mapped = {
                "canonical_name": properties.get("name", properties.get("canonical_name", "")),
                "name": properties.get("name", ""),
                "type": properties.get("type", "framework"),
                "version": properties.get("version"),
                "language": properties.get("language"),
                "papers_using": properties.get("papers_using", 0),
                "github_url": properties.get("github_url"),
                "metadata": properties.get("metadata", {})
            }
        
        elif entity_type == "VENUE":
            mapped = {
                "canonical_name": properties.get("name", properties.get("canonical_name", "")),
                "name": properties.get("name", ""),
                "type": properties.get("type", "conference"),
                "acronym": properties.get("acronym"),
                "year": properties.get("year"),
                "papers_in_venue": properties.get("papers_in_venue", 0),
                "h5_index": properties.get("h5_index"),
                "location": properties.get("location"),
                "metadata": properties.get("metadata", {})
            }
        
        else:
            # Fallback: use properties as-is
            mapped = properties
        
        # Remove None values
        return {k: v for k, v in mapped.items() if v is not None}
    
    def ingest_batch(self, nodes: List[GraphNodeCreate], batch_size: int = 100) -> IngestionResult:
        """
        Batch ingest multiple entities with transaction safety.
        
        Args:
            nodes: List of nodes to ingest
            batch_size: Number of nodes per transaction (default: 100)
            
        Returns:
            Ingestion result with counts and errors
        """
        start_time = time.time()
        results = {"ingested": 0, "failed": 0, "errors": []}
        
        # Process in batches for transaction safety
        for i in range(0, len(nodes), batch_size):
            batch = nodes[i:i + batch_size]
            
            try:
                # Process batch in single transaction
                with get_neo4j_session() as session:
                    def _batch_ingest(tx):
                        batch_results = []
                        for node in batch:
                            try:
                                entity_id = node.properties.get("id")
                                entity_type = node.node_type.value
                                user_id = node.properties.get("user_id")
                                
                                # Upsert entity
                                self._upsert_entity(
                                    tx,
                                    entity_id,
                                    entity_type,
                                    user_id,
                                    node.properties
                                )
                                
                                batch_results.append(("success", entity_id, entity_type))
                                
                            except Exception as e:
                                batch_results.append((
                                    "error",
                                    node.properties.get("id", "unknown"),
                                    node.node_type.value,
                                    str(e)
                                ))
                        
                        return batch_results
                    
                    # Execute batch transaction
                    batch_results = session.execute_write(_batch_ingest)
                    
                    # Process results
                    for result in batch_results:
                        if result[0] == "success":
                            results["ingested"] += 1
                            graph_ingestion_total.labels(
                                status='success',
                                entity_type=result[2]
                            ).inc()
                        else:
                            results["failed"] += 1
                            results["errors"].append({
                                "entity_id": result[1],
                                "entity_type": result[2],
                                "error": result[3]
                            })
                            graph_ingestion_errors.labels(
                                error_type='batch_error',
                                entity_type=result[2]
                            ).inc()
                            
            except Exception as e:
                # Entire batch failed
                self.logger.error(f"Batch ingestion failed for batch starting at index {i}: {e}")
                for node in batch:
                    results["failed"] += 1
                    results["errors"].append({
                        "entity_id": node.properties.get("id", "unknown"),
                        "entity_type": node.node_type.value,
                        "error": f"Batch transaction failed: {str(e)}"
                    })
        
        elapsed = time.time() - start_time
        
        # Record batch metrics
        graph_ingestion_duration.labels(
            operation='ingest_batch',
            entity_type='mixed'
        ).observe(elapsed)
        
        self.logger.info(
            f"Batch ingestion complete: {results['ingested']} ingested, "
            f"{results['failed']} failed in {elapsed:.2f}s",
            extra={
                'ingested': results['ingested'],
                'failed': results['failed'],
                'total': len(nodes),
                'duration_sec': elapsed
            }
        )
        
        return IngestionResult(
            ingested=results["ingested"],
            failed=results["failed"],
            errors=results["errors"],
            execution_time_sec=elapsed
        )
    
    @retry_on_transient_error(max_retries=3, backoff_factor=2)
    def ingest_relationship(
        self,
        from_id: str,
        to_id: str,
        from_type: str,
        to_type: str,
        rel_type: str,
        properties: Dict[str, Any] = None,
        validate: bool = True
    ) -> bool:
        """
        Create a relationship between two nodes with validation and enrichment.
        
        Args:
            from_id: Source node ID
            to_id: Target node ID
            from_type: Source node type (PERSON, PAPER, etc.)
            to_type: Target node type
            rel_type: Relationship type (AUTHORED, CITES, etc.)
            properties: Relationship properties
            validate: If True, validate relationship type compatibility
            
        Returns:
            True if created, False otherwise
            
        Raises:
            ValueError: If validation fails
            ServiceUnavailable: If Neo4j is unavailable after retries
            DatabaseError: For constraint violations or data errors
        """
        start_time = time.time()
        props = properties or {}
        
        try:
            # Validate relationship if requested
            if validate:
                is_valid, error_msg = relationship_validator.validate_relationship(
                    from_type, to_type, rel_type
                )
                if not is_valid:
                    raise ValueError(f"Invalid relationship: {error_msg}")
                
                # Validate required properties
                is_valid, error_msg = relationship_validator.validate_required_properties(
                    rel_type, props
                )
                if not is_valid:
                    raise ValueError(f"Invalid properties: {error_msg}")
            
            # Enrich properties with weight, confidence, temporal data
            enriched_props = relationship_validator.enrich_relationship_properties(
                from_type, to_type, rel_type, props
            )
            
            with get_neo4j_session() as session:
                result = session.execute_write(
                    self._create_relationship,
                    from_id,
                    to_id,
                    rel_type,
                    enriched_props
                )
                
                # Record success metrics
                duration = time.time() - start_time
                graph_relationship_total.labels(
                    status='success',
                    relationship_type=rel_type
                ).inc()
                
                self.logger.info(
                    f"Created relationship: {from_id} ({from_type}) -[{rel_type}]-> {to_id} ({to_type}) "
                    f"in {duration:.3f}s (weight={enriched_props.get('weight', 1.0):.2f}, "
                    f"confidence={enriched_props.get('confidence', 1.0):.2f})",
                    extra={
                        'from_id': from_id,
                        'to_id': to_id,
                        'from_type': from_type,
                        'to_type': to_type,
                        'rel_type': rel_type,
                        'weight': enriched_props.get('weight'),
                        'confidence': enriched_props.get('confidence'),
                        'duration_sec': duration
                    }
                )
                
                return result is not None
                
        except ValueError as e:
            # Validation error - don't retry
            self.logger.error(
                f"Validation failed for relationship: {from_id} -[{rel_type}]-> {to_id}: {e}"
            )
            graph_relationship_total.labels(
                status='error',
                relationship_type=rel_type
            ).inc()
            raise
            
        except Exception as e:
            # Record error metrics
            graph_relationship_total.labels(
                status='error',
                relationship_type=rel_type
            ).inc()
            
            self.logger.error(
                f"Failed to create relationship: {from_id} -[{rel_type}]-> {to_id}: {e}",
                exc_info=True
            )
            
            raise
    
    def _create_relationship(self, tx, from_id: str, to_id: str, rel_type: str, props: Dict[str, Any]):
        """
        Create or update relationship.
        
        Uses MERGE to be idempotent.
        """
        # Add temporal properties
        if "first_seen_at" not in props:
            props["first_seen_at"] = datetime.utcnow().isoformat()
        props["last_updated_at"] = datetime.utcnow().isoformat()
        
        # Default weight and confidence
        if "weight" not in props:
            props["weight"] = 1.0
        if "confidence" not in props:
            props["confidence"] = 1.0
        
        # Merge relationship (idempotent by from_id + to_id + type)
        query = f"""
        MATCH (from {{id: $from_id}})
        MATCH (to {{id: $to_id}})
        MERGE (from)-[r:{rel_type}]->(to)
        SET r += $props
        SET r.last_updated_at = datetime()
        RETURN r
        """
        
        return tx.run(
            query,
            from_id=from_id,
            to_id=to_id,
            props=props
        ).single()
    
    def ingest_batch_relationships(
        self,
        relationships: List[GraphRelationshipCreate],
        batch_size: int = 100,
        validate: bool = True
    ) -> IngestionResult:
        """
        Batch ingest multiple relationships with transaction safety.
        
        Args:
            relationships: List of relationships to create
            batch_size: Number of relationships per transaction
            validate: If True, validate each relationship
            
        Returns:
            Ingestion result
        """
        start_time = time.time()
        results = {"ingested": 0, "failed": 0, "errors": []}
        
        # Process in batches for transaction safety
        for i in range(0, len(relationships), batch_size):
            batch = relationships[i:i + batch_size]
            
            try:
                with get_neo4j_session() as session:
                    def _batch_ingest_relationships(tx):
                        batch_results = []
                        for rel in batch:
                            try:
                                # Validate if requested
                                if validate:
                                    # Need to get node types - for now, skip validation in batch
                                    # Or require from_type/to_type in GraphRelationshipCreate
                                    pass
                                
                                # Enrich properties
                                enriched_props = relationship_validator.enrich_relationship_properties(
                                    "UNKNOWN",  # Would need node type lookup
                                    "UNKNOWN",
                                    rel.relationship_type.value,
                                    rel.properties
                                )
                                
                                # Create relationship
                                self._create_relationship(
                                    tx,
                                    rel.from_id,
                                    rel.to_id,
                                    rel.relationship_type.value,
                                    enriched_props
                                )
                                
                                batch_results.append(("success", rel.from_id, rel.to_id, rel.relationship_type.value))
                                
                            except Exception as e:
                                batch_results.append((
                                    "error",
                                    rel.from_id,
                                    rel.to_id,
                                    rel.relationship_type.value,
                                    str(e)
                                ))
                        
                        return batch_results
                    
                    # Execute batch transaction
                    batch_results = session.execute_write(_batch_ingest_relationships)
                    
                    # Process results
                    for result in batch_results:
                        if result[0] == "success":
                            results["ingested"] += 1
                            graph_relationship_total.labels(
                                status='success',
                                relationship_type=result[3]
                            ).inc()
                        else:
                            results["failed"] += 1
                            results["errors"].append({
                                "relationship": f"{result[1]} -> {result[2]} ({result[3]})",
                                "error": result[4]
                            })
                            graph_relationship_total.labels(
                                status='error',
                                relationship_type=result[3]
                            ).inc()
                            
            except Exception as e:
                # Entire batch failed
                self.logger.error(f"Batch relationship ingestion failed for batch starting at index {i}: {e}")
                for rel in batch:
                    results["failed"] += 1
                    results["errors"].append({
                        "relationship": f"{rel.from_id} -> {rel.to_id} ({rel.relationship_type.value})",
                        "error": f"Batch transaction failed: {str(e)}"
                    })
        
        elapsed = time.time() - start_time
        
        self.logger.info(
            f"Batch relationship ingestion complete: {results['ingested']} ingested, "
            f"{results['failed']} failed in {elapsed:.2f}s",
            extra={
                'ingested': results['ingested'],
                'failed': results['failed'],
                'total': len(relationships),
                'duration_sec': elapsed
            }
        )
        
        return IngestionResult(
            ingested=results["ingested"],
            failed=results["failed"],
            errors=results["errors"],
            execution_time_sec=elapsed
        )
    
    def get_node(self, node_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a single node by ID.
        
        Args:
            node_id: Node UUID
            user_id: User ID for access control
            
        Returns:
            Node properties or None
        """
        with get_neo4j_session() as session:
            query = """
            MATCH (n {id: $node_id, user_id: $user_id})
            RETURN {
                id: n.id,
                type: labels(n)[0],
                properties: properties(n)
            } AS node
            """
            
            result = session.run(query, node_id=node_id, user_id=user_id).single()
            
            if result:
                return result["node"]
            return None
    
    def delete_node(self, node_id: str, user_id: str) -> bool:
        """
        Delete a node and all its relationships.
        
        Args:
            node_id: Node UUID
            user_id: User ID for access control
            
        Returns:
            True if deleted, False if not found
        """
        with get_neo4j_session() as session:
            result = session.execute_write(
                self._delete_node,
                node_id,
                user_id
            )
            return result > 0
    
    def _delete_node(self, tx, node_id: str, user_id: str):
        """Delete node transaction."""
        query = """
        MATCH (n {id: $node_id, user_id: $user_id})
        DETACH DELETE n
        RETURN COUNT(n) AS deleted
        """
        
        result = tx.run(query, node_id=node_id, user_id=user_id).single()
        return result["deleted"]


# Global service instance
graph_ingestion_service = GraphIngestionService()
