"""
Neo4j Graph Synchronization Service for Week 8.

Handles:
- Entity node creation/updates
- CO_OCCURS_WITH relationship creation
- Relationship strength calculation
- Graph synchronization with PostgreSQL
"""

from typing import List, Dict, Optional
from uuid import UUID
from datetime import datetime
import structlog

from database.postgres import SessionLocal
from models import Entity, ActivityEntityLink

logger = structlog.get_logger()


class Neo4jGraphSyncService:
    """
    Service for synchronizing entities and relationships to Neo4j knowledge graph.
    
    Handles graceful degradation when Neo4j is not available.
    """
    
    def __init__(self):
        """Initialize with optional Neo4j client."""
        self.neo4j_available = False
        self.neo4j_client = None
        
        try:
            from database.neo4j_client import get_neo4j_client
            self.neo4j_client = get_neo4j_client()
            self.neo4j_available = True
            logger.info("Neo4j graph sync service initialized")
        except Exception as e:
            logger.warning("Neo4j not available, graph sync disabled", error=str(e))
    
    def create_or_update_entity_node(self, entity: Entity) -> bool:
        """
        Create or update entity node in Neo4j.
        
        Args:
            entity: Entity model instance
        
        Returns:
            True if successful, False otherwise
        """
        if not self.neo4j_available:
            logger.debug("Neo4j unavailable, skipping entity node creation")
            return False
        
        try:
            query = """
            MERGE (e:Entity {id: $id})
            SET e.canonical_name = $canonical_name,
                e.type = $type,
                e.user_id = $user_id,
                e.frequency = $frequency,
                e.aliases = $aliases,
                e.updated_at = datetime()
            RETURN e
            """
            
            self.neo4j_client.run(query, {
                'id': str(entity.id),
                'canonical_name': entity.name,
                'type': entity.entity_type,
                'user_id': str(entity.user_id),
                'frequency': entity.occurrence_count or 1,
                'aliases': []
            })
            
            logger.debug("Entity node created/updated in Neo4j", entity_id=str(entity.id))
            return True
            
        except Exception as e:
            logger.error("Failed to create/update entity node", entity_id=str(entity.id), error=str(e))
            return False
    
    def create_co_occurrence_relationships(self, activity_id: UUID) -> int:
        """
        Create CO_OCCURS_WITH relationships between entities that appear in the same activity.
        
        Args:
            activity_id: Activity UUID
        
        Returns:
            Number of relationships created
        """
        if not self.neo4j_available:
            return 0
        
        db = SessionLocal()
        try:
            # Get all entity occurrences for this activity
            occurrences = db.query(ActivityEntityLink).filter(
                ActivityEntityLink.activity_id == activity_id
            ).all()
            
            if len(occurrences) < 2:
                return 0  # Need at least 2 entities to create relationships
            
            relationships_created = 0
            
            # Create relationships between all pairs of entities in this activity
            for i, occ1 in enumerate(occurrences):
                for occ2 in occurrences[i+1:]:
                    strength = self._calculate_relationship_strength(occ1, occ2)
                    
                    if self._create_relationship(
                        entity1_id=occ1.entity_id,
                        entity2_id=occ2.entity_id,
                        relationship_type='CO_OCCURS_WITH',
                        strength=strength,
                        activity_id=activity_id
                    ):
                        relationships_created += 1
            
            logger.info(
                "Created co-occurrence relationships",
                activity_id=str(activity_id),
                count=relationships_created
            )
            
            return relationships_created
            
        except Exception as e:
            logger.error("Failed to create co-occurrence relationships", error=str(e))
            return 0
        finally:
            db.close()
    
    def _calculate_relationship_strength(
        self,
        occ1: ActivityEntityLink,
        occ2: ActivityEntityLink
    ) -> float:
        """
        Calculate relationship strength between two entity occurrences.
        
        Factors:
        - Confidence scores of both entities
        - Proximity in the activity (if position data available)
        - Activity type
        
        Returns:
            Strength score (0.0-1.0)
        """
        # Base strength on relevance scores
        relevance_avg = ((occ1.relevance_score or 1.0) + (occ2.relevance_score or 1.0)) / 2.0
        
        # Boost for high-confidence pairs
        if relevance_avg > 0.9:
            strength = 0.9
        elif relevance_avg > 0.8:
            strength = 0.7
        else:
            strength = 0.5
        
        # Could add proximity calculation if we have position data
        # For now, use simple confidence-based strength
        
        return strength
    
    def _create_relationship(
        self,
        entity1_id: UUID,
        entity2_id: UUID,
        relationship_type: str,
        strength: float,
        activity_id: UUID
    ) -> bool:
        """
        Create a relationship between two entities in Neo4j.
        
        The relationship is bidirectional (undirected).
        """
        if not self.neo4j_available:
            return False
        
        try:
            query = """
            MATCH (e1:Entity {id: $entity1_id})
            MATCH (e2:Entity {id: $entity2_id})
            MERGE (e1)-[r:CO_OCCURS_WITH]-(e2)
            ON CREATE SET r.strength = $strength,
                         r.count = 1,
                         r.first_seen = datetime(),
                         r.last_seen = datetime(),
                         r.activity_ids = [$activity_id]
            ON MATCH SET r.strength = (r.strength + $strength) / 2.0,
                        r.count = r.count + 1,
                        r.last_seen = datetime(),
                        r.activity_ids = r.activity_ids + $activity_id
            RETURN r
            """
            
            self.neo4j_client.run(query, {
                'entity1_id': str(entity1_id),
                'entity2_id': str(entity2_id),
                'strength': strength,
                'activity_id': str(activity_id)
            })
            
            return True
            
        except Exception as e:
            logger.error("Failed to create relationship", error=str(e))
            return False
    
    def infer_relationships(self, entity_id: UUID, min_co_occurrences: int = 3) -> List[Dict]:
        """
        Infer higher-level relationships from co-occurrence patterns.
        
        For example:
        - Person + Organization (frequent) -> WORKS_AT
        - Person + Tool (frequent) -> USES
        - Person + Paper (appears together) -> AUTHORED
        
        Args:
            entity_id: Entity to analyze
            min_co_occurrences: Minimum co-occurrences to consider
        
        Returns:
            List of inferred relationships
        """
        if not self.neo4j_available:
            return []
        
        try:
            # Get entity type
            db = SessionLocal()
            entity = db.query(Entity).filter(Entity.id == entity_id).first()
            
            if not entity:
                return []
            
            # Query Neo4j for co-occurrence patterns
            query = """
            MATCH (e1:Entity {id: $entity_id})-[r:CO_OCCURS_WITH]-(e2:Entity)
            WHERE r.count >= $min_count
            RETURN e2.id as related_id, 
                   e2.canonical_name as related_name,
                   e2.type as related_type,
                   r.count as co_occurrence_count,
                   r.strength as strength
            ORDER BY r.count DESC
            LIMIT 50
            """
            
            results = self.neo4j_client.run(query, {
                'entity_id': str(entity_id),
                'min_count': min_co_occurrences
            })
            
            inferred = []
            for record in results:
                relationship_type = self._infer_relationship_type(
                    entity.entity_type,
                    record['related_type'],
                    record['co_occurrence_count']
                )
                
                inferred.append({
                    'related_entity_id': record['related_id'],
                    'related_entity_name': record['related_name'],
                    'related_entity_type': record['related_type'],
                    'relationship_type': relationship_type,
                    'co_occurrence_count': record['co_occurrence_count'],
                    'confidence': record['strength']
                })
            
            logger.info("Inferred relationships", entity_id=str(entity_id), count=len(inferred))
            return inferred
            
        except Exception as e:
            logger.error("Failed to infer relationships", error=str(e))
            return []
        finally:
            db.close()
    
    def _infer_relationship_type(
        self,
        entity_type: str,
        related_type: str,
        co_occurrence_count: int
    ) -> str:
        """
        Infer relationship type based on entity types and frequency.
        
        Rules:
        - PERSON + ORG (high freq) -> WORKS_AT
        - PERSON + TOOL (high freq) -> USES
        - PERSON + PAPER -> AUTHORED
        - PERSON + PLACE -> LOCATED_AT
        - ORG + TOOL -> USES
        - Default -> CO_OCCURS_WITH
        """
        # High frequency threshold
        high_freq = co_occurrence_count >= 5
        
        # PERSON relationships
        if entity_type == 'PERSON':
            if related_type == 'ORG' and high_freq:
                return 'WORKS_AT'
            elif related_type == 'TOOL' and high_freq:
                return 'USES'
            elif related_type == 'PAPER':
                return 'AUTHORED'
            elif related_type == 'PLACE':
                return 'LOCATED_AT'
        
        # ORG relationships
        elif entity_type == 'ORG':
            if related_type == 'PERSON' and high_freq:
                return 'EMPLOYS'
            elif related_type == 'TOOL':
                return 'USES'
            elif related_type == 'PLACE':
                return 'LOCATED_AT'
        
        # Default fallback
        return 'CO_OCCURS_WITH'


# Global instance
neo4j_sync_service = Neo4jGraphSyncService()
