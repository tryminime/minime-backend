"""
Qdrant Entity Service for managing entity embeddings in vector database.

Handles:
- Uploading entity embeddings to Qdrant
- Searching for similar entities
- Batch operations
"""

from database.qdrant_client import get_qdrant_client
from typing import List, Dict, Optional
from uuid import UUID
import structlog

logger = structlog.get_logger()


class QdrantEntityService:
    """Service for managing entity embeddings in Qdrant."""
    
    COLLECTION_NAME = "entities"
    
    def __init__(self):
        """Initialize with lazy Qdrant client."""
        self._client = None
    
    @property
    def client(self):
        """Lazy-init the Qdrant client on first access."""
        if self._client is None:
            try:
                self._client = get_qdrant_client()
            except Exception as e:
                logger.debug("Qdrant client not yet available", error=str(e))
                return None
        return self._client
    
    def upload_entity(
        self,
        entity_id: UUID,
        canonical_name: str,
        entity_type: str,
        embedding: List[float]
    ):
        """
        Upload entity embedding to Qdrant.
        
        Args:
            entity_id: Entity UUID
            canonical_name: Entity canonical name
            entity_type: Entity type (PERSON, ORG, etc.)
            embedding: 384-dimensional vector
        """
        if not self.client:
            logger.debug("Qdrant client not initialized, skipping upload")
            return
        
        try:
            from qdrant_client.models import PointStruct
            
            point = PointStruct(
                id=str(entity_id),
                vector=embedding,
                payload={
                    "canonical_name": canonical_name,
                    "type": entity_type
                }
            )
            
            self.client.upsert(
                collection_name=self.COLLECTION_NAME,
                points=[point]
            )
            
            logger.debug("Entity uploaded to Qdrant", entity_id=str(entity_id))
            
        except Exception as e:
            logger.error(
                "Failed to upload entity to Qdrant",
                entity_id=str(entity_id),
                error=str(e)
            )
    
    def upload_batch(self, entities: List[Dict]):
        """
        Upload multiple entities in batch.
        
        Args:
            entities: List of dicts with keys: entity_id, canonical_name, type, embedding
        """
        if not self.client or not entities:
            return
        
        try:
            from qdrant_client.models import PointStruct
            
            points = []
            for entity in entities:
                point = PointStruct(
                    id=str(entity['entity_id']),
                    vector=entity['embedding'],
                    payload={
                        "canonical_name": entity['canonical_name'],
                        "type": entity['type']
                    }
                )
                points.append(point)
            
            self.client.upsert(
                collection_name=self.COLLECTION_NAME,
                points=points
            )
            
            logger.info("Batch uploaded to Qdrant", count=len(entities))
            
        except Exception as e:
            logger.error("Failed to batch upload to Qdrant", error=str(e))
    
    def find_similar_entities(
        self,
        embedding: List[float],
        limit: int = 10,
        score_threshold: float = 0.8
    ) -> List[Dict]:
        """
        Find similar entities in Qdrant using vector similarity.
        
        Args:
            embedding: Query embedding vector
            limit: Max number of results
            score_threshold: Minimum similarity score (0-1)
        
        Returns:
            List of similar entities with scores
        """
        if not self.client:
            logger.debug("Qdrant client not initialized, returning empty results")
            return []
        
        try:
            results = self.client.search(
                collection_name=self.COLLECTION_NAME,
                query_vector=embedding,
                limit=limit,
                score_threshold=score_threshold
            )
            
            similar_entities = []
            for result in results:
                similar_entities.append({
                    "entity_id": result.id,
                    "canonical_name": result.payload.get("canonical_name"),
                    "type": result.payload.get("type"),
                    "similarity": result.score
                })
            
            logger.debug("Found similar entities", count=len(similar_entities))
            return similar_entities
            
        except Exception as e:
            logger.error("Failed to search Qdrant", error=str(e))
            return []
    
    def delete_entity(self, entity_id: UUID):
        """Delete entity from Qdrant."""
        if not self.client:
            return
        
        try:
            from qdrant_client.models import PointIdsList
            
            self.client.delete(
                collection_name=self.COLLECTION_NAME,
                points_selector=PointIdsList(points=[str(entity_id)])
            )
            
            logger.debug("Entity deleted from Qdrant", entity_id=str(entity_id))
            
        except Exception as e:
            logger.error("Failed to delete from Qdrant", entity_id=str(entity_id), error=str(e))


# Global instance
qdrant_entity_service = QdrantEntityService()
