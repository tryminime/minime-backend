"""
Celery tasks for Week 8 Entity Intelligence.

Tasks:
- Generate embeddings for entities
- Upload to Qdrant
- Scan for duplicates
- Sync to Neo4j
"""

from config.celery_config import celery_app
from database.postgres import SessionLocal
from models import Entity
from services.embedding_service import embedding_service
from services.qdrant_entity_service import qdrant_entity_service
from services.entity_deduplication import deduplication_service
from typing import Optional
from uuid import UUID
import structlog

logger = structlog.get_logger()


@celery_app.task(name="generate_entity_embedding", bind=True, max_retries=3)
def generate_entity_embedding(self, entity_id: str):
    """
    Generate and store embedding for an entity.
    
    Args:
        entity_id: Entity UUID as string
    
    Returns:
        Dict with status and embedding info
    """
    db = SessionLocal()
    
    try:
        entity = db.query(Entity).filter(Entity.id == UUID(entity_id)).first()
        
        if not entity:
            logger.error("Entity not found for embedding", entity_id=entity_id)
            return {"status": "error", "message": "Entity not found"}
        
        # Generate embedding from entity name
        embedding = embedding_service.generate_embedding(entity.name)
        
        # Store in database
        entity.embedding = embedding
        db.commit()
        
        # Upload to Qdrant
        qdrant_entity_service.upload_entity(
            entity_id=entity.id,
            canonical_name=entity.name,
            entity_type=entity.entity_type,
            embedding=embedding
        )
        
        logger.info(
            "Entity embedding generated",
            entity_id=entity_id,
            name=entity.name
        )
        
        return {
            "status": "success",
            "entity_id": entity_id,
            "embedding_dimension": len(embedding)
        }
        
    except Exception as exc:
        logger.error("Failed to generate embedding", entity_id=entity_id, error=str(exc))
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))
    finally:
        db.close()


@celery_app.task(name="scan_entity_duplicates", bind=True, max_retries=2)
def scan_entity_duplicates(self, entity_id: str, auto_merge_threshold: float = 0.95):
    """
    Scan for duplicates of an entity and optionally auto-merge.
    
    Args:
        entity_id: Entity UUID as string
        auto_merge_threshold: Confidence threshold for automatic merging
    
    Returns:
        Dict with duplicate scan results
    """
    db = SessionLocal()
    
    try:
        entity = db.query(Entity).filter(Entity.id == UUID(entity_id)).first()
        
        if not entity:
            return {"status": "error", "message": "Entity not found"}
        
        # Find duplicates
        duplicates = deduplication_service.find_duplicates(entity, limit=10)
        
        auto_merged_count = 0
        suggested_count = 0
        
        for duplicate in duplicates:
            if duplicate['confidence'] >= auto_merge_threshold:
                # Auto-merge high-confidence duplicates
                merged = deduplication_service.merge_entities(
                    source_id=UUID(duplicate['entity_id']),
                    target_id=entity.id,
                    user_id=entity.user_id
                )
                if merged:
                    auto_merged_count += 1
                    logger.info(
                        "Auto-merged duplicate entity",
                        source=duplicate['entity_id'],
                        target=str(entity.id),
                        confidence=duplicate['confidence']
                    )
            elif duplicate['confidence'] >= 0.80:
                suggested_count += 1
                # TODO: Create notification for user to review
        
        return {
            "status": "success",
            "entity_id": entity_id,
            "duplicates_found": len(duplicates),
            "auto_merged": auto_merged_count,
            "suggested": suggested_count
        }
        
    except Exception as exc:
        logger.error("Failed to scan duplicates", entity_id=entity_id, error=str(exc))
        raise self.retry(exc=exc, countdown=120 * (self.request.retries + 1))
    finally:
        db.close()


@celery_app.task(name="sync_entity_to_neo4j", bind=True, max_retries=3)
def sync_entity_to_neo4j(self, entity_id: str):
    """
    Create or update entity node in Neo4j knowledge graph.
    
    Args:
        entity_id: Entity UUID as string
    
    Returns:
        Dict with sync status
    """
    db = SessionLocal()
    
    try:
        entity = db.query(Entity).filter(Entity.id == UUID(entity_id)).first()
        
        if not entity:
            return {"status": "error", "message": "Entity not found"}
        
        # TODO: Implement Neo4j sync when authentication is set up
        try:
            from database.neo4j_client import get_neo4j_client
            neo4j = get_neo4j_client()
            
            # Create or update entity node
            query = """
            MERGE (e:Entity {id: $id})
            SET e.canonical_name = $canonical_name,
                e.type = $type,
                e.user_id = $user_id,
                e.frequency = $frequency,
                e.updated_at = datetime()
            RETURN e
            """
            
            result = neo4j.run(query, {
                'id': str(entity.id),
                'canonical_name': entity.name,
                'type': entity.entity_type,
                'user_id': str(entity.user_id),
                'frequency': entity.occurrence_count or 1
            })
            
            logger.info("Entity synced to Neo4j", entity_id=entity_id)
            
            return {
                "status": "success",
                "entity_id": entity_id,
                "synced_to": "neo4j"
            }
            
        except Exception as neo_error:
            logger.warning(
                "Neo4j sync skipped (auth not configured)",
                entity_id=entity_id,
                error=str(neo_error)
            )
            return {
                "status": "skipped",
                "message": "Neo4j not configured",
                "entity_id": entity_id
            }
        
    except Exception as exc:
        logger.error("Failed to sync to Neo4j", entity_id=entity_id, error=str(exc))
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))
    finally:
        db.close()


@celery_app.task(name="batch_generate_embeddings", bind=True)
def batch_generate_embeddings(self, limit: int = 100):
    """
    Generate embeddings for entities that don't have them yet.
    
    Args:
        limit: Max number of entities to process in this batch
    
    Returns:
        Dict with batch processing stats
    """
    db = SessionLocal()
    
    try:
        # Get entities without embeddings
        entities = db.query(Entity).filter(
            (Entity.embedding == None) | (Entity.embedding == [])
        ).limit(limit).all()
        
        if not entities:
            logger.info("No entities need embeddings")
            return {"status": "success", "processed": 0}
        
        # Generate embeddings in batch
        texts = [e.name for e in entities]
        embeddings = embedding_service.generate_batch_embeddings(texts)
        
        # Update entities
        for entity, embedding in zip(entities, embeddings):
            entity.embedding = embedding
        
        db.commit()
        
        # Upload to Qdrant in batch
        qdrant_entities = [
            {
                'entity_id': e.id,
                'canonical_name': e.name,
                'type': e.entity_type,
                'embedding': emb
            }
            for e, emb in zip(entities, embeddings)
        ]
        
        qdrant_entity_service.upload_batch(qdrant_entities)
        
        logger.info("Batch embedding generation complete", count=len(entities))
        
        return {
            "status": "success",
            "processed": len(entities),
            "total_pending": limit
        }
        
    except Exception as exc:
        logger.error("Batch embedding generation failed", error=str(exc))
        db.rollback()
        raise
    finally:
        db.close()
