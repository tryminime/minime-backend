"""
Celery Tasks for Node2Vec Embeddings
Scheduled tasks for automatic embedding generation.
"""

from celery import shared_task
from typing import Dict, Any
import logging
from datetime import datetime

from services.node2vec_service import node2vec_service

logger = logging.getLogger(__name__)


@shared_task(name="generate_embeddings_weekly", bind=True)
def generate_embeddings_weekly(self, user_id: str = None) -> Dict[str, Any]:
    """
    Weekly task to generate Node2Vec embeddings.
    
    Runs every Sunday at 3 AM (after centrality computation).
    
    Args:
        user_id: Optional specific user ID. If None, process all users.
        
    Returns:
        Summary of embedding generation
    """
    logger.info(f"Starting weekly embedding generation (user_id={user_id})")
    
    try:
        # Train embeddings
        result = node2vec_service.train_embeddings(
            user_id=user_id or "all_users",
            use_cache=False  # Force fresh computation weekly
        )
        
        if "error" in result:
            raise Exception(result["error"])
        
        # Store in Qdrant for similarity search
        embeddings_count = node2vec_service.store_embeddings_qdrant(
            user_id=result["user_id"],
            embeddings=result["embeddings"],
            metadata=result["metadata"]
        )
        
        # Store reduced embeddings in Neo4j
        neo4j_count = node2vec_service.store_embeddings_neo4j(
            user_id=result["user_id"],
            embeddings=result["embeddings"],
            store_full=False  # Only 8 dims in Neo4j
        )
        
        summary = {
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "num_nodes": result["metadata"]["num_nodes"],
            "num_edges": result["metadata"]["num_edges"],
            "embedding_dimensions": result["metadata"]["dimensions"],
            "training_time_sec": result["metadata"]["training_time_sec"],
            "embeddings_stored_qdrant": embeddings_count,
            "embeddings_stored_neo4j": neo4j_count,
            "status": "success"
        }
        
        logger.info(f"Weekly embedding generation complete: {summary}")
        
        return summary
        
    except Exception as e:
        logger.error(f"Weekly embedding generation failed: {e}", exc_info=True)
        raise
