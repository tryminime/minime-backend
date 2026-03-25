"""
Qdrant vector database client for embeddings and semantic search.
"""

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
from typing import Optional, List, Dict, Any
import structlog

from config import settings

logger = structlog.get_logger()

# Global Qdrant client
client: Optional[AsyncQdrantClient] = None

# Collection names
ACTIVITIES_COLLECTION = "activities"
ENTITIES_COLLECTION = "entities"


async def init_qdrant():
    """Initialize Qdrant client."""
    global client
    
    client = AsyncQdrantClient(url=settings.QDRANT_URL)
    
    # Create collections if they don't exist
    await ensure_collections()
    
    logger.info("Qdrant client initialized")


async def close_qdrant():
    """Close Qdrant client."""
    global client
    if client:
        await client.close()
        logger.info("Qdrant client closed")


def get_qdrant_client() -> AsyncQdrantClient:
    """Get Qdrant client instance."""
    if client is None:
        raise RuntimeError("Qdrant client not initialized")
    return client


async def ensure_collections():
    """Create collections if they don't exist."""
    collections = await client.get_collections()
    collection_names = [c.name for c in collections.collections]
    
    # Activities collection (384-dim for sentence-transformers)
    if ACTIVITIES_COLLECTION not in collection_names:
        await client.create_collection(
            collection_name=ACTIVITIES_COLLECTION,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        logger.info(f"Created collection: {ACTIVITIES_COLLECTION}")
    
    # Entities collection
    if ENTITIES_COLLECTION not in collection_names:
        await client.create_collection(
            collection_name=ENTITIES_COLLECTION,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        logger.info(f"Created collection: {ENTITIES_COLLECTION}")


async def upsert_vector(
    collection: str,
    point_id: str,
    vector: List[float],
    payload: Dict[str, Any]
) -> bool:
    """
    Insert or update a vector in a collection.
    
    Args:
        collection: Collection name
        point_id: Unique point ID
        vector: Embedding vector
        payload: Metadata associated with the vector
        
    Returns:
        Success status
    """
    point = PointStruct(
        id=point_id,
        vector=vector,
        payload=payload
    )
    
    await client.upsert(
        collection_name=collection,
        points=[point]
    )
    
    return True


async def search_similar(
    collection: str,
    query_vector: List[float],
    user_id: str,
    limit: int = 10,
    score_threshold: float = 0.7
) -> List[Dict[str, Any]]:
    """
    Search for similar vectors.
    
    Args:
        collection: Collection name
        query_vector: Query embedding vector
        user_id: User ID for filtering
        limit: Maximum results to return
        score_threshold: Minimum similarity score
        
    Returns:
        List of similar items with scores
    """
    results = await client.search(
        collection_name=collection,
        query_vector=query_vector,
        query_filter=Filter(
            must=[FieldCondition(key="user_id", match=MatchValue(value=user_id))]
        ),
        limit=limit,
        score_threshold=score_threshold
    )
    
    return [
        {
            "id": hit.id,
            "score": hit.score,
            "payload": hit.payload
        }
        for hit in results
    ]


async def delete_vector(collection: str, point_id: str) -> bool:
    """
    Delete a vector from a collection.
    
    Args:
        collection: Collection name
        point_id: Point ID to delete
        
    Returns:
        Success status
    """
    await client.delete(
        collection_name=collection,
        points_selector=[point_id]
    )
    return True


async def get_collection_info(collection: str) -> Dict[str, Any]:
    """Get information about a collection."""
    info = await client.get_collection(collection_name=collection)
    return {
        "vectors_count": info.vectors_count,
        "points_count": info.points_count,
        "status": info.status
    }
