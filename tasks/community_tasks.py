"""
Celery Tasks for Community Detection
Scheduled tasks for automatic community detection.
"""

from celery import shared_task
from typing import Dict, Any
import logging
from datetime import datetime

from services.community_service import community_service

logger = logging.getLogger(__name__)


@shared_task(name="detect_communities_weekly", bind=True)
def detect_communities_weekly(self, user_id: str = None) -> Dict[str, Any]:
    """
    Weekly task to detect communities in knowledge graphs.
    
    Runs every Sunday at 4 AM (after embeddings @ 3 AM).
    
    Args:
        user_id: Optional specific user ID. If None, process all users.
        
    Returns:
        Summary of community detection
    """
    logger.info(f"Starting weekly community detection (user_id={user_id})")
    
    try:
        # Detect communities
        result = community_service.detect_communities(
            user_id=user_id or "all_users",
            store_results=True
        )
        
        if result["status"] != "success":
            raise Exception(result.get("error", "Unknown error"))
        
        # Get statistics
        stats = community_service.get_community_statistics(
            user_id=result["user_id"]
        )
        
        summary = {
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "num_communities": result["num_communities"],
            "modularity": result["modularity"],
            "nodes_processed": result["nodes_processed"],
            "execution_time_sec": result["execution_time_sec"],
            "statistics": {
                "avg_community_size": stats["avg_community_size"],
                "min_community_size": stats["min_community_size"],
                "max_community_size": stats["max_community_size"],
                "size_distribution": stats["size_distribution"]
            },
            "status": "success"
        }
        
        logger.info(f"Weekly community detection complete: {summary}")
        
        return summary
        
    except Exception as e:
        logger.error(f"Weekly community detection failed: {e}", exc_info=True)
        raise
