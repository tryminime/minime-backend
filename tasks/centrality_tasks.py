"""
Celery Tasks for Centrality Metrics
Scheduled tasks for automatic centrality computation.
"""

from celery import shared_task
from typing import Dict, Any
import logging
from datetime import datetime

from services.centrality_service import centrality_service

logger = logging.getLogger(__name__)


@shared_task(name="compute_centrality_metrics_weekly", bind=True)
def compute_centrality_metrics_weekly(self, user_id: str = None) -> Dict[str, Any]:
    """
    Weekly task to compute centrality metrics for user graphs.
    
    Runs every Sunday at 2 AM (configured in celery beat schedule).
    
    Args:
        user_id: Optional specific user ID. If None, process all users.
        
    Returns:
        Summary of computation results
    """
    logger.info(f"Starting weekly centrality computation (user_id={user_id})")
    
    try:
        # Compute all 5 centrality metrics
        result = centrality_service.compute_all_metrics(
            user_id=user_id or "all_users",
            metrics=["degree", "betweenness", "closeness", "eigenvector", "pagerank"],
            store_results=True
        )
        
        summary = {
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "metrics_computed": result["metrics_computed"],
            "metrics_failed": result["metrics_failed"],
            "execution_time_sec": result["total_execution_time_sec"],
            "status": "success" if len(result["metrics_failed"]) == 0 else "partial",
            "details": {
                metric: result["details"][metric]["execution_time_sec"]
                for metric in result["metrics_computed"]
            }
        }
        
        logger.info(f"Weekly centrality computation complete: {summary}")
        
        return summary
        
    except Exception as e:
        logger.error(f"Weekly centrality computation failed: {e}", exc_info=True)
        raise
