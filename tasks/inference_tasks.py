"""
Celery Tasks for Relationship Inference
Scheduled tasks for automatic relationship discovery.
"""

from celery import shared_task
from typing import Dict, Any
import logging
from datetime import datetime

from services.relationship_inference import relationship_inference_service
from services.graph_ingestion import graph_ingestion_service

logger = logging.getLogger(__name__)


@shared_task(name="infer_relationships_weekly", bind=True)
def infer_relationships_weekly(self, user_id: str = None) -> Dict[str, Any]:
    """
    Weekly task to infer relationships from user activity.
    
    Runs every Sunday at 2 AM (configured in celery beat schedule).
    
    Args:
        user_id: Optional specific user ID. If None, process all users.
        
    Returns:
        Summary of inference results
    """
    logger.info(f"Starting weekly relationship inference (user_id={user_id})")
    
    try:
        # TODO: Fetch activity logs from database
        # For now, using placeholder
        activity_log = []
        
        # Run inference
        result = relationship_inference_service.batch_infer_from_activity_log(
            user_id=user_id or "all_users",
            activity_log=activity_log,
            lookback_days=90
        )
        
        # Apply confidence thresholds
        filtered_relationships = relationship_inference_service.apply_confidence_thresholds(
            result["relationships"]
        )
        
        # Ingest inferred relationships into graph
        ingested_count = 0
        for rel in filtered_relationships:
            try:
                graph_ingestion_service.ingest_relationship(
                    from_id=rel["from_id"],
                    to_id=rel["to_id"],
                    from_type=rel["from_type"],
                    to_type=rel["to_type"],
                    rel_type=rel["rel_type"],
                    properties={
                        "weight": rel["weight"],
                        "confidence": rel["confidence"],
                        "source": rel["source"],
                        "inference_method": rel["inference_method"],
                        "inferred": True
                    },
                    validate=False  # Skip validation for batch performance
                )
                ingested_count += 1
            except Exception as e:
                logger.error(f"Failed to ingest inferred relationship: {e}")
        
        summary = {
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "activities_analyzed": result["activities_analyzed"],
            "relationships_inferred": result["inferred_count"],
            "relationships_filtered": len(filtered_relationships),
            "relationships_ingested": ingested_count,
            "execution_time_sec": result["execution_time_sec"]
        }
        
        logger.info(f"Weekly inference complete: {summary}")
        
        return summary
        
    except Exception as e:
        logger.error(f"Weekly inference failed: {e}", exc_info=True)
        raise
