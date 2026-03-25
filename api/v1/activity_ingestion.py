"""
Activity batch ingestion API endpoint.
Handles multi-source activity ingestion with deduplication and event publishing.
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select, func
from typing import List
from uuid import UUID
from datetime import datetime
import hashlib
import time
import structlog

from api.v1.schemas.activity_schemas import (
    ActivityBatchRequest,
    ActivityBatchResponse,
    ActivityBatchResponseItem
)
from models import Activity
from database.postgres import get_db
from database.redis_client import get_redis_client
from auth.jwt_handler import get_current_user as get_current_user_from_token
from services.activity_dedup import ActivityDeduplicator
from services.event_bus import EventBus
from websocket.manager import notify_activity_created

logger = structlog.get_logger()
router = APIRouter()


@router.post("/activities/batch", response_model=ActivityBatchResponse)
async def ingest_activity_batch(
    request_data: ActivityBatchRequest,
    request: Request,
    current_user = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
    redis_client = Depends(get_redis_client)
):
    """
    Batch ingest activities from clients.
    
    Features:
    - Validates schema and enforces limits
    - Deduplicates by client_generated_id and heuristics
    - Bulk inserts to PostgreSQL for performance
    - Publishes events to Redis Stream for downstream processing
    - Returns detailed metrics and per-item status
    
    Max batch size: 1000 activities
    Typical processing time: <2s for 1000 activities
    """
    start_time = time.time()
    
    # Initialize services
    deduplicator = ActivityDeduplicator(db)
    event_bus = EventBus(redis_client)
    
    # Get user ID from auth token
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    # Extract client metadata
    ingestion_metadata = {
        "schema_version": 1,
        "ip_hash": _hash_ip(request.client.host) if request.client else None,
        "user_agent": request.headers.get("user-agent"),
        "client_timezone": request.headers.get("x-client-timezone"),
        "received_at": datetime.utcnow().isoformat()
    }
    
    # Process activities
    ingested_activities = []
    duplicates = []
    results = []
    
    for activity_item in request_data.activities:
        # Check for duplicates (async now)
        existing, dedup_reason = await deduplicator.check_duplicate(
            user_id,
            request_data.source,
            activity_item
        )
        
        if existing:
            # Duplicate found
            duplicates.append(activity_item)
            results.append(ActivityBatchResponseItem(
                client_generated_id=activity_item.client_generated_id,
                status="duplicate",
                activity_id=existing.id,
                error=None
            ))
            
            logger.debug(
                "activity_duplicate",
                user_id=str(user_id),
                client_id=activity_item.client_generated_id,
                dedup_reason=dedup_reason,
                existing_id=str(existing.id)
            )
            continue
        
        # Prepare activity for insertion
        # Normalize activity types for consistency
        raw_type = activity_item.type
        TYPE_MAP = {"page_view": "web_visit", "tab_focus": "web_visit",
                     "window_focus": "app_focus", "reading_analytics": "reading_analytics"}
        normalized_type = TYPE_MAP.get(raw_type, raw_type)

        activity_data = {
            "user_id": user_id,
            "source": request_data.source,
            "source_version": request_data.source_version,
            "client_generated_id": activity_item.client_generated_id,
            "type": normalized_type,
            "occurred_at": activity_item.occurred_at,
            "received_at": datetime.utcnow(),
            "duration_seconds": activity_item.duration_seconds,
            "context": activity_item.context,
            "data": activity_item.metadata,
            "ingestion_metadata": ingestion_metadata,
            # Backward compatibility - extract to old fields if present
            "domain": activity_item.context.get("domain"),
            "url": activity_item.context.get("url"),
            "title": activity_item.context.get("title"),
            "app": activity_item.context.get("app_name"),
        }
        
        ingested_activities.append(activity_data)
    
    # Bulk insert to database
    inserted_ids = []
    if ingested_activities:
        try:
            # Use bulk insert for performance
            stmt = insert(Activity).returning(Activity.id)
            result = await db.execute(stmt, ingested_activities)
            inserted_ids = [row[0] for row in result]
            await db.commit()
            
            logger.info(
                "activities_ingested",
                user_id=str(user_id),
                source=request_data.source,
                count=len(inserted_ids),
                duplicates=len(duplicates)
            )
            
        except Exception as e:
            await db.rollback()
            logger.error(
                "batch_insert_failed",
                user_id=str(user_id),
                error=str(e),
                batch_size=len(ingested_activities)
            )
            raise HTTPException(
                status_code=500,
                detail=f"Database insertion failed: {str(e)}"
            )
    
    # Publish events to Redis Stream
    for i, activity_id in enumerate(inserted_ids):
        activity_data = ingested_activities[i]
        
        await event_bus.publish_activity_created(
            activity_id=activity_id,
            user_id=user_id,
            activity_type=activity_data["type"],
            context=activity_data.get("context")
        )
        
        # (Week 7) Queue NER processing task
        try:
            from tasks.ner_worker import process_activity_ner
            process_activity_ner.delay(str(activity_id))
        except Exception as ner_exc:
            logger.warning("Failed to queue NER task", activity_id=str(activity_id), error=str(ner_exc))
        
        results.append(ActivityBatchResponseItem(
            client_generated_id=activity_data.get("client_generated_id"),
            status="ingested",
            activity_id=activity_id,
            error=None
        ))
    
    # Push real-time WebSocket notification to connected dashboard clients
    if inserted_ids:
        try:
            await notify_activity_created(str(user_id), {
                "count": len(inserted_ids),
                "source": request_data.source,
                "types": list(set(a["type"] for a in ingested_activities)),
            })
        except Exception as ws_exc:
            logger.warning("WebSocket notification failed", error=str(ws_exc))
    
    # Calculate metrics
    processing_time_ms = (time.time() - start_time) * 1000
    
    return ActivityBatchResponse(
        ingested_count=len(inserted_ids),
        duplicate_count=len(duplicates),
        failed_count=0,
        results=results,
        processing_time_ms=processing_time_ms
    )


@router.get("/activities/stats")
async def get_ingestion_stats(
    current_user = Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
    redis_client = Depends(get_redis_client)
):
    """
    Get activity ingestion statistics for current user.
    
    Returns:
    - Total activities count
    - Activities by source
    - Recent ingestion rate
    - Event stream depth
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    # Get total count
    count_stmt = select(func.count(Activity.id)).where(
        Activity.user_id == user_id
    )
    count_result = await db.execute(count_stmt)
    total_count = count_result.scalar() or 0
    
    # Get count by source
    by_source_stmt = select(
        Activity.source,
        func.count(Activity.id).label('count')
    ).where(
        Activity.user_id == user_id
    ).group_by(Activity.source)
    by_source_result = await db.execute(by_source_stmt)
    by_source = by_source_result.all()
    
    # Get event stream depth
    event_bus = EventBus(redis_client)
    stream_depth = await event_bus.get_stream_length("stream:activity.created")
    
    return {
        "total_activities": total_count,
        "by_source": {source: count for source, count in by_source},
        "event_stream_depth": stream_depth
    }


def _hash_ip(ip: str) -> str:
    """Hash IP address for privacy (GDPR compliance)."""
    return hashlib.sha256(ip.encode()).hexdigest()[:16]
