"""
Event bus service using Redis Streams.
Publishes activity events for downstream consumers (NER workers, analytics, etc.).
"""

from typing import Dict, Any, Optional
from uuid import UUID
from datetime import datetime
import json
import structlog

logger = structlog.get_logger()


class EventBus:
    """Event publishing service using Redis Streams."""
    
    def __init__(self, redis_client):
        """
        Initialize event bus.
        
        Args:
            redis_client: Redis async client instance
        """
        self.redis = redis_client
    
    async def publish_activity_created(
        self,
        activity_id: UUID,
        user_id: UUID,
        activity_type: str,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Publish activity.created event to Redis Stream.
        
        Args:
            activity_id: Activity UUID
            user_id: User UUID
            activity_type: Activity type string
            context: Optional activity context
            
        Returns:
            bool: True if published successfully
        """
        try:
            event = {
                "event_type": "activity.created",
                "activity_id": str(activity_id),
                "user_id": str(user_id),
                "type": activity_type,
                "context": json.dumps(context) if context else "{}",
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Publish to Redis Stream
            stream_id = await self.redis.xadd(
                "stream:activity.created",
                event
            )
            
            logger.info(
                "event_published",
                event_type="activity.created",
                activity_id=str(activity_id),
                user_id=str(user_id),
                stream_id=stream_id
            )
            
            return True
            
        except Exception as e:
            logger.error(
                "event_publish_failed",
                event_type="activity.created",
                activity_id=str(activity_id),
                error=str(e)
            )
            # Don't raise - event publishing failure shouldn't block ingestion
            return False
    
    async def publish_entity_created(
        self,
        entity_id: UUID,
        user_id: UUID,
        entity_type: str,
        canonical_name: str
    ) -> bool:
        """
        Publish entity.created event to Redis Stream.
        
        Args:
            entity_id: Entity UUID
            user_id: User UUID
            entity_type: Entity type string
            canonical_name: Entity canonical name
            
        Returns:
            bool: True if published successfully
        """
        try:
            event = {
                "event_type": "entity.created",
                "entity_id": str(entity_id),
                "user_id": str(user_id),
                "type": entity_type,
                "canonical_name": canonical_name,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            stream_id = await self.redis.xadd(
                "stream:entity.created",
                event
            )
            
            logger.info(
                "event_published",
                event_type="entity.created",
                entity_id=str(entity_id),
                stream_id=stream_id
            )
            
            return True
            
        except Exception as e:
            logger.error(
                "event_publish_failed",
                event_type="entity.created",
                entity_id=str(entity_id),
                error=str(e)
            )
            return False
    
    async def publish_entity_merged(
        self,
        primary_entity_id: UUID,
        merged_entity_ids: list,
        user_id: UUID
    ) -> bool:
        """
        Publish entity.merged event to Redis Stream.
        
        Args:
            primary_entity_id: Primary entity UUID
            merged_entity_ids: List of merged entity UUIDs
            user_id: User UUID
            
        Returns:
            bool: True if published successfully
        """
        try:
            event = {
                "event_type": "entity.merged",
                "primary_entity_id": str(primary_entity_id),
                "merged_entity_ids": json.dumps([str(id) for id in merged_entity_ids]),
                "user_id": str(user_id),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            stream_id = await self.redis.xadd(
                "stream:entity.merged",
                event
            )
            
            logger.info(
                "event_published",
                event_type="entity.merged",
                primary_entity_id=str(primary_entity_id),
                merged_count=len(merged_entity_ids),
                stream_id=stream_id
            )
            
            return True
            
        except Exception as e:
            logger.error(
                "event_publish_failed",
                event_type="entity.merged",
                primary_entity_id=str(primary_entity_id),
                error=str(e)
            )
            return False
    
    async def publish_activity_enriched(
        self,
        activity_id: UUID,
        user_id: UUID,
        entity_count: int,
        tag_count: int,
        stages_completed: list
    ) -> bool:
        """
        Publish activity.enriched event after successful enrichment pipeline.

        Args:
            activity_id: Activity UUID
            user_id: User UUID
            entity_count: Number of entities extracted
            tag_count: Number of tags assigned
            stages_completed: List of completed pipeline stage names
        """
        try:
            event = {
                "event_type": "activity.enriched",
                "activity_id": str(activity_id),
                "user_id": str(user_id),
                "entity_count": str(entity_count),
                "tag_count": str(tag_count),
                "stages_completed": json.dumps(stages_completed),
                "timestamp": datetime.utcnow().isoformat()
            }

            stream_id = await self.redis.xadd(
                "stream:activity.enriched",
                event
            )

            logger.info(
                "event_published",
                event_type="activity.enriched",
                activity_id=str(activity_id),
                entity_count=entity_count,
                stream_id=stream_id
            )

            return True

        except Exception as e:
            logger.error(
                "event_publish_failed",
                event_type="activity.enriched",
                activity_id=str(activity_id),
                error=str(e)
            )
            return False

    async def publish_enrichment_failed(
        self,
        activity_id: UUID,
        user_id: UUID,
        error_message: str,
        failed_stages: list
    ) -> bool:
        """
        Publish enrichment.failed event for error tracking.

        Args:
            activity_id: Activity UUID
            user_id: User UUID
            error_message: Error description
            failed_stages: List of failed pipeline stage names
        """
        try:
            event = {
                "event_type": "enrichment.failed",
                "activity_id": str(activity_id),
                "user_id": str(user_id),
                "error": error_message[:500],
                "failed_stages": json.dumps(failed_stages),
                "timestamp": datetime.utcnow().isoformat()
            }

            stream_id = await self.redis.xadd(
                "stream:enrichment.failed",
                event
            )

            logger.warning(
                "event_published",
                event_type="enrichment.failed",
                activity_id=str(activity_id),
                failed_stages=failed_stages,
                stream_id=stream_id
            )

            return True

        except Exception as e:
            logger.error(
                "event_publish_failed",
                event_type="enrichment.failed",
                activity_id=str(activity_id),
                error=str(e)
            )
            return False

    async def get_stream_length(self, stream_key: str) -> int:
        """Get the length of a Redis Stream."""
        try:
            return await self.redis.xlen(stream_key)
        except Exception:
            return 0
    
    async def trim_stream(self, stream_key: str, max_len: int = 10000) -> bool:
        """
        Trim stream to maximum length (FIFO removal).
        
        Args:
            stream_key: Redis stream key
            max_len: Maximum number of events to keep
            
        Returns:
            bool: True if trimmed successfully
        """
        try:
            await self.redis.xtrim(stream_key, maxlen=max_len, approximate=True)
            return True
        except Exception as e:
            logger.error(
                "stream_trim_failed",
                stream_key=stream_key,
                error=str(e)
            )
            return False
