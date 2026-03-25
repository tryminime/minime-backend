"""
Activity deduplication service.
Handles both client_id-based and heuristic deduplication.
Uses async SQLAlchemy queries.
"""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_
from typing import Optional, Tuple
from datetime import datetime, timedelta
from uuid import UUID
import hashlib

from models import Activity
from api.v1.schemas.activity_schemas import ActivityIngestItem


class ActivityDeduplicator:
    """Service for deduplicating activities during ingestion."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def check_duplicate(
        self,
        user_id: UUID,
        source: str,
        activity: ActivityIngestItem
    ) -> Tuple[Optional[Activity], str]:
        """
        Check if activity is a duplicate.
        
        Returns:
            Tuple of (existing_activity, dedup_reason)
            - If duplicate found: (Activity, "client_id" | "heuristic")
            - If not duplicate: (None, "none")
        """
        # Primary dedup: exact client_generated_id match
        if activity.client_generated_id:
            existing = await self.check_client_id_dedup(
                user_id, source, activity.client_generated_id
            )
            if existing:
                return (existing, "client_id")
        
        # Secondary dedup: heuristic matching
        existing = await self.check_heuristic_dedup(user_id, activity)
        if existing:
            return (existing, "heuristic")
        
        return (None, "none")
    
    async def check_client_id_dedup(
        self,
        user_id: UUID,
        source: str,
        client_id: str
    ) -> Optional[Activity]:
        """
        Check deduplication by client_generated_id.
        
        This is the primary dedup mechanism - most reliable.
        """
        stmt = select(Activity).where(
            Activity.user_id == user_id,
            Activity.source == source,
            Activity.client_generated_id == client_id
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()
    
    async def check_heuristic_dedup(
        self,
        user_id: UUID,
        activity: ActivityIngestItem
    ) -> Optional[Activity]:
        """
        Check deduplication using heuristic matching.
        
        Matches activities with:
        - Same user_id
        - Same type
        - Occurred within ±30 seconds
        - Same domain/app (if available)
        - Similar duration bucket
        
        Used as fallback when client_generated_id is missing.
        """
        # Time window: ±30 seconds
        time_window = timedelta(seconds=30)
        time_start = activity.occurred_at - time_window
        time_end = activity.occurred_at + time_window
        
        # Extract domain or app from context
        domain = activity.context.get('domain', '').lower()
        app_name = activity.context.get('app_name', '').lower()
        
        # Build base conditions
        # activity.type may be a string (use_enum_values=True) or an Enum
        activity_type_str = activity.type.value if hasattr(activity.type, 'value') else str(activity.type)
        conditions = [
            Activity.user_id == user_id,
            Activity.type == activity_type_str,
            Activity.occurred_at >= time_start,
            Activity.occurred_at <= time_end
        ]
        
        # Add domain/app filter if available
        # We use the legacy domain/app columns which are always populated during ingestion
        if domain:
            conditions.append(Activity.domain == domain)
        elif app_name:
            conditions.append(Activity.app == app_name)
        
        stmt = select(Activity).where(*conditions)
        result = await self.db.execute(stmt)
        candidates = result.scalars().all()
        
        if not candidates:
            return None
        
        # If we have duration, match by duration bucket
        if activity.duration_seconds is not None:
            activity_bucket = self._bucket_duration(activity.duration_seconds)
            
            for candidate in candidates:
                if candidate.duration_seconds is not None:
                    candidate_bucket = self._bucket_duration(candidate.duration_seconds)
                    if candidate_bucket == activity_bucket:
                        return candidate
        
        # If no duration or no duration match, return first candidate
        # (within same time window + type + domain is usually enough)
        return candidates[0] if candidates else None
    
    def _bucket_duration(self, duration_seconds: int) -> str:
        """
        Bucket duration into ranges for fuzzy matching.
        
        Buckets:
        - 0-5s: "very_short"
        - 5-30s: "short"
        - 30-120s: "medium"
        - 120-600s: "long"
        - 600+: "very_long"
        """
        if duration_seconds < 5:
            return "very_short"
        elif duration_seconds < 30:
            return "short"
        elif duration_seconds < 120:
            return "medium"
        elif duration_seconds < 600:
            return "long"
        else:
            return "very_long"
    
    def generate_dedup_hash(
        self,
        user_id: UUID,
        activity: ActivityIngestItem
    ) -> str:
        """
        Generate a deterministic hash for deduplication.
        
        Used as alternative dedup key when client_generated_id is missing.
        Hash is based on:
        - user_id
        - type
        - occurred_at (bucketed to 10 seconds)
        - domain/app
        - duration_bucket
        """
        # Bucket time to 10-second intervals
        occurred_bucket = int(activity.occurred_at.timestamp() // 10) * 10
        
        # Get duration bucket
        duration_bucket = "unknown"
        if activity.duration_seconds is not None:
            duration_bucket = self._bucket_duration(activity.duration_seconds)
        
        # Extract domain or app
        domain = activity.context.get('domain', '').lower() or \
                activity.context.get('app_name', '').lower()
        
        # Build hash input
        hash_input = f"{user_id}:{activity.type}:{occurred_bucket}:{domain}:{duration_bucket}"
        
        # Generate SHA256 hash (first 16 chars for readability)
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]
