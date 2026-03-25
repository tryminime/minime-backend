"""
Enrichment API — endpoints to trigger and monitor entity extraction.

Provides:
- POST /enrichment/process-all  — Process all un-enriched activities
- GET  /enrichment/status       — Pipeline status and stats
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from uuid import UUID
from datetime import datetime
import structlog
import time as _time

from database.postgres import get_db
from models import Activity, Entity, ActivityEntityLink
from auth.jwt_handler import get_current_user
from services.lightweight_ner import extract_entities

logger = structlog.get_logger()
router = APIRouter()


async def _find_or_create_entity(
    db: AsyncSession,
    user_id: UUID,
    entity_data: dict,
    activity_id: UUID,
    occurred_at: datetime | None,
) -> Entity:
    """
    Find an existing entity by (user_id, name, entity_type) or create a new one.
    Increments occurrence_count if already exists.
    Also creates the ActivityEntityLink.
    """
    name = entity_data['name']
    entity_type = entity_data['entity_type']
    confidence = entity_data.get('confidence', 0.8)

    # Strip timezone info — Entity.first_seen/last_seen are naive DateTime columns
    naive_at = occurred_at.replace(tzinfo=None) if occurred_at and occurred_at.tzinfo else occurred_at
    now_naive = datetime.utcnow()

    # Check if entity already exists for this user
    stmt = select(Entity).where(
        Entity.user_id == user_id,
        Entity.name == name,
        Entity.entity_type == entity_type,
    )
    result = await db.execute(stmt)
    entity = result.scalar_one_or_none()

    if entity:
        # Update occurrence count and last_seen
        entity.occurrence_count = (entity.occurrence_count or 0) + 1
        if naive_at and (not entity.last_seen or naive_at > entity.last_seen):
            entity.last_seen = naive_at
        if confidence > (entity.confidence or 0):
            entity.confidence = confidence
    else:
        # Create new entity
        entity = Entity(
            user_id=user_id,
            name=name,
            entity_type=entity_type,
            confidence=confidence,
            occurrence_count=1,
            first_seen=naive_at or now_naive,
            last_seen=naive_at or now_naive,
            entity_metadata={'source': entity_data.get('source', 'lightweight_ner')},
        )
        db.add(entity)
        await db.flush()  # Get entity.id

    # Create activity-entity link (skip if already exists)
    link_stmt = select(ActivityEntityLink).where(
        ActivityEntityLink.activity_id == activity_id,
        ActivityEntityLink.entity_id == entity.id,
    )
    link_result = await db.execute(link_stmt)
    existing_link = link_result.scalar_one_or_none()

    if not existing_link:
        link = ActivityEntityLink(
            activity_id=activity_id,
            entity_id=entity.id,
            relevance_score=confidence,
        )
        db.add(link)

    return entity


@router.post("/process-all")
async def process_all_activities(
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Process all activities for the current user and extract entities.
    Runs inline — no Celery/Redis required.
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    start_time = _time.time()

    try:
        # Get all activities for this user
        stmt = select(Activity).where(
            Activity.user_id == user_id
        ).order_by(Activity.created_at.desc())
        result = await db.execute(stmt)
        activities = result.scalars().all()

        total_activities = len(activities)
        total_entities_created = 0
        total_links_created = 0
        entity_type_counts: dict[str, int] = {}

        for activity in activities:
            activity_dict = activity.to_dict()
            extracted = extract_entities(activity_dict)

            for entity_data in extracted:
                entity = await _find_or_create_entity(
                    db=db,
                    user_id=user_id,
                    entity_data=entity_data,
                    activity_id=activity.id,
                    occurred_at=activity.occurred_at,
                )
                total_entities_created += 1
                etype = entity_data['entity_type']
                entity_type_counts[etype] = entity_type_counts.get(etype, 0) + 1

        await db.commit()

        elapsed = round(_time.time() - start_time, 2)

        logger.info(
            "Enrichment process-all complete",
            activities_processed=total_activities,
            entities_extracted=total_entities_created,
            elapsed_seconds=elapsed,
        )

        # Get unique entity count
        count_stmt = select(func.count()).select_from(Entity).where(Entity.user_id == user_id)
        count_result = await db.execute(count_stmt)
        unique_entities = count_result.scalar() or 0

        return {
            "status": "success",
            "activities_processed": total_activities,
            "entity_mentions_found": total_entities_created,
            "unique_entities": unique_entities,
            "by_type": entity_type_counts,
            "processing_time_seconds": elapsed,
        }

    except Exception as e:
        logger.error("Process-all failed", error=str(e))
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@router.get("/status")
async def enrichment_status(
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get enrichment pipeline status and entity statistics.
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    try:
        # Total activities
        act_count = await db.execute(
            select(func.count()).select_from(Activity).where(Activity.user_id == user_id)
        )
        total_activities = act_count.scalar() or 0

        # Total entities
        ent_count = await db.execute(
            select(func.count()).select_from(Entity).where(Entity.user_id == user_id)
        )
        total_entities = ent_count.scalar() or 0

        # Entities by type
        type_stmt = select(Entity.entity_type, func.count()).where(
            Entity.user_id == user_id
        ).group_by(Entity.entity_type)
        type_result = await db.execute(type_stmt)
        by_type = {row[0]: row[1] for row in type_result.fetchall()}

        # Total links
        link_count = await db.execute(
            select(func.count()).select_from(ActivityEntityLink)
        )
        total_links = link_count.scalar() or 0

        # Enriched activities (activities that have at least one entity link)
        enriched_count = await db.execute(
            select(func.count(func.distinct(ActivityEntityLink.activity_id)))
        )
        enriched_activities = enriched_count.scalar() or 0

        return {
            "total_activities": total_activities,
            "total_entities": total_entities,
            "total_links": total_links,
            "enriched_activities": enriched_activities,
            "unenriched_activities": total_activities - enriched_activities,
            "coverage_pct": round((enriched_activities / total_activities * 100) if total_activities > 0 else 0, 1),
            "by_type": by_type,
            "pipeline": "lightweight_ner",
            "stages": ["regex_extract", "normalize", "dedup", "link"],
        }

    except Exception as e:
        logger.error("Status check failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
