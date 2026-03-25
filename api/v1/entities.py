"""
Entity API endpoints for Week 8 Entity Intelligence.

Handles:
- Entity listing and retrieval
- Duplicate detection
- Entity merging
- Graph neighbor queries
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from pydantic import BaseModel
from typing import List, Optional, Dict
from uuid import UUID
import structlog

from database.postgres import get_db
from models import Entity, ActivityEntityLink
from auth.jwt_handler import get_current_user
from services.entity_deduplication import deduplication_service

logger = structlog.get_logger()
router = APIRouter()


# =====================================================
# ENTITY LISTING & RETRIEVAL
# =====================================================

@router.get("/")
async def list_entities(
    entity_type: Optional[str] = Query(None, alias="type", description="Filter by entity type"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    List user's entities with filtering and pagination.
    
    Args:
        entity_type: Optional entity type filter (person, organization, skill, etc.)
        limit: Max number of results (default: 100)
        offset: Pagination offset (default: 0)
    
    Returns:
        List of entities with metadata
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    try:
        # Build base query
        stmt = select(Entity).where(Entity.user_id == user_id)
        
        # Apply type filter
        if entity_type:
            stmt = stmt.where(Entity.entity_type == entity_type.lower())
        
        # Get total count
        count_stmt = select(func.count()).select_from(
            stmt.subquery()
        )
        count_result = await db.execute(count_stmt)
        total = count_result.scalar() or 0
        
        # Apply pagination and ordering
        stmt = stmt.order_by(Entity.occurrence_count.desc()).offset(offset).limit(limit)
        result = await db.execute(stmt)
        entities = result.scalars().all()
        
        logger.debug("Entities retrieved", count=len(entities), total=total)
        
        return {
            "entities": [e.to_dict() for e in entities],
            "total": total,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error("Failed to retrieve entities", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# ENTITY STATS — single query for total + per-type counts
# NOTE: Must be defined before /entities/{entity_id}
# =====================================================

@router.get("/stats")
async def get_entity_stats(
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Return total entity count and counts grouped by entity_type
    in a single database query. Replaces 6 individual list calls.
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    try:
        stmt = (
            select(Entity.entity_type, func.count())
            .where(Entity.user_id == user_id)
            .group_by(Entity.entity_type)
        )
        result = await db.execute(stmt)
        rows = result.all()

        by_type = {row[0]: row[1] for row in rows}
        total = sum(by_type.values())

        return {
            "total": total,
            "by_type": by_type,
        }
    except Exception as e:
        logger.error("Failed to get entity stats", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# BULK DEDUPLICATION SCAN
# NOTE: These MUST be defined before /entities/{entity_id}
# so FastAPI doesn't capture 'dedup-scan' as a UUID path param.
# =====================================================

@router.get("/dedup-scan")
async def dedup_scan(
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Full deduplication scan for the authenticated user.

    Runs multi-signal matching (embedding, external-ID, fuzzy-name,
    token-set, alias) across ALL entities and clusters transitive
    duplicates using Union-Find.

    Returns clusters of size ≥ 2 sorted by confidence descending.
    Each cluster includes: members, canonical_id, max/avg confidence,
    match_reasons, recommendation (auto_merge | suggest | review).
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    try:
        result = await deduplication_service.scan_all_for_user(user_id, db)
        logger.info(
            "Dedup scan complete",
            user_id=str(user_id),
            entities_scanned=result["entities_scanned"],
            clusters=len(result["clusters"]),
        )
        return result
    except Exception as e:
        logger.error("Dedup scan failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


class MergeClusterRequest(BaseModel):
    entity_ids:   List[UUID]
    canonical_id: Optional[UUID] = None


@router.post("/dedup-merge-cluster")
async def merge_cluster(
    body: MergeClusterRequest,
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Merge an entire cluster of duplicate entities.

    - canonical_id: which entity to keep (optional; if omitted, the one
      with the highest occurrence_count is chosen automatically).
    - All other entities are merged into canonical, their aliases and
      external IDs are carried forward, occurrence counts summed.
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    if len(body.entity_ids) < 2:
        raise HTTPException(status_code=400, detail="At least 2 entity IDs required")

    try:
        merged = await deduplication_service.merge_cluster(
            entity_ids=body.entity_ids,
            canonical_id=body.canonical_id,
            user_id=user_id,
            db=db,
        )
        if not merged:
            raise HTTPException(status_code=404, detail="Entities not found or merge failed")

        logger.info(
            "Cluster merged",
            user_id=str(user_id),
            entity_count=len(body.entity_ids),
            canonical_id=str(body.canonical_id) if body.canonical_id else None,
        )
        return {
            "status":        "success",
            "merged_count":  len(body.entity_ids) - 1,
            "canonical":     merged,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Cluster merge failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# ENTITY RETRIEVAL BY ID (dynamic — must come after static routes)
# =====================================================

@router.get("/{entity_id}")
async def get_entity(
    entity_id: UUID,
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get entity by ID with linked activity count.
    
    Returns:
        Entity data with linked activity count
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    try:
        # Get entity
        stmt = select(Entity).where(
            Entity.id == entity_id,
            Entity.user_id == user_id
        )
        result = await db.execute(stmt)
        entity = result.scalar_one_or_none()
        
        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")
        
        # Get linked activity count
        link_count_stmt = select(func.count()).where(
            ActivityEntityLink.entity_id == entity_id
        )
        count_result = await db.execute(link_count_stmt)
        linked_activity_count = count_result.scalar() or 0
        
        # Get recent linked activities
        link_stmt = select(ActivityEntityLink).where(
            ActivityEntityLink.entity_id == entity_id
        ).order_by(ActivityEntityLink.created_at.desc()).limit(5)
        link_result = await db.execute(link_stmt)
        recent_links = link_result.scalars().all()
        
        entity_dict = entity.to_dict()
        entity_dict['linked_activity_count'] = linked_activity_count
        entity_dict['recent_activity_links'] = [link.to_dict() for link in recent_links]
        
        return entity_dict
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get entity", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# DUPLICATE DETECTION
# =====================================================

@router.get("/{entity_id}/duplicates")
async def get_entity_duplicates(
    entity_id: UUID,
    threshold: float = Query(0.80, ge=0.0, le=1.0, description="Minimum confidence score"),
    limit: int = Query(20, ge=1, le=100),
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get potential duplicate entities using multi-factor matching.
    
    Args:
        entity_id: Entity to check for duplicates
        threshold: Minimum confidence score (0-1, default: 0.80)
        limit: Max number of duplicates to return
    
    Returns:
        List of duplicate candidates with confidence scores and recommendations
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    try:
        # Get entity
        stmt = select(Entity).where(
            Entity.id == entity_id,
            Entity.user_id == user_id
        )
        result = await db.execute(stmt)
        entity = result.scalar_one_or_none()
        
        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")
        
        # Find duplicates
        duplicates = deduplication_service.find_duplicates(entity, limit=limit)
        
        # Filter by threshold
        duplicates_filtered = [d for d in duplicates if d['confidence'] >= threshold]
        
        logger.info(
            "Found duplicate entities",
            entity_id=str(entity_id),
            total=len(duplicates_filtered),
            threshold=threshold
        )
        
        return {
            "entity_id": str(entity_id),
            "entity_name": entity.name,
            "entity_type": entity.entity_type,
            "duplicates": duplicates_filtered,
            "count": len(duplicates_filtered),
            "thresholds": {
                "auto_merge": deduplication_service.AUTO_MERGE_THRESHOLD,
                "suggest": deduplication_service.SUGGEST_THRESHOLD
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to find duplicates", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# ENTITY MERGING (2-way, kept for backward compat)
# =====================================================

class MergeRequest(BaseModel):
    source_id: UUID
    target_id: UUID


@router.post("/merge")
async def merge_entities(
    request: MergeRequest,
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Merge two duplicate entities (Week 8).
    
    Merges source entity into target:
    - Updates all ActivityEntityLink records
    - Merges metadata
    - Updates occurrence count
    - Deletes source entity
    
    Args:
        source_id: Entity to merge (will be removed)
        target_id: Target entity (will receive merged data)
    
    Returns:
        Merged entity data
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id
    
    try:
        # Perform merge
        merged_entity = deduplication_service.merge_entities(
            source_id=request.source_id,
            target_id=request.target_id,
            user_id=user_id
        )
        
        if not merged_entity:
            raise HTTPException(status_code=404, detail="Entity not found or merge failed")
        
        logger.info(
            "Entities merged via API",
            source_id=str(request.source_id),
            target_id=str(request.target_id),
            user_id=str(user_id)
        )
        
        return {
            "status": "success",
            "message": "Entities merged successfully",
            "merged_entity": merged_entity.to_dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to merge entities", error=str(e))
        raise HTTPException(status_code=500, detail=f"Merge failed: {str(e)}")


# =====================================================
# GRAPH QUERIES (Placeholder for Neo4j integration)
# =====================================================

@router.get("/{entity_id}/neighbors")
async def get_entity_neighbors(
    entity_id: UUID,
    relationship_type: Optional[str] = Query(None, description="Filter by relationship type"),
    current_user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get neighboring entities in the knowledge graph.

    Uses activity_entity_links co-occurrence to find entities that appear
    together in the same activities. Optionally enriched with Qdrant
    semantic similarity for entities without direct co-occurrence links.

    Returns:
        center entity, list of neighbor entities with co-occurrence counts, edges
    """
    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    try:
        # Verify entity exists
        stmt = select(Entity).where(
            Entity.id == entity_id,
            Entity.user_id == user_id
        )
        result = await db.execute(stmt)
        entity = result.scalar_one_or_none()

        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")

        # Get activities where this entity appears
        link_stmt = select(ActivityEntityLink).where(
            ActivityEntityLink.entity_id == entity_id
        )
        link_result = await db.execute(link_stmt)
        links = link_result.scalars().all()

        activity_ids = [link.activity_id for link in links]

        if not activity_ids:
            return {
                "center": entity.to_dict(),
                "neighbors": [],
                "edges": [],
                "count": 0
            }

        # Find other entities linked to the same activities
        co_stmt = select(ActivityEntityLink).where(
            ActivityEntityLink.activity_id.in_(activity_ids),
            ActivityEntityLink.entity_id != entity_id
        )
        co_result = await db.execute(co_stmt)
        co_occurring = co_result.scalars().all()

        # Count co-occurrences per neighbor entity
        neighbor_map: Dict[UUID, int] = {}
        for link in co_occurring:
            neighbor_map[link.entity_id] = neighbor_map.get(link.entity_id, 0) + 1

        # Fetch full entity objects in a single batch query (avoids N+1)
        sorted_neighbors = sorted(neighbor_map.items(), key=lambda x: x[1], reverse=True)[:20]
        neighbor_ids = [nid for nid, _ in sorted_neighbors]

        if neighbor_ids:
            batch_stmt = select(Entity).where(
                Entity.id.in_(neighbor_ids),
                Entity.user_id == user_id,
            )
            batch_result = await db.execute(batch_stmt)
            entity_by_id = {e.id: e for e in batch_result.scalars().all()}
        else:
            entity_by_id = {}

        neighbors = []
        edges = []
        for neighbor_id, count in sorted_neighbors:
            neighbor_entity = entity_by_id.get(neighbor_id)
            if neighbor_entity:
                neighbors.append({
                    "entity": neighbor_entity.to_dict(),
                    "co_occurrence_count": count,
                    "relationship_type": "CO_OCCURS_WITH"
                })
                edges.append({
                    "source": str(entity_id),
                    "target": str(neighbor_entity.id),
                    "weight": count,
                    "relationship_type": "CO_OCCURS_WITH"
                })

        logger.debug("Found neighbor entities", count=len(neighbors))

        return {
            "center": entity.to_dict(),
            "neighbors": neighbors,
            "edges": edges,
            "count": len(neighbors)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get neighbors", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

