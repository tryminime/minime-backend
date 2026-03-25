"""
NER Worker - Celery task for entity extraction from activities.

This worker:
1. Consumes activity.created events from Redis
2. Extracts text from activity context
3. Runs spaCy NER to identify entities
4. Normalizes and deduplicates entities
5. Stores entity occurrences
6. Creates Neo4j entity nodes
7. Publishes entity.created events
"""

from config.celery_config import celery_app
from database.postgres import SessionLocal
from models import Activity, Entity, EntityOccurrence
from services.spacy_bert_ner import spacy_bert_ner
from services.entity_normalizer import entity_normalizer
from services.event_bus import EventBus
from database.redis_client import get_redis_client
from sqlalchemy import select
from uuid import UUID
import structlog
import asyncio

logger = structlog.get_logger()


@celery_app.task(name="process_activity_ner", bind=True, max_retries=3)
def process_activity_ner(self, activity_id: str):
    """
    Extract entities from activity using NER.
    
    Args:
        activity_id: UUID string of activity to process
        
    Returns:
        Dict with processing results
    """
    try:
        logger.info("Starting NER processing", activity_id=activity_id)
        
        # Create database session
        db = SessionLocal()
        
        try:
            # Load activity
            activity = db.query(Activity).filter(Activity.id == UUID(activity_id)).first()
            if not activity:
                logger.error("Activity not found", activity_id=activity_id)
                return {"status": "error", "message": "Activity not found"}
            
            # Extract text from activity
            text_blob = extract_text_blob(activity)
            if not text_blob or len(text_blob.strip()) < 5:
                logger.info("No meaningful text to process", activity_id=activity_id)
                return {"status": "skipped", "message": "No text content"}
            
            logger.debug("Extracted text for NER", activity_id=activity_id, text_length=len(text_blob))
            
            # Run NER
            extracted_entities = spacy_bert_ner.extract_entities(text_blob, context=activity.context)
            logger.info("Entities extracted by spaCy", count=len(extracted_entities), activity_id=activity_id)
            
            if not extracted_entities:
                return {"status": "success", "entities_created": 0}
            
            # Normalize and store
            created_count = 0
            occurrence_count = 0
            
            for ent in extracted_entities:
                # Normalize entity
                normalized = entity_normalizer.normalize(
                    text=ent['text'],
                    label=ent['label'],
                    user_id=activity.user_id,
                    context=activity.context or {}
                )
                
                # Skip if normalizer says to skip (e.g., DATE, TIME, etc.)
                if not normalized:
                    continue
                
                # Find or create entity
                entity = find_or_create_entity(db, normalized, activity.user_id)
                if entity:
                    created_count += 1
                    
                    # (Week 8) Sync entity to Neo4j
                    try:
                        from services.neo4j_sync_service import neo4j_sync_service
                        neo4j_sync_service.create_or_update_entity_node(entity)
                    except Exception as neo_exc:
                        logger.debug("Neo4j sync skipped", error=str(neo_exc))
                    
                    # Create occurrence record
                    occurrence = EntityOccurrence(
                        entity_id=entity.id,
                        activity_id=activity.id,
                        user_id=activity.user_id,
                        source_type=determine_source_type(activity),
                        start_offset=ent['start'],
                        end_offset=ent['end'],
                        confidence=ent['confidence'],
                        extracted_text=ent['text'],
                        ner_label=ent['label']
                    )
                    db.add(occurrence)
                    occurrence_count += 1
            
            # Commit all changes
            db.commit()
            
            # (Week 8) Create co-occurrence relationships in Neo4j
            if occurrence_count >= 2:
                try:
                    from services.neo4j_sync_service import neo4j_sync_service
                    neo4j_sync_service.create_co_occurrence_relationships(activity.id)
                except Exception as neo_exc:
                    logger.debug("Neo4j relationship creation skipped", error=str(neo_exc))
            
            logger.info(
                "NER processing complete",
                activity_id=activity_id,
                entities_created=created_count,
                occurrences=occurrence_count
            )
            
            # Publish events (async)
            # Note: We'll implement Neo4j and event publishing in next phase
            
            return {
                "status": "success",
                "entities_created": created_count,
                "occurrences": occurrence_count
            }
            
        finally:
            db.close()
            
    except Exception as exc:
        logger.error("NER processing failed", activity_id=activity_id, error=str(exc), exc_info=True)
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


def extract_text_blob(activity: Activity) -> str:
    """
    Extract all meaningful text from activity for NER processing.
    
    Args:
        activity: Activity model instance
        
    Returns:
        Combined text string suitable for NER
    """
    parts = []
    context = activity.context or {}
    
    # Title (highest priority - most likely to contain entities)
    title = context.get('title', '').strip()
    if title:
        parts.append(title)
    
    # Window title (for desktop apps)
    window_title = context.get('window_title', '').strip()
    if window_title and window_title != title:
        parts.append(window_title)
    
    # App name
    app_name = context.get('app_name', '').strip()
    if app_name:
        parts.append(app_name)
    
    # Domain can contain entity names (e.g., github.com/username/repo)
    domain = context.get('domain', '').strip()
    if domain:
        # Clean up domain: turn slashes and dots into spaces
        domain_cleaned = domain.replace('.', ' ').replace('/', ' ')
        parts.append(domain_cleaned)
    
    # URL path might have entity info
    url = context.get('url', '').strip()
    if url and domain:
        # Extract path from URL (everything after domain)
        try:
            if domain in url:
                path = url.split(domain, 1)[1]
                path_cleaned = path.replace('/', ' ').replace('-', ' ').replace('_', ' ')
                if len(path_cleaned.strip()) > 3:
                    parts.append(path_cleaned)
        except:
            pass
    
    # Content/snippet if available (be careful with length)
    content = context.get('content', '').strip()
    if content:
        # Limit content length to avoid processing too much text
        parts.append(content[:500])
    
    snippet = context.get('snippet', '').strip()
    if snippet and snippet != content:
        parts.append(snippet[:200])
    
    # Join with separator for better sentence detection
    return ' | '.join(filter(None, parts))


def find_or_create_entity(db, normalized: dict, user_id: UUID) -> Entity:
    """
    Find existing entity or create new one.
    
    Uses canonical_name + type for matching.
    Updates aliases if new ones are found.
    
    Args:
        db: Database session
        normalized: Normalized entity dict from entity_normalizer
        user_id: User UUID
        
    Returns:
        Entity model instance
    """
    # Check for existing entity by name and entity_type
    stmt = select(Entity).where(
        Entity.user_id == user_id,
        Entity.name == normalized['canonical_name'],
        Entity.entity_type == normalized['type']
    )
    existing = db.execute(stmt).scalar_one_or_none()
    
    if existing:
        # Update occurrence count
        existing.occurrence_count = (existing.occurrence_count or 1) + 1
        existing.last_seen = func.now()
        
        # Merge external IDs into metadata
        if normalized.get('external_ids'):
            existing_metadata = existing.entity_metadata or {}
            existing_ext_ids = existing_metadata.get('external_ids', {})
            existing_ext_ids.update(normalized['external_ids'])
            existing_metadata['external_ids'] = existing_ext_ids
            existing.entity_metadata = existing_metadata
        
        return existing
    
    # Create new entity
    entity = Entity(
        user_id=user_id,
        name=normalized['canonical_name'],
        entity_type=normalized['type'],
        confidence=0.8,  # Default confidence
        occurrence_count=1,
        entity_metadata={
            'external_ids': normalized.get('external_ids', {}),
            'sources': []
        }
    )
    db.add(entity)
    db.flush()  # Get ID without committing
    
    logger.debug("Created new entity", entity_id=str(entity.id), name=entity.name)
    return entity


def determine_source_type(activity: Activity) -> str:
    """
    Determine where in the activity the entity was likely found.
    
    Args:
        activity: Activity model instance
        
    Returns:
        Source type string ('title', 'url', 'content', etc.)
    """
    if activity.type == 'page_view':
        return 'title'
    elif activity.type in ('app_focus', 'window_focus'):
        return 'window_title'
    elif activity.type == 'desktop_activity':
        return 'app_name'
    else:
        return 'content'


# Test helper function
def test_ner_on_text(text: str) -> list:
    """
    Test NER extraction on arbitrary text (for debugging).
    
    Args:
        text: Text to process
        
    Returns:
        List of extracted entities
    """
    entities = spacy_bert_ner.extract_entities(text)
    
    print(f"\nExtracted {len(entities)} entities from text:")
    print(f"Text: {text[:100]}...\n")
    
    for ent in entities:
        normalized = entity_normalizer.normalize(
            text=ent['text'],
            label=ent['label'],
            user_id=UUID('00000000-0000-0000-0000-000000000000'),
            context={}
        )
        
        if normalized:
            print(f"  - {ent['text']:20} ({ent['label']:10}) → {normalized['canonical_name']:20} [{normalized['type']}]")
        else:
            print(f"  - {ent['text']:20} ({ent['label']:10}) → SKIPPED")
    
    return entities
