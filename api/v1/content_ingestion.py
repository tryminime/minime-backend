"""
Content Ingestion API — Phase 3 backend endpoints.

Now uses **persistent PostgreSQL storage** via ContentItem ORM model.
Content survives server restarts and is available for:
  - Knowledge Graph visualization (graph.py)
  - RAG-powered AI chat (ai_chat.py)
  - Semantic search via Qdrant content_vectors

Provides:
  POST /api/v1/content/ingest   — ingest extracted page or document content
  POST /api/v1/content/search   — semantic search over all ingested content
  GET  /api/v1/content/{id}     — retrieve a content record + NLP analysis
  GET  /api/v1/content/         — list recent content (paginated)
  DELETE /api/v1/content/{id}   — delete a content record
  GET  /api/v1/content/export   — export all content (Pro gated)
  GET  /api/v1/content/stats/summary — stats
"""

import uuid as _uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import select, delete as sa_delete, func
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from services.content_pipeline import content_pipeline
from services.content_vector_store import content_vector_store
from database.postgres import get_db
from models import ContentItem

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/content", tags=["Content Intelligence"])
security = HTTPBearer(auto_error=False)


# ============================================================================
# AUTH HELPERS
# ============================================================================

def _optional_user_id(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> str:
    if credentials is None:
        return ""
    try:
        from auth.jwt_handler import decode_token, verify_token_type
        payload = decode_token(credentials.credentials)
        if payload and verify_token_type(payload, "access"):
            return payload.get("sub", "")
    except Exception:
        pass
    return ""


def _require_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> str:
    uid = _optional_user_id(credentials)
    if not uid:
        raise HTTPException(status_code=401, detail="Authentication required")
    return uid


# ============================================================================
# REQUEST / RESPONSE SCHEMAS
# ============================================================================

class ContentIngestRequest(BaseModel):
    url: str = ""
    title: str = ""
    doc_type: str = "webpage"
    full_text: str = Field(..., min_length=1)
    code_language: Optional[str] = None
    word_count: Optional[int] = None
    user_id: str = ""           # for extension compat — auth takes precedence
    metadata: dict = Field(default_factory=dict)
    # Importance scoring from client
    importance_score: Optional[int] = Field(default=None, ge=0, le=100)
    is_important: Optional[bool] = None
    engagement: Optional[dict] = None   # {scroll_depth_pct, time_on_page_ms, ...}
    links: Optional[list] = None        # outbound links from the page


class ContentSearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    user_id: str = ""
    doc_type_filter: Optional[str] = None
    limit: int = Field(default=10, ge=1, le=50)


class ContentRecord(BaseModel):
    id: str
    url: str
    title: str
    doc_type: str
    word_count: int
    reading_time_seconds: int
    keyphrases: list
    entities: list
    topic: Optional[dict]
    language: str
    complexity: float
    text_snippet: str
    created_at: str
    metadata: dict


# ============================================================================
# ENDPOINTS
# ============================================================================

@router.get("/export")
async def export_content(
    format: str = Query(default="json", pattern="^(json|csv)$"),
    user_id: str = Depends(_require_user),
    db: AsyncSession = Depends(get_db),
):
    """Export all content items for the authenticated user (Pro gated)."""
    result = await db.execute(
        select(ContentItem)
        .where(ContentItem.user_id == _uuid.UUID(user_id))
        .order_by(ContentItem.created_at.desc())
    )
    items = [row.to_dict() for row in result.scalars().all()]

    # Remove full_text from export to keep it manageable
    for item in items:
        item.pop("full_text", None)

    if format == "csv":
        import csv, io
        output = io.StringIO()
        if items:
            writer = csv.DictWriter(output, fieldnames=["id", "title", "url", "doc_type", "word_count", "created_at"])
            writer.writeheader()
            for item in items:
                writer.writerow({k: item.get(k, "") for k in ["id", "title", "url", "doc_type", "word_count", "created_at"]})
        return JSONResponse(
            content={"csv": output.getvalue()},
            headers={"Content-Disposition": "attachment; filename=minime-knowledge-export.csv"},
        )

    return JSONResponse(
        content={
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "count": len(items),
            "items": items,
        },
        headers={"Content-Disposition": "attachment; filename=minime-knowledge-export.json"},
    )


@router.post("/ingest", response_model=ContentRecord)
async def ingest_content(
    body: ContentIngestRequest,
    auth_user_id: str = Depends(_optional_user_id),
    db: AsyncSession = Depends(get_db),
):
    """
    Ingest extracted page or document content.
    Runs NLP pipeline, stores in PostgreSQL + Qdrant.
    """
    effective_user_id = body.user_id or auth_user_id
    if not effective_user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    # Skip localhost / dev-server pages — they clutter the knowledge base
    domain = _extract_domain(body.url).lower()
    if domain and any(d in domain for d in ("localhost", "127.0.0.1", "0.0.0.0")):
        logger.info("content_ingestion_skipped_localhost", url=body.url, domain=domain)
        return JSONResponse(
            status_code=200,
            content={"skipped": True, "reason": "localhost content is not stored in Knowledge Base"},
        )

    # Skip binary file types — the desktop tracker may send raw .db/.sqlite files
    # whose binary content contains null bytes that PostgreSQL UTF8 rejects.
    BINARY_EXTENSIONS = (
        ".db", ".sqlite", ".sqlite3", ".bin", ".exe", ".dll",
        ".so", ".dylib", ".zip", ".tar", ".gz", ".bz2", ".xz",
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico",
        ".pdf", ".docx", ".xlsx", ".pptx",
    )
    url_lower = body.url.lower()
    if any(url_lower.endswith(ext) for ext in BINARY_EXTENSIONS):
        logger.info("content_ingestion_skipped_binary", url=body.url)
        return JSONResponse(
            status_code=200,
            content={"skipped": True, "reason": "binary file types are not stored in Knowledge Base"},
        )

    # Sanitize text — strip PostgreSQL-illegal null bytes (\x00)
    sanitized_text = _sanitize_text(body.full_text)
    if not sanitized_text.strip():
        return JSONResponse(
            status_code=200,
            content={"skipped": True, "reason": "content is empty after sanitization"},
        )

    content_id = _uuid.uuid4()

    # Build context for NLP
    context = {
        "url": body.url,
        "domain": _extract_domain(body.url),
        "app_name": body.doc_type,
    }
    context.update(body.metadata)

    # Run NLP pipeline (on sanitized text)
    try:
        analysis = content_pipeline.process(sanitized_text, context=context)
    except Exception as e:
        logger.error("content_pipeline_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"NLP pipeline failed: {e}")

    word_count = body.word_count or analysis.word_count
    text_snippet = _sanitize_text(sanitized_text[:500].strip())

    entities_list = [
        {"text": e["text"], "label": e["label"]}
        for e in analysis.entities[:15]
    ]
    topic_dict = analysis.topic.__dict__ if analysis.topic else None

    # Merge client metadata with importance signals
    importance_score = body.importance_score or 0
    is_important = body.is_important if body.is_important is not None else (importance_score >= 50)
    enriched_metadata = {
        **body.metadata,
        "importance_score": importance_score,
        "is_important": is_important,
        "engagement": body.engagement or {},
        "links": (body.links or [])[:20],
        # Strategic graph marker: pages >= 50 become knowledge graph nodes
        "knowledge_node": importance_score >= 50,
        # Tags for filtering in graph explorer
        "graph_tags": _compute_graph_tags(importance_score, body.doc_type, analysis),
    }

    # Create DB record (all text fields sanitized)
    db_item = ContentItem(
        id=content_id,
        user_id=_uuid.UUID(effective_user_id),
        url=_sanitize_text(body.url),
        title=_sanitize_text(body.title or _title_from_url(body.url)),
        doc_type=body.doc_type,
        full_text=sanitized_text,
        text_snippet=text_snippet,
        word_count=word_count,
        reading_time_seconds=analysis.reading_time_seconds,
        keyphrases=analysis.keyphrases,
        entities=entities_list,
        topic=topic_dict,
        language=analysis.language,
        complexity=analysis.complexity,
        content_metadata=enriched_metadata,
    )
    db.add(db_item)
    await db.commit()
    await db.refresh(db_item)

    # Embed + store in Qdrant (non-blocking)
    vector_meta = {
        "title": db_item.title,
        "url": body.url,
        "doc_type": body.doc_type,
    }
    try:
        await content_vector_store.upsert_chunks(
            content_id=str(content_id),
            text=sanitized_text,
            user_id=effective_user_id,
            metadata=vector_meta,
        )
    except Exception as e:
        logger.warning("vector_store_unavailable", error=str(e))

    logger.info(
        "content_ingested",
        id=str(content_id),
        doc_type=body.doc_type,
        words=word_count,
        importance_score=importance_score,
        knowledge_node=importance_score >= 50,
        keyphrases=len(analysis.keyphrases),
        persistent=True,
    )

    return ContentRecord(**db_item.to_dict())


@router.post("/search")
async def search_content(
    body: ContentSearchRequest,
    db: AsyncSession = Depends(get_db),
):
    """Semantic search over all ingested content."""
    effective_user_id = body.user_id
    results = []

    # Try Qdrant vector search
    try:
        vector_results = await content_vector_store.search(
            query=body.query,
            user_id=effective_user_id,
            limit=body.limit,
            doc_type_filter=body.doc_type_filter,
        )
        # Enrich with DB record data
        for vr in vector_results:
            try:
                cid = _uuid.UUID(vr["content_id"])
                rec = await db.get(ContentItem, cid)
                if rec:
                    d = rec.to_dict()
                    results.append({
                        **vr,
                        "keyphrases": d.get("keyphrases", []),
                        "topic": d.get("topic"),
                        "language": d.get("language", "en"),
                    })
                else:
                    results.append(vr)
            except Exception:
                results.append(vr)
    except Exception as e:
        logger.warning("vector_search_failed_using_keyword_fallback", error=str(e))

    # Keyword fallback
    if not results:
        stmt = select(ContentItem)
        if effective_user_id:
            stmt = stmt.where(ContentItem.user_id == _uuid.UUID(effective_user_id))
        stmt = stmt.order_by(ContentItem.created_at.desc()).limit(200)
        result = await db.execute(stmt)
        all_records = result.scalars().all()

        query_lower = body.query.lower()
        for rec in all_records:
            score = 0.0
            if query_lower in (rec.title or "").lower():
                score += 0.6
            if any(query_lower in kp.lower() for kp in (rec.keyphrases or [])):
                score += 0.3
            if query_lower in (rec.text_snippet or "").lower():
                score += 0.2
            if score > 0:
                results.append({
                    "content_id": str(rec.id),
                    "score": round(score, 3),
                    "title": rec.title,
                    "url": rec.url,
                    "doc_type": rec.doc_type,
                    "snippet": rec.text_snippet,
                    "keyphrases": rec.keyphrases or [],
                    "topic": rec.topic,
                    "language": rec.language or "en",
                })
        results.sort(key=lambda x: x["score"], reverse=True)
        results = results[:body.limit]

    return {
        "query": body.query,
        "results": results,
        "total": len(results),
        "search_type": "vector" if results and "chunk_index" in results[0] else "keyword",
    }


@router.get("/")
async def list_content(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    doc_type: Optional[str] = None,
    exclude_types: Optional[str] = Query(default=None, description="Comma-separated doc types to exclude"),
    user_id: str = Depends(_optional_user_id),
    db: AsyncSession = Depends(get_db),
):
    """List recently ingested content, paginated — filtered to the requesting user."""
    stmt = select(ContentItem)
    count_stmt = select(func.count(ContentItem.id))

    if user_id:
        uid = _uuid.UUID(user_id)
        stmt = stmt.where(ContentItem.user_id == uid)
        count_stmt = count_stmt.where(ContentItem.user_id == uid)

    if doc_type:
        stmt = stmt.where(ContentItem.doc_type == doc_type)
        count_stmt = count_stmt.where(ContentItem.doc_type == doc_type)
    elif exclude_types:
        excluded = [t.strip() for t in exclude_types.split(",") if t.strip()]
        if excluded:
            stmt = stmt.where(~ContentItem.doc_type.in_(excluded))
            count_stmt = count_stmt.where(~ContentItem.doc_type.in_(excluded))

    total = (await db.execute(count_stmt)).scalar() or 0

    stmt = stmt.order_by(ContentItem.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(stmt)
    items = result.scalars().all()

    return {
        "items": [
            {
                "id": str(r.id),
                "title": r.title,
                "url": r.url or "",
                "doc_type": r.doc_type,
                "word_count": r.word_count or 0,
                "keyphrases": (r.keyphrases or [])[:5],
                "topic": r.topic,
                "text_snippet": r.text_snippet or "",
                "created_at": r.created_at.isoformat() if r.created_at else "",
            }
            for r in items
        ],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.get("/stats/summary")
async def content_stats(
    user_id: str = Depends(_optional_user_id),
    db: AsyncSession = Depends(get_db),
):
    """Return content store statistics."""
    stmt = select(ContentItem.doc_type, func.count(ContentItem.id))
    if user_id:
        stmt = stmt.where(ContentItem.user_id == _uuid.UUID(user_id))
    stmt = stmt.group_by(ContentItem.doc_type)
    result = await db.execute(stmt)
    doc_types = {row[0]: row[1] for row in result.all()}

    total = sum(doc_types.values())

    vector_stats = {}
    try:
        vector_stats = await content_vector_store.get_stats()
    except Exception:
        pass

    return {
        "total_records": total,
        "by_doc_type": doc_types,
        "vector_store": vector_stats,
    }


@router.get("/{content_id}", response_model=ContentRecord)
async def get_content(
    content_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Retrieve a specific content record with full NLP analysis."""
    try:
        cid = _uuid.UUID(content_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid content ID")

    record = await db.get(ContentItem, cid)
    if not record:
        raise HTTPException(status_code=404, detail="Content record not found")

    return ContentRecord(**record.to_dict())


@router.delete("/{content_id}")
async def delete_content(
    content_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a content record and its vectors."""
    try:
        cid = _uuid.UUID(content_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid content ID")

    record = await db.get(ContentItem, cid)
    if not record:
        raise HTTPException(status_code=404, detail="Content record not found")

    await db.delete(record)
    await db.commit()

    # Remove from Qdrant
    try:
        await content_vector_store.delete(content_id)
    except Exception as e:
        logger.warning("vector_delete_failed", content_id=content_id, error=str(e))

    return {"deleted": True, "id": content_id}


# ============================================================================
# HELPERS
# ============================================================================

def _compute_graph_tags(importance_score: int, doc_type: str, analysis) -> list:
    """
    Return graph display tags based on importance score and NLP results.
    These tags control how the node appears in the Graph Explorer.
    """
    tags = []
    if importance_score >= 70:
        tags.append('essential')      # Largest, most prominent nodes
    elif importance_score >= 50:
        tags.append('curated')        # Medium nodes — deliberate reading
    elif importance_score >= 35:
        tags.append('browsed')        # Small nodes — quick visits
    else:
        tags.append('ephemeral')      # Not shown in graph by default

    if doc_type in ('research', 'documentation'):
        tags.append('reference')
    if doc_type == 'code_repo':
        tags.append('code')
    if analysis and analysis.topic and getattr(analysis.topic, 'primary', None):
        tags.append(f"topic:{analysis.topic.primary.lower().replace(' ', '_')}")
    return tags


def _extract_domain(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc
    except Exception:
        return ""


def _title_from_url(url: str) -> str:
    if not url:
        return "Untitled"
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path = parsed.path.strip("/")
        return path.split("/")[-1] or parsed.netloc or "Untitled"
    except Exception:
        return "Untitled"


def _sanitize_text(text: str) -> str:
    """Strip PostgreSQL-illegal null bytes (\x00) from text.

    PostgreSQL UTF8 encoding rejects null bytes — they appear when binary
    files (SQLite DBs, executables, etc.) are accidentally ingested as text.
    """
    if not text:
        return text
    return text.replace("\x00", "")
