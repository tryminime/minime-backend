"""
Documents API — upload and extract content from PDF/Office files.

Now **persists to PostgreSQL** via ContentItem model.

Endpoints:
  POST /api/v1/documents/extract  — upload file, extract text + run NLP + persist
  GET  /api/v1/documents/{id}     — retrieve stored document content
  GET  /api/v1/documents/         — list documents (from DB)
"""

import uuid as _uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from services.document_extractor import document_extractor
from services.code_extractor import code_extractor
from services.content_pipeline import content_pipeline
from services.content_vector_store import content_vector_store
from database.postgres import get_db
from models import ContentItem

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/documents", tags=["Documents"])
security = HTTPBearer(auto_error=False)

SUPPORTED_CONTENT_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/msword",
    "application/vnd.ms-excel",
    "application/vnd.ms-powerpoint",
    "text/plain",
    "text/x-python",
    "application/javascript",
    "text/javascript",
}

CODE_CONTENT_TYPES = {
    "text/x-python": ".py",
    "application/javascript": ".js",
    "text/javascript": ".js",
    "text/x-rust": ".rs",
    "text/x-go": ".go",
    "text/x-java": ".java",
    "text/x-typescript": ".ts",
}


def _get_user_id(credentials: Optional[HTTPAuthorizationCredentials]) -> str:
    """Extract user_id from JWT if present."""
    if not credentials:
        return ""
    try:
        from auth.jwt_handler import decode_token, verify_token_type
        payload = decode_token(credentials.credentials)
        if payload and verify_token_type(payload, "access"):
            return payload.get("sub", "")
    except Exception:
        pass
    return ""


class DocumentResponse(BaseModel):
    id: str
    filename: str
    doc_type: str
    title: str
    page_count: int
    word_count: int
    reading_time_seconds: int
    keyphrases: list
    entities: list
    topic: Optional[dict] = None
    language: str
    section_count: int
    table_count: int
    code_structure: Optional[dict] = None
    text_preview: str
    created_at: str


@router.post("/extract", response_model=DocumentResponse)
async def extract_document(
    file: UploadFile = File(...),
    user_id: str = Form(default=""),
    store_vectors: bool = Form(default=True),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload a document and extract its text content + NLP enrichment.
    Now also persists to content_items table for permanent storage.
    """
    # Use JWT user_id if available, fallback to form field
    auth_user_id = _get_user_id(credentials)
    effective_user_id = auth_user_id or user_id

    doc_id = _uuid.uuid4()
    created_at = datetime.now(timezone.utc).isoformat()

    content_type = file.content_type or ""
    filename = file.filename or "upload"

    # Fallback: detect MIME from extension
    if not content_type or content_type in ("application/octet-stream", ""):
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        content_type = {
            "pdf": "application/pdf",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "doc": "application/msword",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xls": "application/vnd.ms-excel",
            "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "ppt": "application/vnd.ms-powerpoint",
            "txt": "text/plain",
            "py": "text/x-python",
            "js": "application/javascript",
            "ts": "text/x-typescript",
            "rs": "text/x-rust",
        }.get(ext, "text/plain")

    # Read file bytes
    try:
        data = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read file: {e}")

    if not data:
        raise HTTPException(status_code=400, detail="Empty file uploaded")

    # Detect if this is a code file
    is_code = content_type in CODE_CONTENT_TYPES
    code_structure = None
    section_count = 0

    if is_code:
        lang_hint = CODE_CONTENT_TYPES.get(content_type, "")
        try:
            code_text = data.decode("utf-8", errors="ignore")
        except Exception:
            code_text = ""

        struct = code_extractor.extract(code_text, language_hint=lang_hint)
        code_structure = struct.to_dict()
        extracted_text = struct.to_searchable_text() or code_text
        title = struct.language + " code: " + filename
        page_count = 1
        table_count = 0

    elif content_type == "text/plain":
        try:
            extracted_text = data.decode("utf-8", errors="ignore")
        except Exception:
            extracted_text = ""
        title = filename
        page_count = 1
        table_count = 0

    else:
        doc_content = document_extractor.extract(
            source=data,
            mime_type=content_type,
            filename=filename,
        )

        if doc_content.error:
            raise HTTPException(status_code=422, detail=doc_content.error)

        extracted_text = doc_content.full_text
        title = doc_content.title or filename
        page_count = doc_content.page_count
        table_count = len(doc_content.tables)
        section_count = len(doc_content.sections)

    if not extracted_text or not extracted_text.strip():
        raise HTTPException(status_code=422, detail="No text could be extracted from the document")

    # Run NLP pipeline
    context = {"doc_type": content_type, "filename": filename}
    analysis = content_pipeline.process(extracted_text, context=context)

    entities_list = [
        {"text": e["text"], "label": e["label"]}
        for e in analysis.entities[:15]
    ]
    topic_dict = analysis.topic.__dict__ if analysis.topic else None

    # Detect simple doc_type from content_type
    simple_doc_type = content_type.split("/")[-1]
    if "pdf" in simple_doc_type:
        simple_doc_type = "pdf"
    elif "wordprocessing" in simple_doc_type or "msword" in simple_doc_type:
        simple_doc_type = "docx"
    elif "spreadsheet" in simple_doc_type or "ms-excel" in simple_doc_type:
        simple_doc_type = "xlsx"
    elif "presentation" in simple_doc_type or "ms-powerpoint" in simple_doc_type:
        simple_doc_type = "pptx"
    elif "python" in simple_doc_type or "javascript" in simple_doc_type or "typescript" in simple_doc_type:
        simple_doc_type = "code"
    elif "plain" in simple_doc_type:
        simple_doc_type = "txt"

    # ── Persist to content_items table ─────────────────────────────────
    if effective_user_id:
        try:
            db_item = ContentItem(
                id=doc_id,
                user_id=_uuid.UUID(effective_user_id),
                url="",
                title=title,
                doc_type=simple_doc_type,
                full_text=extracted_text,
                text_snippet=extracted_text[:500].strip(),
                word_count=analysis.word_count,
                reading_time_seconds=analysis.reading_time_seconds,
                keyphrases=analysis.keyphrases,
                entities=entities_list,
                topic=topic_dict,
                language=analysis.language,
                complexity=analysis.complexity,
                content_metadata={"filename": filename, "page_count": page_count},
            )
            db.add(db_item)
            await db.commit()
            await db.refresh(db_item)
            logger.info("document_persisted_to_db", id=str(doc_id), title=title)
        except Exception as e:
            logger.error("document_db_persist_failed", error=str(e))
            # Don't fail the request — still return the extraction result

    # Embed in Qdrant
    if store_vectors and extracted_text:
        try:
            await content_vector_store.upsert_chunks(
                content_id=str(doc_id),
                text=extracted_text,
                user_id=effective_user_id,
                metadata={
                    "title": title,
                    "doc_type": simple_doc_type,
                    "filename": filename,
                },
            )
        except Exception as e:
            logger.warning("doc_vector_store_failed", error=str(e))

    record = {
        "id": str(doc_id),
        "filename": filename,
        "doc_type": simple_doc_type,
        "title": title,
        "page_count": page_count,
        "word_count": analysis.word_count,
        "reading_time_seconds": analysis.reading_time_seconds,
        "keyphrases": analysis.keyphrases,
        "entities": entities_list,
        "topic": topic_dict,
        "language": analysis.language,
        "section_count": section_count,
        "table_count": table_count,
        "code_structure": code_structure,
        "text_preview": extracted_text[:500].strip(),
        "created_at": created_at,
    }

    logger.info(
        "document_extracted",
        id=str(doc_id),
        filename=filename,
        words=analysis.word_count,
        pages=page_count,
    )

    return DocumentResponse(**record)


@router.get("/{doc_id}")
async def get_document(
    doc_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Retrieve a previously extracted document from DB."""
    try:
        cid = _uuid.UUID(doc_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID")

    record = await db.get(ContentItem, cid)
    if not record:
        raise HTTPException(status_code=404, detail="Document not found")

    return record.to_dict()


@router.get("/")
async def list_documents(
    limit: int = 20,
    offset: int = 0,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """List all extracted documents from DB, newest first."""
    user_id = _get_user_id(credentials)

    stmt = select(ContentItem)
    if user_id:
        stmt = stmt.where(ContentItem.user_id == _uuid.UUID(user_id))
    stmt = stmt.order_by(ContentItem.created_at.desc()).offset(offset).limit(limit)

    result = await db.execute(stmt)
    items = result.scalars().all()

    count_stmt = select(func.count(ContentItem.id))
    if user_id:
        count_stmt = count_stmt.where(ContentItem.user_id == _uuid.UUID(user_id))
    total = (await db.execute(count_stmt)).scalar() or 0

    return {
        "items": [
            {
                "id": str(d.id),
                "title": d.title,
                "url": d.url or "",
                "doc_type": d.doc_type,
                "word_count": d.word_count or 0,
                "keyphrases": (d.keyphrases or [])[:5],
                "topic": d.topic,
                "text_snippet": d.text_snippet or "",
                "created_at": d.created_at.isoformat() if d.created_at else "",
            }
            for d in items
        ],
        "total": total,
    }
