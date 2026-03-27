"""
Content Vector Store — Qdrant collection for semantic content search.

Separate from entity vectors (used by qdrant_entity_service.py).
This collection stores extracted page/document content embeddings for
offline-capable semantic search via the content ingestion API.

Collection name: content_vectors
Vector dim: 384 (all-MiniLM-L6-v2)
"""

import uuid
from datetime import datetime
from typing import Optional
import structlog

# Lazy import — sentence-transformers not installed on Render (desktop-only)
try:
    from services.embedding_service import embedding_service
except ImportError:
    embedding_service = None  # type: ignore

logger = structlog.get_logger()

COLLECTION_NAME = "content_vectors"
VECTOR_DIM = 384


class ContentVectorStore:
    """
    Manages the Qdrant 'content_vectors' collection.

    Each vector point represents a chunk of extracted page or document content.
    Payload stored alongside each vector:
        - content_id: str (references the content record in Postgres)
        - user_id:    str
        - title:      str
        - url:        str
        - doc_type:   str  (webpage, pdf, docx, code, etc.)
        - chunk_index: int (for multi-chunk docs)
        - created_at: str (ISO 8601)
        - text_snippet: str (first 500 chars for display)
    """

    def __init__(self):
        self._client = None
        self._initialized = False

    async def _get_client(self):
        """Lazy-initialize Qdrant async client."""
        if self._client:
            return self._client
        try:
            from database.qdrant_client import get_qdrant_client, init_qdrant, client as _global
            # If global client not yet initialized, init it now
            if _global is None:
                await init_qdrant()
            self._client = get_qdrant_client()   # sync getter — no await
            await self._ensure_collection()
            self._initialized = True
        except Exception as e:
            logger.error("qdrant_client_init_failed", error=str(e))
            raise
        return self._client


    async def _ensure_collection(self):
        """Create the content_vectors collection if it doesn't exist."""
        from qdrant_client.models import VectorParams, Distance  # type: ignore
        try:
            client = self._client
            collections = await client.get_collections()
            existing = {c.name for c in collections.collections}
            if COLLECTION_NAME not in existing:
                await client.create_collection(
                    collection_name=COLLECTION_NAME,
                    vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
                )
                logger.info("content_vectors_collection_created")
        except Exception as e:
            logger.error("collection_ensure_failed", error=str(e))

    async def upsert(
        self,
        content_id: str,
        text: str,
        user_id: str = "",
        metadata: Optional[dict] = None,
        chunk_index: int = 0,
    ) -> bool:
        """
        Embed text and upsert into Qdrant.

        Args:
            content_id:  Unique content record identifier
            text:        Text to embed (will be truncated to 512 tokens)
            user_id:     Owning user
            metadata:    Additional payload (title, url, doc_type, etc.)
            chunk_index: For large docs split into chunks

        Returns:
            True on success
        """
        if not text or not text.strip():
            return False

        meta = metadata or {}
        try:
            from qdrant_client.models import PointStruct  # type: ignore

            # Generate embedding
            embedding = embedding_service.generate_embedding(text[:4000])

            point_id = str(uuid.uuid5(
                uuid.NAMESPACE_URL, f"{content_id}:{chunk_index}"
            ))

            payload = {
                "content_id": content_id,
                "user_id": user_id,
                "chunk_index": chunk_index,
                "title": meta.get("title", ""),
                "url": meta.get("url", ""),
                "doc_type": meta.get("doc_type", "webpage"),
                "text_snippet": text[:500],
                "created_at": datetime.utcnow().isoformat(),
                **{k: v for k, v in meta.items()
                   if k not in ("title", "url", "doc_type") and isinstance(v, (str, int, float, bool))},
            }

            client = await self._get_client()
            await client.upsert(
                collection_name=COLLECTION_NAME,
                points=[PointStruct(id=point_id, vector=embedding, payload=payload)],
            )
            logger.debug("content_vector_upserted", content_id=content_id, chunk=chunk_index)
            return True

        except Exception as e:
            logger.error("content_vector_upsert_failed", content_id=content_id, error=str(e))
            return False

    async def upsert_chunks(
        self,
        content_id: str,
        text: str,
        user_id: str = "",
        metadata: Optional[dict] = None,
        chunk_size: int = 1000,
        overlap: int = 100,
    ) -> int:
        """
        Split large text into overlapping chunks and upsert all.

        Returns number of chunks upserted.
        """
        words = text.split()
        chunks = []
        step = chunk_size - overlap
        for i in range(0, len(words), step):
            chunk = " ".join(words[i:i + chunk_size])
            if chunk:
                chunks.append(chunk)

        success = 0
        for idx, chunk in enumerate(chunks):
            ok = await self.upsert(
                content_id=content_id,
                text=chunk,
                user_id=user_id,
                metadata=metadata,
                chunk_index=idx,
            )
            if ok:
                success += 1

        logger.info("chunks_upserted", content_id=content_id, chunks=success)
        return success

    async def search(
        self,
        query: str,
        user_id: str = "",
        limit: int = 10,
        doc_type_filter: Optional[str] = None,
        score_threshold: float = 0.3,
    ) -> list:
        """
        Semantic search over content vectors.

        Args:
            query:           Search query text
            user_id:         Filter by user (empty = no filter)
            limit:           Max results
            doc_type_filter: Filter by doc_type (webpage/pdf/docx/code etc.)
            score_threshold: Min cosine similarity to include

        Returns:
            List of result dicts with score + payload
        """
        try:
            from qdrant_client.models import Filter, FieldCondition, MatchValue  # type: ignore

            query_embedding = embedding_service.generate_embedding(query)

            # Build optional filter
            conditions = []
            if user_id:
                conditions.append(FieldCondition(
                    key="user_id", match=MatchValue(value=user_id)
                ))
            if doc_type_filter:
                conditions.append(FieldCondition(
                    key="doc_type", match=MatchValue(value=doc_type_filter)
                ))

            filt = Filter(must=conditions) if conditions else None

            client = await self._get_client()
            results = await client.search(
                collection_name=COLLECTION_NAME,
                query_vector=query_embedding,
                limit=limit,
                query_filter=filt,
                score_threshold=score_threshold,
            )

            return [
                {
                    "content_id": r.payload.get("content_id", ""),
                    "score": round(r.score, 4),
                    "title": r.payload.get("title", ""),
                    "url": r.payload.get("url", ""),
                    "doc_type": r.payload.get("doc_type", ""),
                    "snippet": r.payload.get("text_snippet", ""),
                    "chunk_index": r.payload.get("chunk_index", 0),
                    "created_at": r.payload.get("created_at", ""),
                }
                for r in results
            ]

        except Exception as e:
            logger.error("content_vector_search_failed", error=str(e))
            return []

    async def delete(self, content_id: str) -> bool:
        """Delete all vectors for a content_id."""
        try:
            from qdrant_client.models import Filter, FieldCondition, MatchValue  # type: ignore
            client = await self._get_client()
            await client.delete(
                collection_name=COLLECTION_NAME,
                points_selector=Filter(
                    must=[FieldCondition(key="content_id", match=MatchValue(value=content_id))]
                ),
            )
            return True
        except Exception as e:
            logger.error("content_vector_delete_failed", content_id=content_id, error=str(e))
            return False

    async def get_stats(self) -> dict:
        """Return collection statistics."""
        try:
            client = await self._get_client()
            info = await client.get_collection(COLLECTION_NAME)
            return {
                "collection": COLLECTION_NAME,
                "vectors_count": info.vectors_count,
                "indexed_vectors": info.indexed_vectors_count,
                "status": str(info.status),
            }
        except Exception as e:
            return {"error": str(e)}


# Global singleton
content_vector_store = ContentVectorStore()
