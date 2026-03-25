"""
RAG (Retrieval-Augmented Generation) Service

Provides context-aware Q&A with citation tracking:
- Vector-based retrieval from knowledge store
- Context augmentation for LLM prompts
- Source citation tracking and formatting
- Smart contextual search with re-ranking
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from collections import defaultdict
import math
import uuid
import structlog

logger = structlog.get_logger()


# ============================================================================
# CITATION DATA MODEL
# ============================================================================

class Citation:
    """Represents a source citation attached to an AI response."""

    __slots__ = ('id', 'source_type', 'source_id', 'title', 'snippet',
                 'relevance_score', 'timestamp', 'url', 'metadata')

    def __init__(
        self,
        source_type: str,
        source_id: str,
        title: str,
        snippet: str,
        relevance_score: float = 0.0,
        timestamp: Optional[str] = None,
        url: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.id = str(uuid.uuid4())
        self.source_type = source_type    # 'activity', 'document', 'note', 'project'
        self.source_id = source_id
        self.title = title
        self.snippet = snippet
        self.relevance_score = relevance_score
        self.timestamp = timestamp
        self.url = url
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'source_type': self.source_type,
            'source_id': self.source_id,
            'title': self.title,
            'snippet': self.snippet,
            'relevance_score': round(self.relevance_score, 4),
            'timestamp': self.timestamp,
            'url': self.url,
            'metadata': self.metadata,
        }

    def format_inline(self, index: int) -> str:
        """Format citation for inline reference, e.g. [1]."""
        return f"[{index}]"

    def format_footnote(self, index: int) -> str:
        """Format citation for footnote display."""
        return f"[{index}] {self.title} — {self.snippet[:100]}"


class RAGService:
    """
    Retrieval-Augmented Generation service.

    Retrieves context from a knowledge store (Qdrant / in-memory),
    augments LLM prompts, and tracks citations.
    """

    # Retrieval config
    DEFAULT_TOP_K = 5
    MIN_RELEVANCE_SCORE = 0.3
    MAX_CONTEXT_TOKENS = 2000
    CONTEXT_SNIPPET_LENGTH = 300

    def __init__(self, knowledge_store: Optional[Dict[str, List[Dict[str, Any]]]] = None):
        """
        Args:
            knowledge_store: Optional in-memory knowledge base.
                             Maps collection names to lists of documents.
                             Each document: {id, title, content, embedding, metadata, timestamp}
        """
        self._store: Dict[str, List[Dict[str, Any]]] = knowledge_store or {}
        self._citation_cache: Dict[str, List[Citation]] = {}  # response_id -> citations

    # ========================================================================
    # KNOWLEDGE STORE MANAGEMENT
    # ========================================================================

    def add_documents(
        self,
        collection: str,
        documents: List[Dict[str, Any]],
    ) -> int:
        """Add documents to the knowledge store."""
        if collection not in self._store:
            self._store[collection] = []

        added = 0
        for doc in documents:
            if 'id' not in doc:
                doc['id'] = str(uuid.uuid4())
            if 'timestamp' not in doc:
                doc['timestamp'] = datetime.now(tz=None).isoformat()
            self._store[collection].append(doc)
            added += 1

        logger.info("documents_added", collection=collection, count=added)
        return added

    def get_collections(self) -> List[str]:
        """List all collections in the knowledge store."""
        return list(self._store.keys())

    def get_collection_stats(self, collection: str) -> Dict[str, Any]:
        """Get statistics for a collection."""
        docs = self._store.get(collection, [])
        return {
            'collection': collection,
            'document_count': len(docs),
            'has_embeddings': any('embedding' in d for d in docs),
        }

    # ========================================================================
    # RETRIEVAL
    # ========================================================================

    def retrieve(
        self,
        query: str,
        collections: Optional[List[str]] = None,
        top_k: int = DEFAULT_TOP_K,
        min_score: float = MIN_RELEVANCE_SCORE,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve relevant documents for a query.

        Uses keyword matching + embedding similarity when available.

        Args:
            query: User query string
            collections: Collections to search (all if None)
            top_k: Number of results to return
            min_score: Minimum relevance score threshold
            filters: Additional filters (type, date range, etc.)

        Returns:
            Ranked list of relevant documents with scores
        """
        target_collections = collections or list(self._store.keys())
        all_results = []

        query_terms = set(query.lower().split())

        for col_name in target_collections:
            docs = self._store.get(col_name, [])

            for doc in docs:
                # Apply filters
                if filters and not self._matches_filters(doc, filters):
                    continue

                # Compute relevance score
                score = self._compute_relevance(query_terms, doc)

                if score >= min_score:
                    all_results.append({
                        'document': doc,
                        'collection': col_name,
                        'relevance_score': score,
                    })

        # Sort by relevance descending
        all_results.sort(key=lambda x: x['relevance_score'], reverse=True)

        return all_results[:top_k]

    def semantic_search(
        self,
        query_embedding: List[float],
        collections: Optional[List[str]] = None,
        top_k: int = DEFAULT_TOP_K,
        min_score: float = MIN_RELEVANCE_SCORE,
    ) -> List[Dict[str, Any]]:
        """
        Semantic search using query embedding.

        Args:
            query_embedding: Vector representation of the query
            collections: Collections to search
            top_k: Number of results
            min_score: Minimum cosine similarity

        Returns:
            Ranked results with similarity scores
        """
        target_collections = collections or list(self._store.keys())
        results = []

        for col_name in target_collections:
            docs = self._store.get(col_name, [])

            for doc in docs:
                if 'embedding' not in doc:
                    continue

                similarity = self._cosine_similarity(query_embedding, doc['embedding'])
                if similarity >= min_score:
                    results.append({
                        'document': doc,
                        'collection': col_name,
                        'relevance_score': similarity,
                    })

        results.sort(key=lambda x: x['relevance_score'], reverse=True)
        return results[:top_k]

    # ========================================================================
    # CONTEXT AUGMENTATION
    # ========================================================================

    def build_augmented_prompt(
        self,
        query: str,
        retrieved_docs: List[Dict[str, Any]],
        max_context_length: int = MAX_CONTEXT_TOKENS,
    ) -> Dict[str, Any]:
        """
        Build an augmented prompt with retrieved context.

        Returns:
            Dict with 'context_text' for prompt injection and 'citations' list
        """
        context_parts = []
        citations = []
        total_length = 0

        for i, result in enumerate(retrieved_docs):
            doc = result['document']
            content = doc.get('content', '')
            title = doc.get('title', f'Source {i + 1}')

            # Truncate content to snippet length
            snippet = content[:self.CONTEXT_SNIPPET_LENGTH]
            if len(content) > self.CONTEXT_SNIPPET_LENGTH:
                snippet += '...'

            # Check token budget
            if total_length + len(snippet) > max_context_length * 4:  # ~4 chars per token
                break

            context_parts.append(f"[Source {i + 1}: {title}]\n{snippet}\n")
            total_length += len(snippet)

            # Create citation
            citation = Citation(
                source_type=doc.get('type', 'document'),
                source_id=doc.get('id', ''),
                title=title,
                snippet=snippet[:150],
                relevance_score=result.get('relevance_score', 0.0),
                timestamp=doc.get('timestamp'),
                url=doc.get('url'),
                metadata=doc.get('metadata', {}),
            )
            citations.append(citation)

        context_text = '\n'.join(context_parts)

        augmented_instruction = (
            "Use the following context to answer the user's question. "
            "Cite sources using [1], [2], etc. when referencing information.\n\n"
            f"Context:\n{context_text}\n\n"
            f"Question: {query}"
        )

        return {
            'augmented_prompt': augmented_instruction,
            'context_text': context_text,
            'citations': [c.to_dict() for c in citations],
            'num_sources': len(citations),
            'context_length': total_length,
        }

    def format_response_with_citations(
        self,
        response_text: str,
        citations: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Format an AI response with citation footnotes.

        Args:
            response_text: Raw AI response (may contain [1], [2] markers)
            citations: List of citation dicts

        Returns:
            Formatted response with footnotes section
        """
        # Build footnotes
        footnotes = []
        for i, citation in enumerate(citations, 1):
            footnote = f"[{i}] {citation['title']}"
            if citation.get('timestamp'):
                footnote += f" ({citation['timestamp'][:10]})"
            footnotes.append(footnote)

        formatted = response_text
        if footnotes:
            formatted += '\n\n---\n**Sources:**\n' + '\n'.join(footnotes)

        return {
            'response': formatted,
            'raw_response': response_text,
            'citations': citations,
            'has_citations': len(citations) > 0,
        }

    # ========================================================================
    # SMART SEARCH
    # ========================================================================

    def smart_search(
        self,
        query: str,
        collections: Optional[List[str]] = None,
        top_k: int = 10,
        rerank: bool = True,
    ) -> Dict[str, Any]:
        """
        Smart contextual search: retrieve → re-rank → format.

        Args:
            query: Search query
            collections: Collections to search
            top_k: Number of results
            rerank: Whether to apply re-ranking

        Returns:
            Search results with relevance scores and snippets
        """
        # Phase 1: Broad retrieval
        results = self.retrieve(
            query=query,
            collections=collections,
            top_k=top_k * 2,  # Over-retrieve for re-ranking
            min_score=0.1,
        )

        # Phase 2: Re-rank if enabled
        if rerank and len(results) > 1:
            results = self._rerank(query, results)

        # Phase 3: Format results
        final_results = results[:top_k]

        formatted = []
        for r in final_results:
            doc = r['document']
            content = doc.get('content', '')
            formatted.append({
                'id': doc.get('id', ''),
                'title': doc.get('title', 'Untitled'),
                'snippet': self._extract_best_snippet(query, content),
                'relevance_score': round(r['relevance_score'], 4),
                'collection': r['collection'],
                'type': doc.get('type', 'document'),
                'timestamp': doc.get('timestamp'),
            })

        return {
            'query': query,
            'results': formatted,
            'total_results': len(formatted),
            'searched_collections': collections or list(self._store.keys()),
        }

    # ========================================================================
    # INTERNAL METHODS
    # ========================================================================

    def _compute_relevance(
        self,
        query_terms: set,
        doc: Dict[str, Any],
    ) -> float:
        """Compute keyword-based relevance score (TF-IDF-like)."""
        content = doc.get('content', '').lower()
        title = doc.get('title', '').lower()

        if not content and not title:
            return 0.0

        content_words = set(content.split())
        title_words = set(title.split())

        # Title matches are worth more
        title_overlap = len(query_terms & title_words)
        content_overlap = len(query_terms & content_words)

        if not query_terms:
            return 0.0

        title_score = title_overlap / len(query_terms) * 0.6
        content_score = content_overlap / len(query_terms) * 0.4

        score = title_score + content_score

        # Boost for exact phrase match
        query_str = ' '.join(query_terms)
        if query_str in title:
            score += 0.3
        if query_str in content:
            score += 0.1

        return min(score, 1.0)

    def _cosine_similarity(self, vec_a: List[float], vec_b: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if len(vec_a) != len(vec_b) or not vec_a:
            return 0.0

        dot = sum(a * b for a, b in zip(vec_a, vec_b))
        mag_a = math.sqrt(sum(a * a for a in vec_a))
        mag_b = math.sqrt(sum(b * b for b in vec_b))

        if mag_a == 0 or mag_b == 0:
            return 0.0

        return dot / (mag_a * mag_b)

    def _matches_filters(
        self,
        doc: Dict[str, Any],
        filters: Dict[str, Any],
    ) -> bool:
        """Check if a document matches the given filters."""
        for key, value in filters.items():
            if key == 'type' and doc.get('type') != value:
                return False
            if key == 'after' and doc.get('timestamp', '') < value:
                return False
            if key == 'before' and doc.get('timestamp', '') > value:
                return False
            if key == 'collection' and doc.get('collection') != value:
                return False
        return True

    def _rerank(
        self,
        query: str,
        results: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Re-rank results based on additional signals.

        Boosts: recency, exact matches, title matches.
        """
        query_lower = query.lower()

        for result in results:
            doc = result['document']
            score = result['relevance_score']

            # Recency boost (more recent = higher)
            ts = doc.get('timestamp', '')
            if ts:
                try:
                    doc_time = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    days_old = (datetime.now(tz=None) - doc_time.replace(tzinfo=None)).days
                    recency_boost = max(0, 0.1 * (1 - days_old / 365))
                    score += recency_boost
                except (ValueError, TypeError):
                    pass

            # Exact title match boost
            title = doc.get('title', '').lower()
            if query_lower in title:
                score += 0.15

            result['relevance_score'] = min(score, 1.0)

        results.sort(key=lambda x: x['relevance_score'], reverse=True)
        return results

    def _extract_best_snippet(
        self,
        query: str,
        content: str,
        snippet_length: int = 200,
    ) -> str:
        """Extract the most relevant snippet from content."""
        if not content:
            return ''

        query_terms = query.lower().split()
        content_lower = content.lower()

        # Find the position of the first query term
        best_pos = 0
        for term in query_terms:
            pos = content_lower.find(term)
            if pos >= 0:
                best_pos = max(0, pos - 50)
                break

        snippet = content[best_pos:best_pos + snippet_length]
        if best_pos > 0:
            snippet = '...' + snippet
        if best_pos + snippet_length < len(content):
            snippet += '...'

        return snippet


# Global instance
rag_service = RAGService()
