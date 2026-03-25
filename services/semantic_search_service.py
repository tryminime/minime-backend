"""
Semantic Search Service — Advanced vector search with hybrid scoring.

Provides:
- Multi-field semantic search across activities, entities, documents
- Hybrid search: vector similarity + BM25 keyword scoring
- Search result re-ranking with diversity sampling
- Query expansion via embedding neighbors
- Collection/index management (create, delete, optimize, payload indexing)
- Faceted search with metadata filters
"""

import math
import re
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import structlog

logger = structlog.get_logger()


class SemanticSearchService:
    """
    Advanced semantic search over Qdrant vector collections.

    Combines dense vector similarity with sparse keyword scoring (BM25)
    for hybrid retrieval.  Supports re-ranking, diversity sampling,
    query expansion, faceted filtering, and index lifecycle management.
    """

    # ── Collection registry ──────────────────────────────────────────
    COLLECTIONS = {
        "activities": {"dim": 384, "distance": "Cosine"},
        "entities": {"dim": 384, "distance": "Cosine"},
        "documents": {"dim": 384, "distance": "Cosine"},
        "conversations": {"dim": 384, "distance": "Cosine"},
    }

    # BM25 tuning
    BM25_K1 = 1.2
    BM25_B = 0.75
    AVG_DOC_LEN = 50  # default, recalculated per corpus

    def __init__(self):
        self._stores: Dict[str, List[Dict[str, Any]]] = {
            name: [] for name in self.COLLECTIONS
        }
        self._indexes: Dict[str, Dict[str, Any]] = {}
        self._payload_indexes: Dict[str, List[str]] = {
            name: [] for name in self.COLLECTIONS
        }
        self._stats: Dict[str, Any] = {
            "total_searches": 0,
            "total_upserts": 0,
            "total_deletes": 0,
            "hybrid_searches": 0,
        }

    # ── Index / collection management ────────────────────────────────

    def create_collection(
        self,
        name: str,
        dimension: int = 384,
        distance: str = "Cosine",
    ) -> Dict[str, Any]:
        """Create a new vector collection."""
        if name in self._stores:
            return {"success": False, "error": f"Collection '{name}' already exists"}

        self.COLLECTIONS[name] = {"dim": dimension, "distance": distance}
        self._stores[name] = []
        self._payload_indexes[name] = []
        logger.info("collection_created", name=name, dim=dimension)
        return {"success": True, "collection": name, "dimension": dimension}

    def delete_collection(self, name: str) -> Dict[str, Any]:
        """Delete an existing collection and all its vectors."""
        if name not in self._stores:
            return {"success": False, "error": f"Collection '{name}' not found"}

        count = len(self._stores[name])
        del self._stores[name]
        del self._payload_indexes[name]
        self.COLLECTIONS.pop(name, None)
        logger.info("collection_deleted", name=name, vectors_removed=count)
        return {"success": True, "vectors_removed": count}

    def optimize_collection(self, name: str) -> Dict[str, Any]:
        """
        Optimize a collection: deduplicate, rebuild internal IDF table.
        """
        if name not in self._stores:
            return {"success": False, "error": f"Collection '{name}' not found"}

        before = len(self._stores[name])
        seen_ids: set = set()
        deduped: list = []
        for pt in self._stores[name]:
            if pt["id"] not in seen_ids:
                seen_ids.add(pt["id"])
                deduped.append(pt)
        self._stores[name] = deduped
        removed = before - len(deduped)
        logger.info("collection_optimized", name=name, duplicates_removed=removed)
        return {
            "success": True,
            "collection": name,
            "duplicates_removed": removed,
            "total_vectors": len(deduped),
        }

    def create_payload_index(
        self, collection: str, field: str
    ) -> Dict[str, Any]:
        """Create a payload index on a metadata field for fast filtering."""
        if collection not in self._payload_indexes:
            return {"success": False, "error": f"Collection '{collection}' not found"}
        if field in self._payload_indexes[collection]:
            return {"success": False, "error": f"Index on '{field}' already exists"}

        self._payload_indexes[collection].append(field)
        logger.info("payload_index_created", collection=collection, field=field)
        return {"success": True, "collection": collection, "indexed_field": field}

    def list_collections(self) -> List[Dict[str, Any]]:
        """List all collections with metadata."""
        return [
            {
                "name": name,
                "dimension": cfg["dim"],
                "distance": cfg["distance"],
                "vectors_count": len(self._stores.get(name, [])),
                "payload_indexes": self._payload_indexes.get(name, []),
            }
            for name, cfg in self.COLLECTIONS.items()
        ]

    def get_collection_info(self, name: str) -> Dict[str, Any]:
        """Detailed info about a single collection."""
        if name not in self._stores:
            return {"error": f"Collection '{name}' not found"}
        vectors = self._stores[name]
        return {
            "name": name,
            "dimension": self.COLLECTIONS[name]["dim"],
            "distance": self.COLLECTIONS[name]["distance"],
            "vectors_count": len(vectors),
            "payload_indexes": self._payload_indexes.get(name, []),
            "storage_bytes_estimate": len(vectors) * self.COLLECTIONS[name]["dim"] * 4,
        }

    # ── CRUD ─────────────────────────────────────────────────────────

    def upsert(
        self,
        collection: str,
        point_id: str,
        vector: List[float],
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Insert or update a vector point."""
        if collection not in self._stores:
            return {"success": False, "error": f"Collection '{collection}' not found"}

        store = self._stores[collection]
        for i, pt in enumerate(store):
            if pt["id"] == point_id:
                store[i] = {"id": point_id, "vector": vector, "payload": payload}
                self._stats["total_upserts"] += 1
                return {"success": True, "action": "updated", "id": point_id}

        store.append({"id": point_id, "vector": vector, "payload": payload})
        self._stats["total_upserts"] += 1
        return {"success": True, "action": "inserted", "id": point_id}

    def upsert_batch(
        self,
        collection: str,
        points: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Batch upsert multiple points."""
        if collection not in self._stores:
            return {"success": False, "error": f"Collection '{collection}' not found"}

        inserted = 0
        updated = 0
        for pt in points:
            res = self.upsert(collection, pt["id"], pt["vector"], pt["payload"])
            if res.get("action") == "inserted":
                inserted += 1
            else:
                updated += 1
        return {"success": True, "inserted": inserted, "updated": updated}

    def delete(self, collection: str, point_id: str) -> Dict[str, Any]:
        """Delete a point by ID."""
        if collection not in self._stores:
            return {"success": False, "error": f"Collection '{collection}' not found"}
        before = len(self._stores[collection])
        self._stores[collection] = [
            p for p in self._stores[collection] if p["id"] != point_id
        ]
        removed = before - len(self._stores[collection])
        self._stats["total_deletes"] += removed
        return {"success": True, "removed": removed}

    # ── Vector math helpers ──────────────────────────────────────────

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(x * x for x in b))
        if na == 0 or nb == 0:
            return 0.0
        return dot / (na * nb)

    @staticmethod
    def _euclidean_distance(a: List[float], b: List[float]) -> float:
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

    # ── BM25 helpers ─────────────────────────────────────────────────

    def _tokenize(self, text: str) -> List[str]:
        return re.findall(r"\w+", text.lower())

    def _compute_idf(
        self, term: str, documents: List[List[str]]
    ) -> float:
        n = len(documents)
        df = sum(1 for doc in documents if term in doc)
        if df == 0:
            return 0.0
        return math.log((n - df + 0.5) / (df + 0.5) + 1)

    def _bm25_score(
        self,
        query_tokens: List[str],
        doc_tokens: List[str],
        idf_map: Dict[str, float],
        avg_dl: float,
    ) -> float:
        score = 0.0
        dl = len(doc_tokens)
        tf_map = Counter(doc_tokens)
        for qt in query_tokens:
            tf = tf_map.get(qt, 0)
            idf = idf_map.get(qt, 0.0)
            numerator = tf * (self.BM25_K1 + 1)
            denominator = tf + self.BM25_K1 * (
                1 - self.BM25_B + self.BM25_B * (dl / max(avg_dl, 1))
            )
            score += idf * (numerator / max(denominator, 1e-9))
        return score

    # ── Core search ──────────────────────────────────────────────────

    def semantic_search(
        self,
        collection: str,
        query_vector: List[float],
        limit: int = 10,
        score_threshold: float = 0.0,
        filters: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Pure vector similarity search with optional metadata filters.
        """
        if collection not in self._stores:
            return []
        self._stats["total_searches"] += 1

        results: List[Tuple[float, Dict]] = []
        for pt in self._stores[collection]:
            # user_id filter
            if user_id and pt["payload"].get("user_id") != user_id:
                continue
            # metadata filters
            if filters and not self._matches_filters(pt["payload"], filters):
                continue
            score = self._cosine_similarity(query_vector, pt["vector"])
            if score >= score_threshold:
                results.append((score, pt))

        results.sort(key=lambda x: x[0], reverse=True)
        return [
            {"id": pt["id"], "score": s, "payload": pt["payload"]}
            for s, pt in results[:limit]
        ]

    def keyword_search(
        self,
        collection: str,
        query: str,
        text_field: str = "text",
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """BM25 keyword search over a text payload field."""
        if collection not in self._stores:
            return []

        query_tokens = self._tokenize(query)
        if not query_tokens:
            return []

        candidates = self._stores[collection]
        if user_id:
            candidates = [p for p in candidates if p["payload"].get("user_id") == user_id]
        if filters:
            candidates = [p for p in candidates if self._matches_filters(p["payload"], filters)]

        # build token lists per doc
        doc_token_lists = [
            self._tokenize(str(pt["payload"].get(text_field, "")))
            for pt in candidates
        ]
        avg_dl = (
            sum(len(tl) for tl in doc_token_lists) / max(len(doc_token_lists), 1)
        )
        idf_map = {
            t: self._compute_idf(t, doc_token_lists) for t in set(query_tokens)
        }

        scored: List[Tuple[float, Dict]] = []
        for pt, dtokens in zip(candidates, doc_token_lists):
            s = self._bm25_score(query_tokens, dtokens, idf_map, avg_dl)
            if s > 0:
                scored.append((s, pt))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [
            {"id": pt["id"], "score": s, "payload": pt["payload"]}
            for s, pt in scored[:limit]
        ]

    def hybrid_search(
        self,
        collection: str,
        query_vector: List[float],
        query_text: str,
        text_field: str = "text",
        limit: int = 10,
        alpha: float = 0.7,
        score_threshold: float = 0.0,
        filters: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Hybrid search: combines vector similarity (weight=alpha) with
        BM25 keyword score (weight=1-alpha) via Reciprocal Rank Fusion.
        """
        self._stats["hybrid_searches"] += 1

        vec_results = self.semantic_search(
            collection, query_vector, limit=limit * 3,
            score_threshold=0.0, filters=filters, user_id=user_id,
        )
        kw_results = self.keyword_search(
            collection, query_text, text_field, limit=limit * 3,
            filters=filters, user_id=user_id,
        )

        # Reciprocal Rank Fusion
        rrf_scores: Dict[str, float] = {}
        rrf_payloads: Dict[str, Dict] = {}
        k = 60  # RRF constant

        for rank, r in enumerate(vec_results):
            rid = r["id"]
            rrf_scores[rid] = rrf_scores.get(rid, 0) + alpha / (k + rank + 1)
            rrf_payloads[rid] = r["payload"]

        for rank, r in enumerate(kw_results):
            rid = r["id"]
            rrf_scores[rid] = rrf_scores.get(rid, 0) + (1 - alpha) / (k + rank + 1)
            rrf_payloads[rid] = r["payload"]

        fused = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
        out = []
        for rid, score in fused[:limit]:
            if score >= score_threshold:
                out.append({"id": rid, "score": score, "payload": rrf_payloads[rid]})
        return out

    # ── Re-ranking & diversity ───────────────────────────────────────

    def rerank(
        self,
        results: List[Dict[str, Any]],
        query_vector: List[float],
        boost_fields: Optional[Dict[str, float]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Re-rank results using a weighted combination of vector score
        and optional payload field boosts.

        boost_fields example: {"priority": 0.2, "recency": 0.1}
        """
        boost_fields = boost_fields or {}
        scored = []
        for r in results:
            base = r.get("score", 0.0)
            bonus = 0.0
            for field, weight in boost_fields.items():
                val = r["payload"].get(field)
                if isinstance(val, (int, float)):
                    bonus += weight * val
            scored.append({**r, "score": base + bonus})
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored

    def diversity_sample(
        self,
        results: List[Dict[str, Any]],
        limit: int = 10,
        diversity_field: str = "type",
    ) -> List[Dict[str, Any]]:
        """
        MMR-style diversity sampling.  Interleaves results so that
        adjacent items differ on *diversity_field* when possible.
        """
        if not results:
            return []

        selected: List[Dict[str, Any]] = []
        remaining = list(results)

        while remaining and len(selected) < limit:
            if not selected:
                selected.append(remaining.pop(0))
                continue
            last_type = selected[-1]["payload"].get(diversity_field)
            # prefer a different type
            idx = next(
                (
                    i
                    for i, r in enumerate(remaining)
                    if r["payload"].get(diversity_field) != last_type
                ),
                0,
            )
            selected.append(remaining.pop(idx))
        return selected

    # ── Query expansion ──────────────────────────────────────────────

    def expand_query(
        self,
        collection: str,
        query_vector: List[float],
        expansion_factor: int = 3,
        user_id: Optional[str] = None,
    ) -> List[float]:
        """
        Expand query by averaging the query vector with its top-N
        nearest neighbors (pseudo-relevance feedback / Rocchio).
        """
        neighbors = self.semantic_search(
            collection, query_vector, limit=expansion_factor, user_id=user_id,
        )
        if not neighbors:
            return query_vector

        dim = len(query_vector)
        centroid = list(query_vector)
        for nb in neighbors:
            vec = self._get_vector(collection, nb["id"])
            if vec:
                for i in range(dim):
                    centroid[i] += vec[i]
        n = 1 + len(neighbors)
        return [c / n for c in centroid]

    def _get_vector(self, collection: str, point_id: str) -> Optional[List[float]]:
        for pt in self._stores.get(collection, []):
            if pt["id"] == point_id:
                return pt["vector"]
        return None

    # ── Faceted search ───────────────────────────────────────────────

    def faceted_search(
        self,
        collection: str,
        query_vector: List[float],
        facet_fields: List[str],
        limit: int = 10,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search + compute facet counts for the result set.
        """
        results = self.semantic_search(
            collection, query_vector, limit=limit * 5, user_id=user_id,
        )
        facets: Dict[str, Dict[str, int]] = {f: {} for f in facet_fields}
        for r in results:
            for field in facet_fields:
                val = str(r["payload"].get(field, "unknown"))
                facets[field][val] = facets[field].get(val, 0) + 1

        return {"results": results[:limit], "facets": facets, "total": len(results)}

    # ── Multi-collection search ──────────────────────────────────────

    def search_across_collections(
        self,
        query_vector: List[float],
        collections: Optional[List[str]] = None,
        limit: int = 10,
        user_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Search across multiple collections, merge and rank."""
        targets = collections or list(self._stores.keys())
        merged: List[Dict[str, Any]] = []
        for coll in targets:
            hits = self.semantic_search(
                coll, query_vector, limit=limit, user_id=user_id,
            )
            for h in hits:
                h["collection"] = coll
            merged.extend(hits)
        merged.sort(key=lambda x: x["score"], reverse=True)
        return merged[:limit]

    # ── Filter helpers ───────────────────────────────────────────────

    @staticmethod
    def _matches_filters(payload: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        for key, value in filters.items():
            pval = payload.get(key)
            if isinstance(value, list):
                if pval not in value:
                    return False
            elif isinstance(value, dict):
                # range filter: {"gte": 5, "lte": 10}
                if "gte" in value and (pval is None or pval < value["gte"]):
                    return False
                if "lte" in value and (pval is None or pval > value["lte"]):
                    return False
                if "gt" in value and (pval is None or pval <= value["gt"]):
                    return False
                if "lt" in value and (pval is None or pval >= value["lt"]):
                    return False
            else:
                if pval != value:
                    return False
        return True

    # ── Stats ────────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Return service-level statistics."""
        total_vectors = sum(len(v) for v in self._stores.values())
        return {
            **self._stats,
            "total_vectors": total_vectors,
            "collections_count": len(self._stores),
        }
