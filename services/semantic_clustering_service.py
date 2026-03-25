"""
Semantic Similarity Clustering Service.

Groups related entities into semantic clusters using Qdrant embeddings
and DBSCAN clustering on cosine distance.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Dict, List, Any, Optional
from uuid import UUID

import structlog
import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from models import Entity

logger = structlog.get_logger()


def _cosine_distance_matrix(vectors: np.ndarray) -> np.ndarray:
    """Compute pairwise cosine distance matrix."""
    # Normalize vectors
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1  # avoid division by zero
    normalized = vectors / norms
    # Cosine similarity → distance
    similarity = normalized @ normalized.T
    return 1.0 - similarity


def _dbscan(distance_matrix: np.ndarray, eps: float = 0.35, min_samples: int = 2) -> List[int]:
    """
    Simple DBSCAN implementation on precomputed distance matrix.
    Returns cluster labels (-1 = noise).
    """
    n = len(distance_matrix)
    labels = [-1] * n
    cluster_id = 0
    visited = [False] * n

    def region_query(p: int) -> List[int]:
        return [i for i in range(n) if distance_matrix[p][i] <= eps]

    def expand_cluster(p: int, neighbors: List[int], cid: int):
        labels[p] = cid
        queue = list(neighbors)
        while queue:
            q = queue.pop(0)
            if not visited[q]:
                visited[q] = True
                q_neighbors = region_query(q)
                if len(q_neighbors) >= min_samples:
                    queue.extend(q_neighbors)
            if labels[q] == -1:
                labels[q] = cid

    for i in range(n):
        if visited[i]:
            continue
        visited[i] = True
        neighbors = region_query(i)
        if len(neighbors) < min_samples:
            labels[i] = -1  # noise
        else:
            expand_cluster(i, neighbors, cluster_id)
            cluster_id += 1

    return labels


class SemanticClusteringService:
    """Groups user entities into semantic clusters using embeddings."""

    async def get_clusters(
        self, user_id: UUID, db: AsyncSession, eps: float = 0.35, min_samples: int = 2
    ) -> Dict[str, Any]:
        """
        Fetch entity embeddings from Qdrant, cluster with DBSCAN,
        and return labeled clusters.
        """
        # 1. Get user's entities from Postgres
        result = await db.execute(
            select(Entity)
            .where(Entity.user_id == user_id)
            .order_by(Entity.occurrence_count.desc())
            .limit(300)
        )
        entities = result.scalars().all()

        if len(entities) < 3:
            return {
                "clusters": [],
                "noise_entities": [{"name": e.name, "type": e.entity_type} for e in entities],
                "total_clusters": 0,
                "total_entities": len(entities),
                "note": "Not enough entities for clustering (minimum 3).",
            }

        # 2. Try to get embeddings from Qdrant
        embeddings = []
        entity_list = []
        try:
            from database.qdrant_client import get_qdrant_client, ENTITIES_COLLECTION
            qclient = get_qdrant_client()

            entity_ids = [str(e.id) for e in entities]
            points = await qclient.retrieve(
                collection_name=ENTITIES_COLLECTION,
                ids=entity_ids,
                with_vectors=True,
            )

            # Build aligned lists
            point_map = {p.id: p.vector for p in points if p.vector}
            for e in entities:
                eid = str(e.id)
                if eid in point_map:
                    embeddings.append(point_map[eid])
                    entity_list.append(e)

        except Exception as ex:
            logger.warning("qdrant_embeddings_unavailable", error=str(ex))
            # Fallback: use simple bag-of-chars encoding (very basic)
            for e in entities:
                # Create a simple 384-dim hash-based vector as fallback
                vec = np.zeros(384, dtype=np.float32)
                for i, c in enumerate(e.name.lower()):
                    vec[(ord(c) * (i + 1)) % 384] += 1.0
                embeddings.append(vec.tolist())
                entity_list.append(e)

        if len(embeddings) < 3:
            return {
                "clusters": [],
                "noise_entities": [],
                "total_clusters": 0,
                "total_entities": len(entities),
                "note": "Not enough embeddings available for clustering.",
            }

        # 3. Compute distance matrix and run DBSCAN
        vectors = np.array(embeddings, dtype=np.float32)
        dist_matrix = _cosine_distance_matrix(vectors)
        labels = _dbscan(dist_matrix, eps=eps, min_samples=min_samples)

        # 4. Group into clusters
        cluster_map: Dict[int, List[int]] = defaultdict(list)
        noise_indices: List[int] = []

        for idx, label in enumerate(labels):
            if label == -1:
                noise_indices.append(idx)
            else:
                cluster_map[label].append(idx)

        clusters = []
        for cid, indices in sorted(cluster_map.items()):
            members = []
            type_counts: Dict[str, int] = Counter()

            # Find centroid (mean vector)
            cluster_vectors = vectors[indices]
            centroid = cluster_vectors.mean(axis=0)
            centroid_norm = np.linalg.norm(centroid)
            if centroid_norm > 0:
                centroid = centroid / centroid_norm

            for idx in indices:
                e = entity_list[idx]
                # Cosine similarity to centroid
                v = vectors[idx]
                v_norm = np.linalg.norm(v)
                sim = float(np.dot(v, centroid) / (v_norm * 1.0)) if v_norm > 0 else 0

                members.append({
                    "id": str(e.id),
                    "name": e.name,
                    "entity_type": e.entity_type,
                    "similarity_to_centroid": round(sim, 3),
                    "occurrence_count": e.occurrence_count or 1,
                })
                type_counts[e.entity_type] = type_counts.get(e.entity_type, 0) + 1

            # Sort by similarity to centroid
            members.sort(key=lambda m: -m["similarity_to_centroid"])
            dominant_type = max(type_counts, key=type_counts.get) if type_counts else "unknown"

            # Coherence = avg pairwise similarity within cluster
            if len(indices) > 1:
                pairwise_sims = []
                for i in range(len(indices)):
                    for j in range(i + 1, len(indices)):
                        pairwise_sims.append(1.0 - dist_matrix[indices[i]][indices[j]])
                coherence = round(float(np.mean(pairwise_sims)), 3)
            else:
                coherence = 1.0

            clusters.append({
                "id": cid,
                "label": f"{dominant_type.title()} cluster: {members[0]['name']}",
                "dominant_type": dominant_type,
                "size": len(members),
                "coherence_score": coherence,
                "entities": members,
                "type_breakdown": dict(type_counts),
            })

        # Sort by size descending
        clusters.sort(key=lambda c: -c["size"])

        noise_entities = [
            {"name": entity_list[i].name, "entity_type": entity_list[i].entity_type}
            for i in noise_indices
        ]

        return {
            "clusters": clusters,
            "total_clusters": len(clusters),
            "total_entities": len(entity_list),
            "noise_entities": noise_entities[:20],
            "noise_count": len(noise_indices),
            "parameters": {"eps": eps, "min_samples": min_samples},
        }


# Global singleton
semantic_clustering_service = SemanticClusteringService()
