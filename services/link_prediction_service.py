"""
Link Prediction Service

Predicts likely future connections in the knowledge graph using multiple methods:
- Common Neighbors
- Jaccard Coefficient
- Adamic-Adar Index
- Preferential Attachment
- Embedding Similarity (cosine)
- Aggregate scoring with configurable weights
- Batch prediction
"""

from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict
import math
import structlog

logger = structlog.get_logger()


# ============================================================================
# DEFAULT METHOD WEIGHTS
# ============================================================================

DEFAULT_METHOD_WEIGHTS = {
    'common_neighbors': 0.25,
    'jaccard': 0.25,
    'adamic_adar': 0.20,
    'preferential_attachment': 0.15,
    'embedding_similarity': 0.15,
}


class LinkPredictionService:
    """
    Service for predicting missing or future links in the knowledge graph.

    All methods operate on in-memory adjacency data (no live Neo4j needed).
    """

    def __init__(self, method_weights: Optional[Dict[str, float]] = None):
        self.method_weights = method_weights or DEFAULT_METHOD_WEIGHTS.copy()

    # ========================================================================
    # CORE PREDICTION METHODS
    # ========================================================================

    def common_neighbors(
        self,
        node_a: str,
        node_b: str,
        adjacency: Dict[str, Set[str]],
    ) -> Dict[str, Any]:
        """
        Compute Common Neighbors score.

        Score = |N(a) ∩ N(b)| — number of shared neighbors.

        Args:
            node_a: First node
            node_b: Second node
            adjacency: Node → set of neighbors

        Returns:
            Score and shared neighbor details
        """
        neighbors_a = adjacency.get(node_a, set())
        neighbors_b = adjacency.get(node_b, set())
        common = neighbors_a & neighbors_b

        return {
            'method': 'common_neighbors',
            'score': len(common),
            'normalized_score': self._normalize_cn(len(common), adjacency),
            'common_neighbors': sorted(list(common)),
            'count': len(common),
        }

    def jaccard_coefficient(
        self,
        node_a: str,
        node_b: str,
        adjacency: Dict[str, Set[str]],
    ) -> Dict[str, Any]:
        """
        Compute Jaccard Coefficient.

        Score = |N(a) ∩ N(b)| / |N(a) ∪ N(b)|

        Args:
            node_a: First node
            node_b: Second node
            adjacency: Node → set of neighbors

        Returns:
            Jaccard coefficient (0.0 to 1.0)
        """
        neighbors_a = adjacency.get(node_a, set())
        neighbors_b = adjacency.get(node_b, set())

        intersection = neighbors_a & neighbors_b
        union = neighbors_a | neighbors_b

        score = len(intersection) / len(union) if union else 0.0

        return {
            'method': 'jaccard',
            'score': round(score, 4),
            'normalized_score': round(score, 4),  # Already normalized
            'intersection_size': len(intersection),
            'union_size': len(union),
        }

    def adamic_adar_index(
        self,
        node_a: str,
        node_b: str,
        adjacency: Dict[str, Set[str]],
    ) -> Dict[str, Any]:
        """
        Compute Adamic-Adar Index.

        Score = Σ 1/log(|N(z)|) for z in N(a) ∩ N(b)

        Weights common neighbors by inverse log of their degree,
        giving more credit to rare shared connections.

        Args:
            node_a: First node
            node_b: Second node
            adjacency: Node → set of neighbors

        Returns:
            Adamic-Adar index score
        """
        neighbors_a = adjacency.get(node_a, set())
        neighbors_b = adjacency.get(node_b, set())
        common = neighbors_a & neighbors_b

        score = 0.0
        for z in common:
            degree_z = len(adjacency.get(z, set()))
            if degree_z > 1:
                score += 1.0 / math.log(degree_z)

        # Normalize by max possible score
        max_score = len(common) / math.log(2) if common else 1.0
        normalized = score / max_score if max_score > 0 else 0.0

        return {
            'method': 'adamic_adar',
            'score': round(score, 4),
            'normalized_score': round(min(normalized, 1.0), 4),
            'common_neighbor_count': len(common),
        }

    def preferential_attachment(
        self,
        node_a: str,
        node_b: str,
        adjacency: Dict[str, Set[str]],
    ) -> Dict[str, Any]:
        """
        Compute Preferential Attachment score.

        Score = |N(a)| × |N(b)| — product of node degrees.

        Based on the idea that new links are more likely between
        highly connected nodes.

        Args:
            node_a: First node
            node_b: Second node
            adjacency: Node → set of neighbors

        Returns:
            Preferential attachment score
        """
        degree_a = len(adjacency.get(node_a, set()))
        degree_b = len(adjacency.get(node_b, set()))
        score = degree_a * degree_b

        # Normalize by max possible
        max_degree = max(len(n) for n in adjacency.values()) if adjacency else 1
        max_pa = max_degree * max_degree
        normalized = score / max_pa if max_pa > 0 else 0.0

        return {
            'method': 'preferential_attachment',
            'score': score,
            'normalized_score': round(min(normalized, 1.0), 4),
            'degree_a': degree_a,
            'degree_b': degree_b,
        }

    def embedding_similarity(
        self,
        node_a: str,
        node_b: str,
        embeddings: Dict[str, List[float]],
    ) -> Dict[str, Any]:
        """
        Compute cosine similarity between node embeddings.

        Args:
            node_a: First node
            node_b: Second node
            embeddings: Node → embedding vector

        Returns:
            Cosine similarity (-1.0 to 1.0)
        """
        emb_a = embeddings.get(node_a)
        emb_b = embeddings.get(node_b)

        if emb_a is None or emb_b is None:
            return {
                'method': 'embedding_similarity',
                'score': 0.0,
                'normalized_score': 0.0,
                'available': False,
            }

        similarity = self._cosine_similarity(emb_a, emb_b)

        # Map from [-1, 1] to [0, 1]
        normalized = (similarity + 1) / 2

        return {
            'method': 'embedding_similarity',
            'score': round(similarity, 4),
            'normalized_score': round(normalized, 4),
            'available': True,
        }

    # ========================================================================
    # AGGREGATE PREDICTION
    # ========================================================================

    def predict_link(
        self,
        node_a: str,
        node_b: str,
        adjacency: Dict[str, Set[str]],
        embeddings: Optional[Dict[str, List[float]]] = None,
    ) -> Dict[str, Any]:
        """
        Predict likelihood of a link using all methods.

        Args:
            node_a: First node
            node_b: Second node
            adjacency: Node → set of neighbors
            embeddings: Optional node embeddings

        Returns:
            Aggregate prediction with per-method breakdown
        """
        embeddings = embeddings or {}

        # Already connected?
        already_connected = node_b in adjacency.get(node_a, set())

        # Compute each method
        cn = self.common_neighbors(node_a, node_b, adjacency)
        jc = self.jaccard_coefficient(node_a, node_b, adjacency)
        aa = self.adamic_adar_index(node_a, node_b, adjacency)
        pa = self.preferential_attachment(node_a, node_b, adjacency)
        es = self.embedding_similarity(node_a, node_b, embeddings)

        methods = {
            'common_neighbors': cn,
            'jaccard': jc,
            'adamic_adar': aa,
            'preferential_attachment': pa,
            'embedding_similarity': es,
        }

        # Weighted aggregate
        aggregate_score = 0.0
        total_weight = 0.0
        for method_name, result in methods.items():
            weight = self.method_weights.get(method_name, 0.0)
            if method_name == 'embedding_similarity' and not result.get('available'):
                continue
            aggregate_score += result['normalized_score'] * weight
            total_weight += weight

        if total_weight > 0:
            aggregate_score /= total_weight

        return {
            'node_a': node_a,
            'node_b': node_b,
            'aggregate_score': round(aggregate_score, 4),
            'already_connected': already_connected,
            'prediction': 'likely' if aggregate_score >= 0.5 else (
                'possible' if aggregate_score >= 0.2 else 'unlikely'
            ),
            'methods': methods,
        }

    def predict_top_links(
        self,
        node: str,
        adjacency: Dict[str, Set[str]],
        embeddings: Optional[Dict[str, List[float]]] = None,
        top_k: int = 10,
        exclude_existing: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Predict top-k most likely new links for a node.

        Args:
            node: Source node
            adjacency: Node → set of neighbors
            embeddings: Optional node embeddings
            top_k: Number of predictions
            exclude_existing: Whether to exclude already-connected nodes

        Returns:
            Top-k predictions sorted by aggregate score
        """
        existing = adjacency.get(node, set())
        candidates = set()

        # Candidates: 2nd hop neighbors (friends of friends)
        for neighbor in existing:
            for second_hop in adjacency.get(neighbor, set()):
                if second_hop != node:
                    if not exclude_existing or second_hop not in existing:
                        candidates.add(second_hop)

        # Score all candidates
        predictions = []
        for candidate in candidates:
            pred = self.predict_link(node, candidate, adjacency, embeddings)
            predictions.append(pred)

        # Sort by aggregate score
        predictions.sort(key=lambda x: x['aggregate_score'], reverse=True)
        return predictions[:top_k]

    def batch_predict(
        self,
        adjacency: Dict[str, Set[str]],
        embeddings: Optional[Dict[str, List[float]]] = None,
        min_score: float = 0.3,
        top_k_per_node: int = 5,
    ) -> Dict[str, Any]:
        """
        Predict new links for all nodes in the graph.

        Args:
            adjacency: Full graph adjacency
            embeddings: Optional node embeddings
            min_score: Minimum aggregate score to include
            top_k_per_node: Top predictions per node

        Returns:
            Batch prediction results
        """
        all_predictions = []
        seen_pairs: Set[Tuple[str, str]] = set()

        for node in adjacency:
            preds = self.predict_top_links(
                node, adjacency, embeddings,
                top_k=top_k_per_node, exclude_existing=True
            )
            for pred in preds:
                pair = tuple(sorted([pred['node_a'], pred['node_b']]))
                if pair not in seen_pairs and pred['aggregate_score'] >= min_score:
                    seen_pairs.add(pair)
                    all_predictions.append(pred)

        # Sort globally by score
        all_predictions.sort(key=lambda x: x['aggregate_score'], reverse=True)

        return {
            'predictions': all_predictions,
            'total_predictions': len(all_predictions),
            'nodes_analyzed': len(adjacency),
            'min_score_threshold': min_score,
        }

    # ========================================================================
    # UTILITY METHODS
    # ========================================================================

    def _normalize_cn(
        self,
        cn_score: int,
        adjacency: Dict[str, Set[str]],
    ) -> float:
        """Normalize common neighbors score to [0, 1]."""
        if not adjacency:
            return 0.0
        max_degree = max(len(n) for n in adjacency.values())
        return round(cn_score / max(max_degree, 1), 4)

    @staticmethod
    def _cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if len(vec_a) != len(vec_b) or not vec_a:
            return 0.0

        dot = sum(a * b for a, b in zip(vec_a, vec_b))
        norm_a = math.sqrt(sum(a * a for a in vec_a))
        norm_b = math.sqrt(sum(b * b for b in vec_b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot / (norm_a * norm_b)


# Global instance
link_prediction_service = LinkPredictionService()
