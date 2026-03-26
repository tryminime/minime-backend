"""
Node2Vec Embeddings Service
Generate graph embeddings using Node2Vec algorithm via karateclub library.
"""

from typing import Dict, List, Optional, Any, Tuple
import numpy as np
import networkx as nx
from datetime import datetime
import logging
import time
import hashlib
import pickle

try:
    from karateclub import Node2Vec
    _KARATECLUB_AVAILABLE = True
except ImportError:
    _KARATECLUB_AVAILABLE = False
    Node2Vec = None
from prometheus_client import Counter, Histogram, Gauge

from config.neo4j_config import get_neo4j_session
from config.settings import settings

logger = logging.getLogger(__name__)


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

embedding_generation_total = Counter(
    'embedding_generation_total',
    'Total embedding generation operations',
    ['status']
)

embedding_generation_duration = Histogram(
    'embedding_generation_duration_seconds',
    'Embedding generation duration',
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
)

embedding_graph_size = Gauge(
    'embedding_graph_size_nodes',
    'Size of graph for embedding generation',
    ['user_id']
)

embedding_cache_hits = Counter(
    'embedding_cache_hits_total',
    'Embedding cache hits vs misses',
    ['status']  # hit, miss
)


class Node2VecService:
    """
    Service for generating and managing Node2Vec embeddings.
    
    Features:
    - Extract subgraphs from Neo4j
    - Train Node2Vec embeddings (karateclub)
    - Store in Qdrant (high-dim) and Neo4j (low-dim)
    - Similarity search
    - Caching for performance
    """
    
    def __init__(
        self,
        dimensions: int = 128,
        walk_length: int = 30,
        num_walks: int = 10,
        window_size: int = 5,
        min_count: int = 1,
        workers: int = 4,
        p: float = 1.0,
        q: float = 1.0
    ):
        """
        Initialize Node2Vec service.
        
        Args:
            dimensions: Embedding vector dimensions (default: 128)
            walk_length: Length of random walks (default: 30)
            num_walks: Number of walks per node (default: 10)
            window_size: Context window size (default: 5)
            min_count: Minimum word count (default: 1)
            workers: Number of parallel workers (default: 4)
            p: Return parameter (default: 1.0)
            q: In-out parameter (default: 1.0)
        """
        self.logger = logging.getLogger(__name__)
        self.dimensions = dimensions
        self.walk_length = walk_length
        self.num_walks = num_walks
        self.window_size = window_size
        self.min_count = min_count
        self.workers = workers
        self.p = p
        self.q = q
        
        # Cache for embeddings
        self._embedding_cache: Dict[str, Tuple[np.ndarray, Dict]] = {}
        
        # Qdrant client (lazy initialization)
        self._qdrant_client = None
    
    def _get_qdrant_client(self):
        """Get or create Qdrant client."""
        if self._qdrant_client is None:
            from qdrant_client import QdrantClient
            from qdrant_client.models import Distance, VectorParams
            
            self._qdrant_client = QdrantClient(
                host=settings.QDRANT_HOST,
                port=settings.QDRANT_PORT
            )
            
            # Ensure collection exists
            collection_name = "node_embeddings"
            collections = self._qdrant_client.get_collections().collections
            
            if collection_name not in [c.name for c in collections]:
                self._qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=VectorParams(
                        size=self.dimensions,
                        distance=Distance.COSINE
                    )
                )
                self.logger.info(f"Created Qdrant collection '{collection_name}'")
        
        return self._qdrant_client
    
    def _extract_subgraph(
        self,
        user_id: str,
        node_types: List[str] = None,
        relationship_types: List[str] = None,
        limit: int = None
    ) -> nx.Graph:
        """
        Extract subgraph from Neo4j as NetworkX graph.
        
        Args:
            user_id: User ID for filtering
            node_types: Node types to include (default: all)
            relationship_types: Relationship types to include (default: all)
            limit: Maximum number of nodes (for large graphs)
            
        Returns:
            NetworkX undirected graph
        """
        self.logger.info(f"Extracting subgraph for user {user_id}...")
        
        # Build node filter
        node_filter = ""
        if node_types:
            labels = "|".join(node_types)
            node_filter = f":{labels}"
        
        # Build relationship filter
        rel_filter = ""
        if relationship_types:
            rel_types = "|".join(relationship_types)
            rel_filter = f":{rel_types}"
        
        # Build limit clause
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        with get_neo4j_session() as session:
            # Extract nodes
            node_query = f"""
            MATCH (n{node_filter})
            WHERE n.user_id = $userId
            RETURN id(n) as nodeId, labels(n) as labels, n.canonical_name as name
            {limit_clause}
            """
            
            nodes = session.run(node_query, userId=user_id).data()
            
            # Extract relationships
            rel_query = f"""
            MATCH (a{node_filter})-[r{rel_filter}]-(b{node_filter})
            WHERE a.user_id = $userId AND b.user_id = $userId
            RETURN id(a) as source, id(b) as target, type(r) as relType, r.weight as weight
            """
            
            relationships = session.run(rel_query, userId=user_id).data()
        
        # Build NetworkX graph
        G = nx.Graph()
        
        # Add nodes
        for node in nodes:
            G.add_node(
                node["nodeId"],
                labels=node["labels"],
                name=node.get("name", f"node_{node['nodeId']}")
            )
        
        # Add edges
        for rel in relationships:
            weight = rel.get("weight", 1.0)
            G.add_edge(
                rel["source"],
                rel["target"],
                weight=weight,
                rel_type=rel["relType"]
            )
        
        self.logger.info(
            f"Extracted graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges"
        )
        
        embedding_graph_size.labels(user_id=user_id).set(G.number_of_nodes())
        
        return G
    
    def _compute_graph_hash(self, G: nx.Graph) -> str:
        """
        Compute hash of graph structure for caching.
        
        Args:
            G: NetworkX graph
            
        Returns:
            SHA256 hash of graph
        """
        # Create canonical representation
        nodes_str = str(sorted(G.nodes()))
        edges_str = str(sorted(G.edges()))
        
        combined = f"{nodes_str}_{edges_str}"
        
        return hashlib.sha256(combined.encode()).hexdigest()
    
    def train_embeddings(
        self,
        user_id: str,
        node_types: List[str] = None,
        relationship_types: List[str] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Train Node2Vec embeddings for user's graph.
        
        Args:
            user_id: User ID
            node_types: Node types to include
            relationship_types: Relationship types to include
            use_cache: If True, use cached embeddings if available
            
        Returns:
            Dictionary with embeddings and metadata
        """
        start_time = time.time()
        
        try:
            # Extract subgraph
            G = self._extract_subgraph(
                user_id=user_id,
                node_types=node_types,
                relationship_types=relationship_types
            )
            
            if G.number_of_nodes() == 0:
                raise ValueError(f"Empty graph for user {user_id}")
            
            # Check cache
            graph_hash = self._compute_graph_hash(G)
            cache_key = f"{user_id}_{graph_hash}"
            
            if use_cache and cache_key in self._embedding_cache:
                self.logger.info(f"Using cached embeddings for {user_id}")
                embedding_cache_hits.labels(status='hit').inc()
                
                embeddings, metadata = self._embedding_cache[cache_key]
                metadata["from_cache"] = True
                
                return {
                    "user_id": user_id,
                    "embeddings": embeddings,
                    "node_ids": list(G.nodes()),
                    "metadata": metadata
                }
            
            embedding_cache_hits.labels(status='miss').inc()
            
            # Convert to contiguous node IDs (0, 1, 2, ...) for karateclub
            node_mapping = {node: idx for idx, node in enumerate(G.nodes())}
            reverse_mapping = {idx: node for node, idx in node_mapping.items()}
            
            G_mapped = nx.relabel_nodes(G, node_mapping)
            
            # Train Node2Vec
            self.logger.info(
                f"Training Node2Vec: {G.number_of_nodes()} nodes, "
                f"dim={self.dimensions}, walks={self.num_walks}, length={self.walk_length}"
            )
            
            model = Node2Vec(
                dimensions=self.dimensions,
                walk_length=self.walk_length,
                walk_number=self.num_walks,
                window_size=self.window_size,
                min_count=self.min_count,
                workers=self.workers,
                p=self.p,
                q=self.q
            )
            
            model.fit(G_mapped)
            
            # Get embeddings (in mapped order)
            embeddings_mapped = model.get_embedding()
            
            # Remap to original node IDs
            embeddings = {}
            for idx, embedding in enumerate(embeddings_mapped):
                original_node_id = reverse_mapping[idx]
                embeddings[original_node_id] = embedding
            
            elapsed = time.time() - start_time
            
            metadata = {
                "dimensions": self.dimensions,
                "walk_length": self.walk_length,
                "num_walks": self.num_walks,
                "num_nodes": G.number_of_nodes(),
                "num_edges": G.number_of_edges(),
                "training_time_sec": elapsed,
                "timestamp": datetime.utcnow().isoformat(),
                "from_cache": False
            }
            
            # Cache embeddings
            if use_cache:
                self._embedding_cache[cache_key] = (embeddings, metadata)
            
            # Track metrics
            embedding_generation_total.labels(status='success').inc()
            embedding_generation_duration.observe(elapsed)
            
            self.logger.info(f"Embeddings trained in {elapsed:.2f}s")
            
            return {
                "user_id": user_id,
                "embeddings": embeddings,
                "node_ids": list(G.nodes()),
                "metadata": metadata
            }
            
        except Exception as e:
            elapsed = time.time() - start_time
            
            embedding_generation_total.labels(status='error').inc()
            
            self.logger.error(f"Embedding generation failed: {e}", exc_info=True)
            
            return {
                "user_id": user_id,
                "status": "error",
                "error": str(e),
                "execution_time_sec": elapsed
            }
    
    def store_embeddings_qdrant(
        self,
        user_id: str,
        embeddings: Dict[int, np.ndarray],
        metadata: Dict[str, Any] = None
    ) -> int:
        """
        Store embeddings in Qdrant for similarity search.
        
        Args:
            user_id: User ID
            embeddings: Dict mapping node IDs to embedding vectors
            metadata: Additional metadata
            
        Returns:
            Number of vectors stored
        """
        from qdrant_client.models import PointStruct
        
        client = self._get_qdrant_client()
        collection_name = "node_embeddings"
        
        points = []
        for node_id, embedding in embeddings.items():
            point = PointStruct(
                id=node_id,
                vector=embedding.tolist(),
                payload={
                    "user_id": user_id,
                    "node_id": node_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    **(metadata or {})
                }
            )
            points.append(point)
        
        # Batch upload
        client.upsert(
            collection_name=collection_name,
            points=points
        )
        
        self.logger.info(f"Stored {len(points)} embeddings in Qdrant")
        
        return len(points)
    
    def store_embeddings_neo4j(
        self,
        user_id: str,
        embeddings: Dict[int, np.ndarray],
        store_full: bool = False
    ) -> int:
        """
        Store embeddings in Neo4j as node properties.
        
        For large dimensions (128), only store first few dimensions.
        For full storage, set store_full=True.
        
        Args:
            user_id: User ID
            embeddings: Dict mapping node IDs to embedding vectors
            store_full: If True, store full embeddings (can be large)
            
        Returns:
            Number of nodes updated
        """
        with get_neo4j_session() as session:
            count = 0
            
            for node_id, embedding in embeddings.items():
                # For Neo4j, store reduced dimensions (first 8) by default
                # Full embeddings go to Qdrant
                if store_full:
                    embedding_to_store = embedding.tolist()
                    property_name = "embedding_full"
                else:
                    embedding_to_store = embedding[:8].tolist()  # First 8 dims
                    property_name = "embedding_reduced"
                
                query = f"""
                MATCH (n)
                WHERE id(n) = $nodeId AND n.user_id = $userId
                SET n.{property_name} = $embedding
                RETURN n
                """
                
                result = session.run(
                    query,
                    nodeId=node_id,
                    userId=user_id,
                    embedding=embedding_to_store
                ).single()
                
                if result:
                    count += 1
        
        self.logger.info(
            f"Stored {'full' if store_full else 'reduced'} embeddings "
            f"in Neo4j for {count} nodes"
        )
        
        return count
    
    def find_similar_nodes(
        self,
        node_id: int,
        user_id: str,
        top_k: int = 10,
        min_similarity: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Find similar nodes using cosine similarity in Qdrant.
        
        Args:
            node_id: Source node ID
            user_id: User ID for filtering
            top_k: Number of similar nodes to return
            min_similarity: Minimum similarity threshold
            
        Returns:
            List of similar nodes with scores
        """
        client = self._get_qdrant_client()
        collection_name = "node_embeddings"
        
        # Get embedding for source node
        try:
            point = client.retrieve(
                collection_name=collection_name,
                ids=[node_id]
            )[0]
            
            query_vector = point.vector
        except Exception as e:
            self.logger.error(f"Failed to retrieve node {node_id}: {e}")
            return []
        
        # Search for similar vectors
        results = client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            limit=top_k + 1,  # +1 to exclude self
            query_filter={
                "must": [
                    {"key": "user_id", "match": {"value": user_id}}
                ]
            }
        )
        
        # Filter out self and apply similarity threshold
        similar_nodes = []
        for result in results:
            if result.id == node_id:
                continue  # Skip self
            
            if result.score >= min_similarity:
                similar_nodes.append({
                    "node_id": result.id,
                    "similarity": result.score,
                    "payload": result.payload
                })
        
        return similar_nodes[:top_k]
    
    def compute_all_similarities(
        self,
        user_id: str,
        embeddings: Dict[int, np.ndarray],
        top_k: int = 10
    ) -> Dict[int, List[Tuple[int, float]]]:
        """
        Compute top-k similar nodes for all nodes (batch).
        
        Uses numpy for efficient computation.
        
        Args:
            user_id: User ID
            embeddings: Dict mapping node IDs to embeddings
            top_k: Number of similar nodes per node
            
        Returns:
            Dict mapping each node to list of (similar_node_id, similarity)
        """
        # Convert to matrix
        node_ids = list(embeddings.keys())
        embedding_matrix = np.array([embeddings[nid] for nid in node_ids])
        
        # Normalize for cosine similarity
        from sklearn.preprocessing import normalize
        embedding_matrix_norm = normalize(embedding_matrix, axis=1)
        
        # Compute similarity matrix (all-pairs)
        similarity_matrix = embedding_matrix_norm @ embedding_matrix_norm.T
        
        # For each node, find top-k similar
        similarities = {}
        for i, node_id in enumerate(node_ids):
            # Get similarities for this node
            node_similarities = similarity_matrix[i]
            
            # Get top-k (excluding self)
            top_indices = np.argsort(node_similarities)[::-1]
            
            similar_nodes = []
            for idx in top_indices:
                if idx == i:
                    continue  # Skip self
                
                similar_node_id = node_ids[idx]
                similarity_score = node_similarities[idx]
                
                similar_nodes.append((similar_node_id, float(similarity_score)))
                
                if len(similar_nodes) >= top_k:
                    break
            
            similarities[node_id] = similar_nodes
        
        return similarities
    
    def clear_cache(self):
        """Clear embedding cache."""
        self._embedding_cache.clear()
        self.logger.info("Embedding cache cleared")


# Global service instance
node2vec_service = Node2VecService()
