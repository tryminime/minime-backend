"""
Unit Tests for Node2Vec Service
Tests embedding generation, storage, and similarity search.
"""

import pytest
import numpy as np
import networkx as nx
from unittest.mock import Mock, patch, MagicMock

from services.node2vec_service import Node2VecService, node2vec_service


class TestNode2VecService:
    """Test Node2Vec service functionality."""
    
    @pytest.fixture
    def service(self):
        """Create Node2Vec service instance."""
        return Node2VecService(
            dimensions=128,
            walk_length=30,
            num_walks=10
        )
    
    @pytest.fixture
    def sample_graph(self):
        """Create sample NetworkX graph."""
        G = nx.Graph()
        G.add_nodes_from([1, 2, 3, 4, 5])
        G.add_edges_from([(1, 2), (2, 3), (3, 4), (4, 5), (5, 1)])
        return G
    
    def test_initialization(self, service):
        """Test service initialization with parameters."""
        assert service.dimensions == 128
        assert service.walk_length == 30
        assert service.num_walks == 10
        assert service.window_size == 5
        assert service.workers == 4
    
    def test_initialization_custom_params(self):
        """Test service with custom parameters."""
        service = Node2VecService(
            dimensions=64,
            walk_length=20,
            num_walks=5,
            p=2.0,
            q=0.5
        )
        
        assert service.dimensions == 64
        assert service.walk_length == 20
        assert service.num_walks == 5
        assert service.p == 2.0
        assert service.q == 0.5
    
    @patch('backend.services.node2vec_service.get_neo4j_session')
    def test_extract_subgraph_basic(self, mock_get_session, service):
        """Test basic subgraph extraction."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)
        mock_get_session.return_value = mock_session
        
        # Mock node query result
        mock_session.run = Mock()
        mock_session.run.return_value.data = Mock(side_effect=[
            # Nodes
            [
                {"nodeId": 1, "labels": ["PERSON"], "name": "Alice"},
                {"nodeId": 2, "labels": ["PAPER"], "name": "Paper1"}
            ],
            # Relationships
            [
                {"source": 1, "target": 2, "relType": "AUTHORED", "weight": 1.0}
            ]
        ])
        
        G = service._extract_subgraph(user_id="user-123")
        
        assert G.number_of_nodes() == 2
        assert G.number_of_edges() == 1
        assert 1 in G.nodes()
        assert 2 in G.nodes()
        assert G.has_edge(1, 2)
    
    @patch('backend.services.node2vec_service.get_neo4j_session')
    def test_extract_subgraph_with_filters(self, mock_get_session, service):
        """Test subgraph extraction with node/relationship filters."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)
        mock_get_session.return_value = mock_session
        
        mock_session.run = Mock()
        mock_session.run.return_value.data = Mock(side_effect=[
            [{"nodeId": 1, "labels": ["PERSON"], "name": "Alice"}],
            []
        ])
        
        G = service._extract_subgraph(
            user_id="user-123",
            node_types=["PERSON"],
            relationship_types=["AUTHORED"]
        )
        
        assert G.number_of_nodes() == 1
    
    def test_compute_graph_hash(self, service, sample_graph):
        """Test graph hash computation for caching."""
        hash1 = service._compute_graph_hash(sample_graph)
        
        # Same graph should have same hash
        hash2 = service._compute_graph_hash(sample_graph)
        assert hash1 == hash2
        
        # Different graph should have different hash
        G2 = nx.Graph()
        G2.add_nodes_from([1, 2, 3])
        G2.add_edges_from([(1, 2)])
        
        hash3 = service._compute_graph_hash(G2)
        assert hash1 != hash3
    
    @patch('backend.services.node2vec_service.get_neo4j_session')
    @patch('backend.services.node2vec_service.Node2Vec')
    def test_train_embeddings_success(
        self,
        mock_node2vec,
        mock_get_session,
        service
    ):
        """Test successful embedding training."""
        # Mock Neo4j session
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)
        mock_get_session.return_value = mock_session
        
        mock_session.run = Mock()
        mock_session.run.return_value.data = Mock(side_effect=[
            # Nodes
            [
                {"nodeId": 1, "labels": ["PERSON"], "name": "Alice"},
                {"nodeId": 2, "labels": ["PERSON"], "name": "Bob"}
            ],
            # Relationships
            [
                {"source": 1, "target": 2, "relType": "COLLABORATES_WITH", "weight": 1.0}
            ]
        ])
        
        # Mock Node2Vec model
        mock_model = Mock()
        mock_embeddings = np.random.rand(2, 128)  # 2 nodes, 128 dims
        mock_model.get_embedding.return_value = mock_embeddings
        mock_node2vec.return_value = mock_model
        
        # Train embeddings
        result = service.train_embeddings(user_id="user-123")
        
        assert "embeddings" in result
        assert len(result["embeddings"]) == 2
        assert "metadata" in result
        assert result["metadata"]["num_nodes"] == 2
        assert result["metadata"]["dimensions"] == 128
        
        # Verify Node2Vec was called with correct params
        mock_node2vec.assert_called_once()
        call_kwargs = mock_node2vec.call_args[1]
        assert call_kwargs["dimensions"] == 128
        assert call_kwargs["walk_length"] == 30
        assert call_kwargs["walk_number"] == 10
    
    @patch('backend.services.node2vec_service.get_neo4j_session')
    def test_train_embeddings_empty_graph(self, mock_get_session, service):
        """Test embedding training with empty graph."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)
        mock_get_session.return_value = mock_session
        
        # Empty graph
        mock_session.run = Mock()
        mock_session.run.return_value.data = Mock(side_effect=[[], []])
        
        result = service.train_embeddings(user_id="user-123")
        
        assert "error" in result
        assert "Empty graph" in result["error"]
    
    @patch('backend.services.node2vec_service.get_neo4j_session')
    @patch('backend.services.node2vec_service.Node2Vec')
    def test_train_embeddings_caching(
        self,
        mock_node2vec,
        mock_get_session,
        service
    ):
        """Test embedding caching mechanism."""
        # Mock Neo4j
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)
        mock_get_session.return_value = mock_session
        
        mock_session.run = Mock()
        mock_session.run.return_value.data = Mock(side_effect=[
            [{"nodeId": 1, "labels": ["PERSON"], "name": "Alice"}],
            [],
            # Second call (should use cache)
            [{"nodeId": 1, "labels": ["PERSON"], "name": "Alice"}],
            []
        ])
        
        # Mock Node2Vec
        mock_model = Mock()
        mock_embeddings = np.random.rand(1, 128)
        mock_model.get_embedding.return_value = mock_embeddings
        mock_node2vec.return_value = mock_model
        
        # First call - trains
        result1 = service.train_embeddings(user_id="user-123", use_cache=True)
        assert result1["metadata"]["from_cache"] is False
        
        # Second call - uses cache
        result2 = service.train_embeddings(user_id="user-123", use_cache=True)
        assert result2["metadata"]["from_cache"] is True
        
        # Node2Vec should only be called once
        assert mock_node2vec.call_count == 1
    
    def test_store_embeddings_qdrant(self, service):
        """Test storing embeddings in Qdrant."""
        mock_client = Mock()
        service._qdrant_client = mock_client
        
        embeddings = {
            1: np.random.rand(128),
            2: np.random.rand(128)
        }
        
        count = service.store_embeddings_qdrant(
            user_id="user-123",
            embeddings=embeddings
        )
        
        assert count == 2
        mock_client.upsert.assert_called_once()
        
        # Verify points structure
        call_args = mock_client.upsert.call_args
        points = call_args[1]["points"]
        assert len(points) == 2
    
    @patch('backend.services.node2vec_service.get_neo4j_session')
    def test_store_embeddings_neo4j_reduced(self, mock_get_session, service):
        """Test storing reduced embeddings in Neo4j."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        embeddings = {
            1: np.random.rand(128)
        }
        
        count = service.store_embeddings_neo4j(
            user_id="user-123",
            embeddings=embeddings,
            store_full=False  # Only first 8 dims
        )
        
        assert count == 1
        
        # Verify query was called
        call_args = mock_session.run.call_args
        assert "embedding_reduced" in call_args[0][0]
        
        # Verify only 8 dimensions stored
        embedding_arg = call_args[1]["embedding"]
        assert len(embedding_arg) == 8
    
    @patch('backend.services.node2vec_service.get_neo4j_session')
    def test_store_embeddings_neo4j_full(self, mock_get_session, service):
        """Test storing full embeddings in Neo4j."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=False)
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        embeddings = {
            1: np.random.rand(128)
        }
        
        count = service.store_embeddings_neo4j(
            user_id="user-123",
            embeddings=embeddings,
            store_full=True  # All 128 dims
        )
        
        assert count == 1
        
        # Verify full storage
        call_args = mock_session.run.call_args
        assert "embedding_full" in call_args[0][0]
        
        embedding_arg = call_args[1]["embedding"]
        assert len(embedding_arg) == 128
    
    def test_find_similar_nodes(self, service):
        """Test finding similar nodes via Qdrant."""
        mock_client = Mock()
        service._qdrant_client = mock_client
        
        # Mock retrieve (get source embedding)
        source_point = Mock()
        source_point.vector = [0.1] * 128
        mock_client.retrieve.return_value = [source_point]
        
        # Mock search (find similar)
        similar_result1 = Mock()
        similar_result1.id = 1
        similar_result1.score = 0.5  # Self (will be filtered)
        similar_result1.payload = {"name": "Self"}
        
        similar_result2 = Mock()
        similar_result2.id = 2
        similar_result2.score = 0.9
        similar_result2.payload = {"name": "Similar1"}
        
        similar_result3 = Mock()
        similar_result3.id = 3
        similar_result3.score = 0.8
        similar_result3.payload = {"name": "Similar2"}
        
        mock_client.search.return_value = [
            similar_result1,
            similar_result2,
            similar_result3
        ]
        
        # Find similar nodes
        results = service.find_similar_nodes(
            node_id=1,
            user_id="user-123",
            top_k=2
        )
        
        # Should exclude self and return top 2
        assert len(results) == 2
        assert results[0]["node_id"] == 2
        assert results[0]["similarity"] == 0.9
        assert results[1]["node_id"] == 3
        assert results[1]["similarity"] == 0.8
    
    def test_find_similar_nodes_with_threshold(self, service):
        """Test similarity search with minimum threshold."""
        mock_client = Mock()
        service._qdrant_client = mock_client
        
        source_point = Mock()
        source_point.vector = [0.1] * 128
        mock_client.retrieve.return_value = [source_point]
        
        # Results with varying scores
        results = []
        for i, score in enumerate([1.0, 0.8, 0.6, 0.4, 0.2]):
            result = Mock()
            result.id = i
            result.score = score
            result.payload = {}
            results.append(result)
        
        mock_client.search.return_value = results
        
        # Find with high threshold
        similar = service.find_similar_nodes(
            node_id=0,
            user_id="user-123",
            top_k=10,
            min_similarity=0.7  # High threshold
        )
        
        # Only scores >= 0.7 should be included (excluding self)
        assert len(similar) == 1
        assert similar[0]["similarity"] == 0.8
    
    def test_compute_all_similarities(self, service):
        """Test batch similarity computation."""
        embeddings = {
            1: np.array([1.0, 0.0, 0.0]),
            2: np.array([0.9, 0.1, 0.0]),  # Very similar to 1
            3: np.array([0.0, 1.0, 0.0])   # Orthogonal to 1
        }
        
        similarities = service.compute_all_similarities(
            user_id="user-123",
            embeddings=embeddings,
            top_k=2
        )
        
        # Each node should have top-2 similar nodes
        assert len(similarities) == 3
        assert len(similarities[1]) == 2
        
        # Node 1's most similar should be node 2
        assert similarities[1][0][0] == 2
        assert similarities[1][0][1] > 0.9  # High similarity
    
    def test_clear_cache(self, service):
        """Test cache clearing."""
        # Add something to cache
        service._embedding_cache["test"] = (np.array([1, 2, 3]), {})
        
        assert len(service._embedding_cache) == 1
        
        service.clear_cache()
        
        assert len(service._embedding_cache) == 0


class TestGlobalInstance:
    """Test global Node2Vec service instance."""
    
    def test_global_instance_exists(self):
        """Test that global instance is accessible."""
        assert node2vec_service is not None
        assert isinstance(node2vec_service, Node2VecService)
    
    def test_global_instance_defaults(self):
        """Test that global instance has sensible defaults."""
        assert node2vec_service.dimensions == 128
        assert node2vec_service.walk_length == 30
        assert node2vec_service.num_walks == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
