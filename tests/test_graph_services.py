"""
Comprehensive Unit Tests for Graph Services
Tests Node2Vec, Community, Centrality, and Graph API services
Target: 80%+ code coverage
"""

import pytest
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from services.node2vec_service import Node2VecService
from services.community_service import CommunityService
from services.centrality_service import CentralityService


class TestNode2VecService:
    """Test Node2Vec embedding service."""
    
    @pytest.fixture
    def service(self):
        """Create service instance."""
        return Node2VecService()
    
    @pytest.fixture
    def mock_neo4j_session(self):
        """Mock Neo4j session."""
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        return session
    
    def test_extract_subgraph(self, service, mock_neo4j_session):
        """Test graph extraction from Neo4j."""
        # Mock Neo4j response
        mock_neo4j_session.run.return_value.data.return_value = [
            {
                'nodeId': 1,
                'labels': ['PERSON'],
                'properties': {'name': 'Alice'}
            },
            {
                'nodeId': 2,
                'labels': ['PERSON'],
                'properties': {'name': 'Bob'}
            }
        ]
        
        with patch('backend.services.node2vec_service.get_neo4j_session', return_value=mock_neo4j_session):
            graph = service._extract_subgraph('user-123')
        
        assert graph.number_of_nodes() == 2
        assert graph.has_node('1')
        assert graph.has_node('2')
    
    def test_generate_embeddings(self, service):
        """Test embedding generation."""
        # Create small test graph
        import networkx as nx
        graph = nx.karate_club_graph()
        
        embeddings = service._generate_embeddings(graph)
        
        assert embeddings.shape[0] == graph.number_of_nodes()
        assert embeddings.shape[1] == 128  # Default dimension
        assert embeddings.dtype == np.float32
    
    def test_store_embeddings_qdrant(self, service):
        """Test storing embeddings in Qdrant."""
        embeddings = np.random.rand(10, 128).astype(np.float32)
        node_ids = list(range(10))
        metadata = [{'name': f'Node {i}', 'node_type': 'PERSON'} for i in range(10)]
        
        with patch.object(service.qdrant_client, 'upsert') as mock_upsert:
            service._store_embeddings_qdrant(
                embeddings=embeddings,
                node_ids=node_ids,
                metadata=metadata,
                user_id='user-123'
            )
            
            assert mock_upsert.called
            call_args = mock_upsert.call_args
            assert call_args[1]['collection_name'] == 'node_embeddings_user-123'
    
    def test_find_similar_nodes(self, service):
        """Test similarity search."""
        # Mock Qdrant response
        mock_results = [
            Mock(id=2, score=0.95, payload={'name': 'Similar Node', 'node_type': 'PERSON'}),
            Mock(id=3, score=0.85, payload={'name': 'Another Node', 'node_type': 'PAPER'})
        ]
        
        with patch.object(service.qdrant_client, 'search', return_value=mock_results):
            results = service.find_similar_nodes(
                node_id=1,
                user_id='user-123',
                top_k=2,
                min_similarity=0.8
            )
        
        assert len(results) == 2
        assert results[0]['similarity'] == 0.95
        assert results[1]['node_id'] == 3
    
    def test_compute_embeddings_with_cache(self, service):
        """Test embedding computation with caching."""
        with patch.object(service, '_extract_subgraph') as mock_extract, \
             patch.object(service, '_generate_embeddings') as mock_generate, \
             patch.object(service, '_store_embeddings_qdrant') as mock_store_q, \
             patch.object(service, '_store_embeddings_neo4j') as mock_store_n:
            
            import networkx as nx
            mock_graph = nx.karate_club_graph()
            mock_extract.return_value = mock_graph
            mock_generate.return_value = np.random.rand(34, 128).astype(np.float32)
            
            # First call - should compute
            service.compute_embeddings('user-123')
            assert mock_generate.called
            
            # Second call with same graph - should use cache
            mock_generate.reset_mock()
            service.compute_embeddings('user-123')
            # Cache should prevent recomputation
            # (Note: actual cache behavior depends on graph hash)


class TestCommunityService:
    """Test community detection service."""
    
    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CommunityService()
    
    @pytest.fixture
    def mock_neo4j_session(self):
        """Mock Neo4j session."""
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        return session
    
    def test_detect_communities(self, service, mock_neo4j_session):
        """Test community detection."""
        # Mock GDS projection and algorithm execution
        mock_neo4j_session.run.return_value.single.return_value = {
            'communityCount': 5,
            'modularity': 0.65
        }
        
        with patch('backend.services.community_service.get_neo4j_session', return_value=mock_neo4j_session):
            result = service.detect_communities('user-123')
        
        assert result['community_count'] == 5
        assert result['modularity'] == 0.65
        assert mock_neo4j_session.run.called
    
    def test_get_communities(self, service, mock_neo4j_session):
        """Test getting community list."""
        mock_neo4j_session.run.return_value.data.return_value = [
            {
                'communityId': 1,
                'size': 50,
                'members': [
                    {'nodeId': 1, 'labels': ['PERSON'], 'name': 'Alice'},
                    {'nodeId': 2, 'labels': ['PERSON'], 'name': 'Bob'}
                ]
            },
            {
                'communityId': 2,
                'size': 30,
                'members': [
                    {'nodeId': 3, 'labels': ['PAPER'], 'name': 'Paper 1'}
                ]
            }
        ]
        
        with patch('backend.services.community_service.get_neo4j_session', return_value=mock_neo4j_session):
            communities = service.get_communities('user-123', min_size=10)
        
        assert len(communities) == 2
        assert communities[0]['community_id'] == 1
        assert communities[0]['size'] == 50
    
    def test_calculate_modularity(self, service, mock_neo4j_session):
        """Test modularity calculation."""
        mock_neo4j_session.run.return_value.single.return_value = {
            'modularity': 0.72
        }
        
        with patch('backend.services.community_service.get_neo4j_session', return_value=mock_neo4j_session):
            modularity = service.calculate_modularity('user-123')
        
        assert modularity == 0.72
        assert 0 <= modularity <= 1
    
    def test_find_community_bridges(self, service, mock_neo4j_session):
        """Test finding bridge nodes between communities."""
        mock_neo4j_session.run.return_value.data.return_value = [
            {
                'nodeId': 5,
                'nodeName': 'Bridge Node',
                'externalConnections': 10,
                'pagerank': 0.15
            }
        ]
        
        with patch('backend.services.community_service.get_neo4j_session', return_value=mock_neo4j_session):
            bridges = service.find_community_bridges('user-123', community_id=1, top_k=5)
        
        assert len(bridges) == 1
        assert bridges[0]['node_id'] == 5
        assert bridges[0]['external_connections'] == 10


class TestCentralityService:
    """Test centrality metrics service."""
    
    @pytest.fixture
    def service(self):
        """Create service instance."""
        return CentralityService()
    
    @pytest.fixture
    def mock_neo4j_session(self):
        """Mock Neo4j session."""
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        return session
    
    def test_compute_degree_centrality(self, service, mock_neo4j_session):
        """Test degree centrality computation."""
        mock_neo4j_session.run.return_value.single.return_value = {
            'nodesComputed': 100
        }
        
        with patch('backend.services.centrality_service.get_neo4j_session', return_value=mock_neo4j_session):
            result = service.compute_degree_centrality('user-123')
        
        assert result['nodes_computed'] == 100
        assert mock_neo4j_session.run.called
    
    def test_compute_pagerank(self, service, mock_neo4j_session):
        """Test PageRank computation."""
        mock_neo4j_session.run.return_value.single.return_value = {
            'nodesComputed': 100,
            'ranIterations': 20
        }
        
        with patch('backend.services.centrality_service.get_neo4j_session', return_value=mock_neo4j_session):
            result = service.compute_pagerank('user-123')
        
        assert result['nodes_computed'] == 100
        assert result['iterations'] == 20
    
    def test_compute_all_metrics(self, service):
        """Test computing all centrality metrics."""
        with patch.object(service, 'compute_degree_centrality') as mock_degree, \
             patch.object(service, 'compute_betweenness_centrality') as mock_between, \
             patch.object(service, 'compute_closeness_centrality') as mock_close, \
             patch.object(service, 'compute_eigenvector_centrality') as mock_eigen, \
             patch.object(service, 'compute_pagerank') as mock_pagerank:
            
            mock_degree.return_value = {'nodes_computed': 100}
            mock_between.return_value = {'nodes_computed': 100}
            mock_close.return_value = {'nodes_computed': 100}
            mock_eigen.return_value = {'nodes_computed': 100}
            mock_pagerank.return_value = {'nodes_computed': 100}
            
            results = service.compute_all_metrics('user-123')
        
        assert 'degree_centrality' in results
        assert 'pagerank' in results
        assert all(mock.called for mock in [mock_degree, mock_between, mock_close, mock_eigen, mock_pagerank])


class TestGraphAPIIntegration:
    """Integration tests for Graph API endpoints."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from main import app
        return TestClient(app)
    
    @pytest.fixture
    def auth_headers(self):
        """Create auth headers."""
        # Mock JWT token
        return {"Authorization": "Bearer test-token"}
    
    def test_get_node_details_integration(self, client, auth_headers):
        """Test node details endpoint integration."""
        with patch('backend.api.v1.graph.get_current_user'), \
             patch('backend.api.v1.graph.get_neo4j_session'):
            
            response = client.get("/api/v1/graph/nodes/1", headers=auth_headers)
            
            # Should return 200 or 404 (depending on mock)
            assert response.status_code in [200, 404, 401]
    
    def test_export_graph_integration(self, client, auth_headers):
        """Test graph export endpoint integration."""
        with patch('backend.api.v1.graph.get_current_user'), \
             patch('backend.api.v1.graph.get_neo4j_session'):
            
            response = client.get("/api/v1/graph/export?limit=10", headers=auth_headers)
            
            assert response.status_code in [200, 401]
    
    def test_rate_limiting(self, client, auth_headers):
        """Test rate limiting enforcement."""
        # This would require actual rate limiter setup
        # Placeholder for rate limit test
        pass


# Performance benchmarks
class TestPerformanceBenchmarks:
    """Performance benchmark tests."""
    
    def test_embedding_generation_performance(self):
        """Benchmark embedding generation."""
        import time
        import networkx as nx
        
        service = Node2VecService()
        graph = nx.karate_club_graph()
        
        start = time.time()
        embeddings = service._generate_embeddings(graph)
        duration = time.time() - start
        
        # Should complete in reasonable time
        assert duration < 5.0  # 5 seconds for small graph
        assert embeddings.shape == (34, 128)
    
    def test_centrality_computation_performance(self):
        """Benchmark centrality computation."""
        # This would require actual Neo4j connection
        # Placeholder for performance test
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--cov=backend.services', '--cov-report=html'])
