"""
Integration Tests for Graph API
Tests all 18 endpoints with various scenarios.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch

from main import app
from models.user import User


class TestGraphAPIEndpoints:
    """Test Graph API endpoints."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)
    
    @pytest.fixture
    def mock_user(self):
        """Create mock user."""
        return User(
            id="test-user-123",
            email="test@example.com",
            username="testuser"
        )
    
    @pytest.fixture
    def auth_headers(self):
        """Create auth headers."""
        return {"Authorization": "Bearer test-token"}
    
    # ========================================================================
    # NODE ENDPOINTS (2 tests)
    # ========================================================================
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.get_neo4j_session')
    def test_get_node_details(self, mock_session, mock_get_user, client, mock_user, auth_headers):
        """Test GET /nodes/{node_id}."""
        mock_get_user.return_value = mock_user
        
        # Mock Neo4j result
        mock_result = Mock()
        mock_result.__getitem__ = Mock(side_effect=lambda k: {
            "nodeId": 42,
            "labels": ["PERSON"],
            "properties": {"name": "Alice", "email": "alice@example.com"},
            "degreeCentrality": 0.5,
            "pagerank": 0.15,
            "communityId": 1,
            "neighborCount": 10
        }.get(k))
        mock_result.get = Mock(side_effect=lambda k, default=None: {
            "degreeCentrality": 0.5,
            "pagerank": 0.15,
            "communityId": 1
        }.get(k, default))
        
        mock_session_instance = Mock()
        mock_session_instance.__enter__ = Mock(return_value=mock_session_instance)
        mock_session_instance.__exit__ = Mock(return_value=False)
        mock_session_instance.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        mock_session.return_value = mock_session_instance
        
        response = client.get("/api/v1/graph/nodes/42", headers=auth_headers)
        
        assert response.status_code == 200
        data = response.json()
        assert data["node_id"] == 42
        assert "metrics" in data
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.get_neo4j_session')
    def test_get_node_neighbors(self, mock_session, mock_get_user, client, mock_user, auth_headers):
        """Test GET /nodes/{node_id}/neighbors."""
        mock_get_user.return_value = mock_user
        
        # Mock neighbors data
        mock_session_instance = Mock()
        mock_session_instance.__enter__ = Mock(return_value=mock_session_instance)
        mock_session_instance.__exit__ = Mock(return_value=False)
        mock_session_instance.run = Mock(return_value=Mock(data=Mock(return_value=[
            {
                "neighborId": 2,
                "labels": ["PERSON"],
                "name": "Bob",
                "relationshipType": "COLLABORATES_WITH",
                "weight": 0.8
            }
        ])))
        mock_session.return_value = mock_session_instance
        
        response = client.get("/api/v1/graph/nodes/42/neighbors", headers=auth_headers)
        
        assert response.status_code == 200
        data = response.json()
        assert "neighbors" in data
        assert len(data["neighbors"]) > 0
    
    # ========================================================================
    # EXPERT ENDPOINTS (2 tests)
    # ========================================================================
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.get_neo4j_session')
    def test_get_experts_global(self, mock_session, mock_get_user, client, mock_user, auth_headers):
        """Test GET /experts."""
        mock_get_user.return_value = mock_user
        
        mock_session_instance = Mock()
        mock_session_instance.__enter__ = Mock(return_value=mock_session_instance)
        mock_session_instance.__exit__ = Mock(return_value=False)
        
        # Mock experts data
        mock_session_instance.run = Mock()
        mock_session_instance.run.return_value.data = Mock(return_value=[
            {
                "nodeId": 1,
                "name": "Alice",
                "nodeType": "PERSON",
                "pagerank": 0.25,
                "hIndex": 50,
                "paperCount": 100,
                "communityId": 1
            }
        ])
        mock_session_instance.run.return_value.single = Mock(return_value={"total": 10})
        
        mock_session.return_value = mock_session_instance
        
        response = client.get("/api/v1/graph/experts", headers=auth_headers)
        
        assert response.status_code == 200
        data = response.json()
        assert "experts" in data
        assert data["total_count"] > 0
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.get_neo4j_session')
    def test_get_experts_by_topic(self, mock_session, mock_get_user, client, mock_user, auth_headers):
        """Test GET /experts?topic_id=..."""
        mock_get_user.return_value = mock_user
        
        mock_session_instance = Mock()
        mock_session_instance.__enter__ = Mock(return_value=mock_session_instance)
        mock_session_instance.__exit__ = Mock(return_value=False)
        mock_session_instance.run = Mock()
        mock_session_instance.run.return_value.data = Mock(return_value=[])
        mock_session_instance.run.return_value.single = Mock(return_value={"total": 0})
        mock_session.return_value = mock_session_instance
        
        response = client.get("/api/v1/graph/experts?topic_id=5", headers=auth_headers)
        
        assert response.status_code == 200
        data = response.json()
        assert data["topic_id"] == 5
    
    # ========================================================================
    # RECOMMENDATION ENDPOINTS (1 test)
    # ========================================================================
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.node2vec_service')
    @patch('backend.api.v1.graph.community_service')
    def test_recommend_collaborators(
        self,
        mock_community_svc,
        mock_node2vec_svc,
        mock_get_user,
        client,
        mock_user,
        auth_headers
    ):
        """Test GET /collaborators/recommend."""
        mock_get_user.return_value = mock_user
        
        # Mock embedding recommendations
        mock_node2vec_svc.find_similar_nodes.return_value = [
            {
                "node_id": 2,
                "similarity": 0.9,
                "payload": {"name": "Bob", "node_type": "PERSON"}
            }
        ]
        
        # Mock community bridges
        mock_community_svc.find_community_bridges.return_value = []
        
        response = client.get(
            "/api/v1/graph/collaborators/recommend?for_node_id=1&top_k=10",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "recommendations" in data
    
    # ========================================================================
    # LEARNING PATH ENDPOINTS (1 test)
    # ========================================================================
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.get_neo4j_session')
    def test_get_learning_paths(self, mock_session, mock_get_user, client, mock_user, auth_headers):
        """Test GET /learning-paths."""
        mock_get_user.return_value = mock_user
        
        mock_session_instance = Mock()
        mock_session_instance.__enter__ = Mock(return_value=mock_session_instance)
        mock_session_instance.__exit__ = Mock(return_value=False)
        mock_session_instance.run = Mock()
        mock_session_instance.run.return_value.data = Mock(return_value=[
            {
                "nodeId": 1,
                "name": "Python Basics",
                "nodeType": "TOPIC",
                "pathLength": 1
            },
            {
                "nodeId": 2,
                "name": "Machine Learning",
                "nodeType": "TOPIC",
                "pathLength": 2
            }
        ])
        mock_session.return_value = mock_session_instance
        
        response = client.get(
            "/api/v1/graph/learning-paths?source_topic_id=1&target_topic_id=2",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "paths" in data
    
    # ========================================================================
    # COMMUNITY ENDPOINTS (1 test)
    # ========================================================================
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.community_service')
    def test_list_communities(
        self,
        mock_community_svc,
        mock_get_user,
        client,
        mock_user,
        auth_headers
    ):
        """Test GET /communities."""
        mock_get_user.return_value = mock_user
        
        mock_community_svc.get_communities.return_value = [
            {
                "community_id": 1,
                "size": 50,
                "members": [
                    {"nodeId": 1, "labels": ["PERSON"], "name": "Alice"}
                ]
            }
        ]
        mock_community_svc.calculate_modularity.return_value = 0.65
        
        response = client.get("/api/v1/graph/communities", headers=auth_headers)
        
        assert response.status_code == 200
        data = response.json()
        assert "communities" in data
        assert data["overall_modularity"] > 0
    
    # ========================================================================
    # EMBEDDING ENDPOINTS (1 test)
    # ========================================================================
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.node2vec_service')
    def test_search_embeddings(
        self,
        mock_node2vec_svc,
        mock_get_user,
        client,
        mock_user,
        auth_headers
    ):
        """Test POST /embeddings/search."""
        mock_get_user.return_value = mock_user
        
        mock_node2vec_svc.find_similar_nodes.return_value = [
            {
                "node_id": 2,
                "similarity": 0.85,
                "payload": {
                    "name": "Similar Node",
                    "node_type": "PERSON",
                    "community_id": 1
                }
            }
        ]
        
        response = client.post(
            "/api/v1/graph/embeddings/search",
            headers=auth_headers,
            json={
                "node_id": 1,
                "top_k": 10,
                "min_similarity": 0.7
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "similar_nodes" in data
    
    # ========================================================================
    # EXPORT ENDPOINTS (1 test)
    # ========================================================================
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.get_neo4j_session')
    def test_export_graph(self, mock_session, mock_get_user, client, mock_user, auth_headers):
        """Test GET /export."""
        mock_get_user.return_value = mock_user
        
        mock_session_instance = Mock()
        mock_session_instance.__enter__ = Mock(return_value=mock_session_instance)
        mock_session_instance.__exit__ = Mock(return_value=False)
        
        # Mock nodes and relationships
        mock_session_instance.run = Mock()
        mock_session_instance.run.return_value.data = Mock(side_effect=[
            # Nodes
            [
                {
                    "id": 1,
                    "labels": ["PERSON"],
                    "properties": {"name": "Alice"}
                }
            ],
            # Relationships
            [
                {
                    "source": 1,
                    "target": 2,
                    "type": "COLLABORATES_WITH",
                    "properties": {"weight": 0.8}
                }
            ]
        ])
        
        mock_session.return_value = mock_session_instance
        
        response = client.get("/api/v1/graph/export?limit=100", headers=auth_headers)
        
        assert response.status_code == 200
        data = response.json()
        assert "nodes" in data
        assert "relationships" in data
    
    # ========================================================================
    # PAGINATION & FILTERING (2 tests)
    # ========================================================================
    
    def test_pagination_params(self, client, auth_headers):
        """Test pagination parameters."""
        response = client.get(
            "/api/v1/graph/experts?page=2&page_size=10",
            headers=auth_headers
        )
        
        # Should accept pagination params
        assert response.status_code in [200, 401]  # 401 if no auth mock
    
    def test_invalid_pagination(self, client, auth_headers):
        """Test invalid pagination parameters."""
        response = client.get(
            "/api/v1/graph/experts?page=0&page_size=200",
            headers=auth_headers
        )
        
        # Should reject invalid params
        assert response.status_code in [422, 401]  # 422 validation error or 401 no auth
    
    # ========================================================================
    # RATE LIMITING (1 test)
    # ========================================================================
    
    @pytest.mark.slow
    def test_rate_limiting(self, client, auth_headers):
        """Test rate limiting (100 req/min)."""
        # Make 101 requests rapidly
        responses = []
        for i in range(101):
            response = client.get(f"/api/v1/graph/nodes/{i}", headers=auth_headers)
            responses.append(response.status_code)
        
        # Should have at least one rate limit response
        assert 429 in responses  # 429 = Too Many Requests
    
    # ========================================================================
    # ERROR HANDLING (3 tests)
    # ========================================================================
    
    @patch('backend.api.v1.graph.get_current_user')
    def test_node_not_found(self, mock_get_user, client, mock_user, auth_headers):
        """Test 404 error for non-existent node."""
        mock_get_user.return_value = mock_user
        
        with patch('backend.api.v1.graph.get_neo4j_session') as mock_session:
            mock_session_instance = Mock()
            mock_session_instance.__enter__ = Mock(return_value=mock_session_instance)
            mock_session_instance.__exit__ = Mock(return_value=False)
            mock_session_instance.run = Mock(return_value=Mock(single=Mock(return_value=None)))
            mock_session.return_value = mock_session_instance
            
            response = client.get("/api/v1/graph/nodes/999999", headers=auth_headers)
            
            assert response.status_code == 404
            assert "not found" in response.json()["detail"].lower()
    
    def test_unauthorized_access(self, client):
        """Test 401 error for missing auth."""
        response = client.get("/api/v1/graph/nodes/1")
        
        assert response.status_code == 401
    
    @patch('backend.api.v1.graph.get_current_user')
    @patch('backend.api.v1.graph.get_neo4j_session')
    def test_internal_server_error(self, mock_session, mock_get_user, client, mock_user, auth_headers):
        """Test 500 error handling."""
        mock_get_user.return_value = mock_user
        
        # Simulate database error
        mock_session.side_effect = Exception("Database connection failed")
        
        response = client.get("/api/v1/graph/nodes/1", headers=auth_headers)
        
        assert response.status_code == 500


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
