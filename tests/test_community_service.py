"""
Unit Tests for Community Detection Service
Tests Louvain algorithm, community queries, and statistics.
"""

import pytest
from unittest.mock import Mock, patch

from services.community_service import CommunityService, community_service


class TestCommunityService:
    """Test community detection service functionality."""
    
    @pytest.fixture
    def service(self):
        """Create community service instance."""
        return CommunityService()
    
    @pytest.fixture
    def mock_session(self):
        """Create mock Neo4j session."""
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        return session
    
    def test_initialization(self, service):
        """Test service initialization."""
        assert service is not None
        assert service.driver is not None
    
    @patch('backend.services.community_service.get_neo4j_session')
    @patch.object(CommunityService, '_project_graph')
    @patch.object(CommunityService, '_drop_graph')
    def test_detect_communities_success(
        self,
        mock_drop,
        mock_project,
        mock_get_session,
        service,
        mock_session
    ):
        """Test successful community detection."""
        mock_get_session.return_value = mock_session
        mock_project.return_value = "test_graph"
        
        # Mock Louvain result
        mock_result = Mock()
        mock_result.get = Mock(side_effect=lambda key, default=None: {
            "communityCount": 5,
            "modularity": 0.45,
            "nodePropertiesWritten": 100
        }.get(key, default))
        mock_result.data = Mock(return_value={
            "communityCount": 5,
            "modularity": 0.45
        })
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.detect_communities(
            user_id="user-123",
            store_results=True
        )
        
        assert result["status"] == "success"
        assert result["num_communities"] == 5
        assert result["modularity"] == 0.45
        assert "execution_time_sec" in result
        
        mock_project.assert_called_once()
        mock_drop.assert_called_once_with("test_graph")
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_detect_communities_with_existing_graph(
        self,
        mock_get_session,
        service,
        mock_session
    ):
        """Test community detection with pre-projected graph."""
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.get = Mock(side_effect=lambda key, default=None: {
            "communityCount": 3,
            "modularity": 0.35,
            "nodePropertiesWritten": 50
        }.get(key, default))
        mock_result.data = Mock(return_value={})
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.detect_communities(
            user_id="user-123",
            graph_name="existing_graph",
            store_results=True
        )
        
        assert result["status"] == "success"
        assert result["num_communities"] == 3
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_project_graph(self, mock_get_session, service, mock_session):
        """Test graph projection for community detection."""
        mock_get_session.return_value = mock_session
        
        # Mock projection result
        mock_result = Mock()
        mock_result.__getitem__ = Mock(side_effect=lambda key: {
            "graphName": "test_graph",
            "nodeCount": 100,
            "relationshipCount": 300
        }[key])
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        graph_name = service._project_graph(user_id="user-123")
        
        assert "community_user_user-123_" in graph_name
        assert mock_session.run.call_count == 2  # Drop + Project
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_drop_graph(self, mock_get_session, service, mock_session):
        """Test graph deletion."""
        mock_get_session.return_value = mock_session
        mock_session.run = Mock()
        
        service._drop_graph("test_graph")
        
        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        assert "gds.graph.drop" in call_args[0][0]
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_get_communities(self, mock_get_session, service, mock_session):
        """Test retrieving all communities."""
        mock_get_session.return_value = mock_session
        
        # Mock community data
        mock_session.run = Mock()
        mock_session.run.return_value.data = Mock(return_value=[
            {
                "communityId": 1,
                "size": 25,
                "members": [
                    {"nodeId": 1, "labels": ["PERSON"], "name": "Alice"},
                    {"nodeId": 2, "labels": ["PERSON"], "name": "Bob"}
                ]
            },
            {
                "communityId": 2,
                "size": 15,
                "members": [
                    {"nodeId": 3, "labels": ["PAPER"], "name": "Paper1"}
                ]
            },
            {
                "communityId": 3,
                "size": 2,  # Small community
                "members": []
            }
        ])
        
        communities = service.get_communities(
            user_id="user-123",
            min_size=5  # Filter small communities
        )
        
        assert len(communities) == 2  # Excludes size=2 community
        assert communities[0]["community_id"] == 1
        assert communities[0]["size"] == 25
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_get_community_by_id(self, mock_get_session, service, mock_session):
        """Test retrieving specific community."""
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.__getitem__ = Mock(side_effect=lambda key: {
            "communityId": 1,
            "size": 10,
            "members": [
                {"nodeId": 1, "labels": ["PERSON"], "name": "Alice"}
            ]
        }[key])
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        community = service.get_community_by_id(
            user_id="user-123",
            community_id=1
        )
        
        assert community is not None
        assert community["community_id"] == 1
        assert community["size"] == 10
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_get_community_by_id_not_found(self, mock_get_session, service, mock_session):
        """Test retrieving non-existent community."""
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.__getitem__ = Mock(side_effect=lambda key: {
            "size": 0
        }.get(key, None))
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        community = service.get_community_by_id(
            user_id="user-123",
            community_id=999
        )
        
        assert community is None
    
    @patch('backend.services.community_service.get_neo4j_session')
    @patch.object(CommunityService, '_project_graph')
    @patch.object(CommunityService, '_drop_graph')
    def test_calculate_modularity(
        self,
        mock_drop,
        mock_project,
        mock_get_session,
        service,
        mock_session
    ):
        """Test modularity calculation."""
        mock_get_session.return_value = mock_session
        mock_project.return_value = "test_graph"
        
        mock_result = Mock()
        mock_result.__getitem__ = Mock(return_value=0.42)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        modularity = service.calculate_modularity(user_id="user-123")
        
        assert modularity == 0.42
        assert 0 <= modularity <= 1.0  # Valid range
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_get_community_statistics(self, mock_get_session, service, mock_session):
        """Test community statistics calculation."""
        mock_get_session.return_value = mock_session
        
        # Mock size distribution query
        size_result = Mock()
        size_result.__getitem__ = Mock(side_effect=lambda key: {
            "numCommunities": 5,
            "avgSize": 20.0,
            "minSize": 5,
            "maxSize": 50,
            "stddevSize": 15.0,
            "sizes": [5, 10, 20, 30, 50]
        }[key])
        
        # Mock total nodes query
        total_result = Mock()
        total_result.__getitem__ = Mock(return_value=115)
        
        # Mock largest communities query
        largest_result = []
        
        mock_session.run = Mock()
        mock_session.run.return_value.single = Mock(side_effect=[size_result, total_result])
        mock_session.run.return_value.data = Mock(return_value=largest_result)
        
        stats = service.get_community_statistics(user_id="user-123")
        
        assert stats["num_communities"] == 5
        assert stats["total_nodes"] == 115
        assert stats["avg_community_size"] == 20.0
        assert stats["min_community_size"] == 5
        assert stats["max_community_size"] == 50
        assert "size_distribution" in stats
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_get_community_statistics_empty(self, mock_get_session, service, mock_session):
        """Test statistics with no communities."""
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.__getitem__ = Mock(side_effect=lambda key: {
            "numCommunities": 0
        }.get(key, None))
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        stats = service.get_community_statistics(user_id="user-123")
        
        assert stats["num_communities"] == 0
        assert stats["total_nodes"] == 0
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_get_node_community(self, mock_get_session, service, mock_session):
        """Test getting community for specific node."""
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.__getitem__ = Mock(return_value=5)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        community_id = service.get_node_community(
            user_id="user-123",
            node_id=42
        )
        
        assert community_id == 5
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_get_node_community_not_found(self, mock_get_session, service, mock_session):
        """Test getting community for non-existent node."""
        mock_get_session.return_value = mock_session
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=None)))
        
        community_id = service.get_node_community(
            user_id="user-123",
            node_id=999
        )
        
        assert community_id is None
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_find_community_bridges(self, mock_get_session, service, mock_session):
        """Test finding bridge nodes between communities."""
        mock_get_session.return_value = mock_session
        
        mock_session.run = Mock()
        mock_session.run.return_value.data = Mock(return_value=[
            {
                "nodeId": 1,
                "nodeName": "Alice",
                "nodeType": "PERSON",
                "numTargetCommunities": 3,
                "totalExternalConnections": 15,
                "targets": [
                    {"communityId": 2, "connections": 7},
                    {"communityId": 3, "connections": 5},
                    {"communityId": 4, "connections": 3}
                ]
            },
            {
                "nodeId": 2,
                "nodeName": "Bob",
                "nodeType": "PERSON",
                "numTargetCommunities": 2,
                "totalExternalConnections": 8,
                "targets": [
                    {"communityId": 3, "connections": 5},
                    {"communityId": 5, "connections": 3}
                ]
            }
        ])
        
        bridges = service.find_community_bridges(
            user_id="user-123",
            community_id=1,
            top_k=10
        )
        
        assert len(bridges) == 2
        assert bridges[0]["node_id"] == 1
        assert bridges[0]["num_target_communities"] == 3
        assert bridges[0]["total_external_connections"] == 15
    
    @patch('backend.services.community_service.get_neo4j_session')
    def test_error_handling(self, mock_get_session, service):
        """Test error handling in community detection."""
        mock_session = Mock()
        mock_session.__enter__ = Mock(side_effect=Exception("Neo4j error"))
        mock_get_session.return_value = mock_session
        
        result = service.detect_communities(
            user_id="user-123",
            graph_name="test_graph"
        )
        
        assert result["status"] == "error"
        assert "error" in result
        assert "execution_time_sec" in result


class TestGlobalInstance:
    """Test global community service instance."""
    
    def test_global_instance_exists(self):
        """Test that global instance is accessible."""
        assert community_service is not None
        assert isinstance(community_service, CommunityService)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
