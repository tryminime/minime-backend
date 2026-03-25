"""
Unit Tests for Centrality Service
Tests centrality computation, graph projection, and orchestration.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from services.centrality_service import CentralityService, centrality_service


class TestCentralityService:
    """Test centrality service functionality."""
    
    @pytest.fixture
    def service(self):
        """Create centrality service instance."""
        return CentralityService()
    
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
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    def test_project_user_graph_success(self, mock_get_session, service, mock_session):
        """Test successful graph projection."""
        # Mock the session
        mock_get_session.return_value = mock_session
        
        # Mock the query result
        mock_result = Mock()
        mock_result.__getitem__ = Mock(side_effect=lambda key: {
            "graphName": "test_graph",
            "nodeCount": 100,
            "relationshipCount": 250
        }[key])
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        # Project graph
        graph_name = service._project_user_graph(
            user_id="user-123",
            graph_name="test_graph"
        )
        
        assert graph_name == "test_graph"
        assert mock_session.run.call_count == 2  # Drop (if exists) + Project
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    def test_project_user_graph_with_filters(self, mock_get_session, service, mock_session):
        """Test graph projection with node/relationship filters."""
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.__getitem__ = Mock(side_effect=lambda key: {
            "graphName": "filtered_graph",
            "nodeCount": 50,
            "relationshipCount": 100
        }[key])
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        graph_name = service._project_user_graph(
            user_id="user-123",
            graph_name="filtered_graph",
            node_types=["PERSON", "PAPER"],
            relationship_types=["AUTHORED", "CITES"]
        )
        
        assert graph_name == "filtered_graph"
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    def test_drop_graph(self, mock_get_session, service, mock_session):
        """Test graph deletion."""
        mock_get_session.return_value = mock_session
        mock_session.run = Mock()
        
        service._drop_graph("test_graph")
        
        mock_session.run.assert_called_once()
        call_args = mock_session.run.call_args
        assert "gds.graph.drop" in call_args[0][0]
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_degree_centrality_success(
        self,
        mock_drop,
        mock_project,
        mock_get_session,
        service,
        mock_session
    ):
        """Test degree centrality computation."""
        mock_get_session.return_value = mock_session
        mock_project.return_value = "test_graph"
        
        # Mock GDS result
        mock_result = Mock()
        mock_result.data = Mock(return_value={
            "nodePropertiesWritten": 100
        })
        mock_result.get = Mock(return_value=100)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.compute_degree_centrality(
            user_id="user-123",
            store_results=True
        )
        
        assert result["status"] == "success"
        assert result["metric"] == "degree_centrality"
        assert "execution_time_sec" in result
        
        mock_project.assert_called_once()
        mock_drop.assert_called_once_with("test_graph")
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    def test_compute_degree_centrality_with_existing_graph(
        self,
        mock_get_session,
        service,
        mock_session
    ):
        """Test degree centrality with pre-projected graph."""
        mock_get_session.return_value = mock_session
        
        mock_result = Mock()
        mock_result.data = Mock(return_value={"nodePropertiesWritten": 100})
        mock_result.get = Mock(return_value=100)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.compute_degree_centrality(
            user_id="user-123",
            graph_name="existing_graph",
            store_results=True
        )
        
        assert result["status"] == "success"
        # Should not drop graph since we didn't create it
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_betweenness_centrality(
        self,
        mock_drop,
        mock_project,
        mock_get_session,
        service,
        mock_session
    ):
        """Test betweenness centrality computation."""
        mock_get_session.return_value = mock_session
        mock_project.return_value = "test_graph"
        
        mock_result = Mock()
        mock_result.data = Mock(return_value={"nodePropertiesWritten": 100})
        mock_result.get = Mock(return_value=100)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.compute_betweenness_centrality(
            user_id="user-123",
            store_results=True
        )
        
        assert result["status"] == "success"
        assert result["metric"] == "betweenness_centrality"
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_closeness_centrality(
        self,
        mock_drop,
        mock_project,
        mock_get_session,
        service,
        mock_session
    ):
        """Test closeness centrality computation."""
        mock_get_session.return_value = mock_session
        mock_project.return_value = "test_graph"
        
        mock_result = Mock()
        mock_result.data = Mock(return_value={"nodePropertiesWritten": 100})
        mock_result.get = Mock(return_value=100)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.compute_closeness_centrality(
            user_id="user-123",
            store_results=True
        )
        
        assert result["status"] == "success"
        assert result["metric"] == "closeness_centrality"
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_eigenvector_centrality(
        self,
        mock_drop,
        mock_project,
        mock_get_session,
        service,
        mock_session
    ):
        """Test eigenvector centrality computation."""
        mock_get_session.return_value = mock_session
        mock_project.return_value = "test_graph"
        
        mock_result = Mock()
        mock_result.data = Mock(return_value={"nodePropertiesWritten": 100})
        mock_result.get = Mock(return_value=100)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.compute_eigenvector_centrality(
            user_id="user-123",
            store_results=True,
            max_iterations=20
        )
        
        assert result["status"] == "success"
        assert result["metric"] == "eigenvector_centrality"
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_pagerank_weighted(
        self,
        mock_drop,
        mock_project,
        mock_get_session,
        service,
        mock_session
    ):
        """Test PageRank with weights."""
        mock_get_session.return_value = mock_session
        mock_project.return_value = "test_graph"
        
        mock_result = Mock()
        mock_result.data = Mock(return_value={"nodePropertiesWritten": 100})
        mock_result.get = Mock(return_value=100)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.compute_pagerank(
            user_id="user-123",
            store_results=True,
            use_weights=True
        )
        
        assert result["status"] == "success"
        assert result["metric"] == "pagerank"
        assert result["weighted"] is True
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_pagerank_unweighted(
        self,
        mock_drop,
        mock_project,
        mock_get_session,
        service,
        mock_session
    ):
        """Test PageRank without weights."""
        mock_get_session.return_value = mock_session
        mock_project.return_value = "test_graph"
        
        mock_result = Mock()
        mock_result.data = Mock(return_value={"nodePropertiesWritten": 100})
        mock_result.get = Mock(return_value=100)
        
        mock_session.run = Mock(return_value=Mock(single=Mock(return_value=mock_result)))
        
        result = service.compute_pagerank(
            user_id="user-123",
            store_results=True,
            use_weights=False
        )
        
        assert result["status"] == "success"
        assert result["weighted"] is False
    
    @patch.object(CentralityService, 'compute_degree_centrality')
    @patch.object(CentralityService, 'compute_betweenness_centrality')
    @patch.object(CentralityService, 'compute_closeness_centrality')
    @patch.object(CentralityService, 'compute_eigenvector_centrality')
    @patch.object(CentralityService, 'compute_pagerank')
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_all_metrics_success(
        self,
        mock_drop,
        mock_project,
        mock_pagerank,
        mock_eigenvector,
        mock_closeness,
        mock_betweenness,
        mock_degree,
        service
    ):
        """Test computing all metrics successfully."""
        mock_project.return_value = "test_graph"
        
        # Mock all metric computations to succeed
        for mock_fn in [mock_degree, mock_betweenness, mock_closeness, mock_eigenvector, mock_pagerank]:
            mock_fn.return_value = {
                "status": "success",
                "execution_time_sec": 1.0
            }
        
        result = service.compute_all_metrics(
            user_id="user-123",
            store_results=True
        )
        
        assert len(result["metrics_computed"]) == 5
        assert len(result["metrics_failed"]) == 0
        assert "total_execution_time_sec" in result
        
        mock_project.assert_called_once()
        mock_drop.assert_called_once()
    
    @patch.object(CentralityService, 'compute_degree_centrality')
    @patch.object(CentralityService, 'compute_betweenness_centrality')
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_all_metrics_partial_failure(
        self,
        mock_drop,
        mock_project,
        mock_betweenness,
        mock_degree,
        service
    ):
        """Test orchestrator with partial failures."""
        mock_project.return_value = "test_graph"
        
        # Degree succeeds
        mock_degree.return_value = {
            "status": "success",
            "execution_time_sec": 1.0
        }
        
        # Betweenness fails
        mock_betweenness.return_value = {
            "status": "error",
            "execution_time_sec": 0.5,
            "error": "Computation failed"
        }
        
        result = service.compute_all_metrics(
            user_id="user-123",
            metrics=["degree", "betweenness"],
            store_results=True
        )
        
        assert len(result["metrics_computed"]) == 1
        assert len(result["metrics_failed"]) == 1
        assert "degree" in result["metrics_computed"]
        assert "betweenness" in result["metrics_failed"]
    
    @patch.object(CentralityService, '_project_user_graph')
    @patch.object(CentralityService, '_drop_graph')
    def test_compute_all_metrics_selective(
        self,
        mock_drop,
        mock_project,
        service
    ):
        """Test computing only selected metrics."""
        mock_project.return_value = "test_graph"
        
        with patch.object(service, 'compute_degree_centrality') as mock_degree, \
             patch.object(service, 'compute_pagerank') as mock_pagerank:
            
            mock_degree.return_value = {"status": "success", "execution_time_sec": 1.0}
            mock_pagerank.return_value = {"status": "success", "execution_time_sec": 1.5}
            
            result = service.compute_all_metrics(
                user_id="user-123",
                metrics=["degree", "pagerank"],  # Only 2 metrics
                store_results=True
            )
            
            assert len(result["metrics_requested"]) == 2
            mock_degree.assert_called_once()
            mock_pagerank.assert_called_once()
    
    @patch('backend.services.centrality_service.get_neo4j_session')
    @patch.object(CentralityService, '_project_user_graph')
    def test_error_handling(
        self,
        mock_project,
        mock_get_session,
        service
    ):
        """Test error handling in computation."""
        mock_project.return_value = "test_graph"
        
        # Mock session to raise exception
        mock_session = Mock()
        mock_session.__enter__ = Mock(side_effect=Exception("Neo4j error"))
        mock_get_session.return_value = mock_session
        
        result = service.compute_degree_centrality(
            user_id="user-123",
            graph_name="test_graph"
        )
        
        assert result["status"] == "error"
        assert "error" in result
        assert "execution_time_sec" in result


class TestGlobalInstance:
    """Test global centrality service instance."""
    
    def test_global_instance_exists(self):
        """Test that global instance is accessible."""
        assert centrality_service is not None
        assert isinstance(centrality_service, CentralityService)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
