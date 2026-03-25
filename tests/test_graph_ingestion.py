"""
Unit Tests for Graph Ingestion Service
Tests entity ingestion, relationship creation, and error handling.
"""

import pytest
import uuid
from unittest.mock import Mock, patch, MagicMock
from neo4j.exceptions import ServiceUnavailable, TransientError, DatabaseError

from services.graph_ingestion import GraphIngestionService, retry_on_transient_error
from models.graph_models import (
    NodeType,
    RelationshipType,
    GraphNodeCreate,
    GraphRelationshipCreate
)


class TestRetryDecorator:
    """Test retry decorator functionality."""
    
    def test_retry_on_transient_error_success_first_try(self):
        """Test successful execution on first attempt."""
        mock_func = Mock(return_value="success")
        decorated = retry_on_transient_error(max_retries=3)(mock_func)
        
        result = decorated()
        
        assert result == "success"
        assert mock_func.call_count == 1
    
    def test_retry_on_transient_error_success_after_retry(self):
        """Test successful execution after transient errors."""
        mock_func = Mock(side_effect=[
            ServiceUnavailable("Connection lost"),
            TransientError("Temporary issue"),
            "success"
        ])
        decorated = retry_on_transient_error(max_retries=3, backoff_factor=0.1)(mock_func)
        
        result = decorated()
        
        assert result == "success"
        assert mock_func.call_count == 3
    
    def test_retry_on_transient_error_max_retries_exceeded(self):
        """Test max retries exceeded."""
        mock_func = Mock(side_effect=ServiceUnavailable("Always fails"))
        decorated = retry_on_transient_error(max_retries=3, backoff_factor=0.1)(mock_func)
        
        with pytest.raises(ServiceUnavailable):
            decorated()
        
        assert mock_func.call_count == 3
    
    def test_retry_on_database_error_no_retry(self):
        """Test database errors are not retried."""
        mock_func = Mock(side_effect=DatabaseError("Constraint violation"))
        decorated = retry_on_transient_error(max_retries=3)(mock_func)
        
        with pytest.raises(DatabaseError):
            decorated()
        
        assert mock_func.call_count == 1  # No retry


class TestGraphIngestionService:
    """Test Graph Ingestion Service."""
    
    @pytest.fixture
    def service(self):
        """Create service instance."""
        return GraphIngestionService()
    
    @pytest.fixture
    def mock_session(self):
        """Mock Neo4j session."""
        session = MagicMock()
        return session
    
    def test_ingest_entity_person_success(self, service, mock_session):
        """Test successful PERSON node ingestion."""
        entity_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        
        properties = {
            "id": entity_id,
            "user_id": user_id,
            "canonical_name": "Prof. Sarah Chen",
            "email": "sarah@mit.edu",
            "affiliation": "MIT CSAIL",
            "h_index": 35,
            "research_interests": ["ML", "NLP"]
        }
        
        # Mock session
        mock_session.execute_write.return_value = entity_id
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.ingest_entity(
                entity_id=entity_id,
                entity_type="PERSON",
                user_id=user_id,
                properties=properties
            )
        
        assert result == entity_id
        assert mock_session.execute_write.called
    
    def test_ingest_entity_paper_success(self, service, mock_session):
        """Test successful PAPER node ingestion."""
        entity_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        
        properties = {
            "id": entity_id,
            "user_id": user_id,
            "title": "Knowledge Graph Embeddings",
            "authors": ["Sarah Chen", "John Doe"],
            "year": 2023,
            "doi": "10.1234/example",
            "venue": "ICML 2023"
        }
        
        mock_session.execute_write.return_value = entity_id
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.ingest_entity(
                entity_id=entity_id,
                entity_type="PAPER",
                user_id=user_id,
                properties=properties
            )
        
        assert result == entity_id
    
    def test_ingest_entity_with_retry(self, service, mock_session):
        """Test entity ingestion with transient error retry."""
        entity_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        
        properties = {
            "id": entity_id,
            "user_id": user_id,
            "canonical_name": "Test Entity"
        }
        
        # Simulate transient error then success
        mock_session.execute_write.side_effect = [
            ServiceUnavailable("Connection lost"),
            entity_id
        ]
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            with patch('time.sleep'):  # Skip sleep in tests
                result = service.ingest_entity(
                    entity_id=entity_id,
                    entity_type="TOPIC",
                    user_id=user_id,
                    properties=properties
                )
        
        assert result == entity_id
        assert mock_session.execute_write.call_count == 2
    
    def test_ingest_batch_success(self, service, mock_session):
        """Test batch ingestion of multiple entities."""
        user_id = str(uuid.uuid4())
        
        nodes = [
            GraphNodeCreate(
                node_type=NodeType.PERSON,
                properties={
                    "id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "canonical_name": f"Person {i}"
                }
            )
            for i in range(5)
        ]
        
        mock_session.execute_write.return_value = [
            ("success", node.properties["id"], node.node_type.value)
            for node in nodes
        ]
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.ingest_batch(nodes)
        
        assert result.ingested == 5
        assert result.failed == 0
        assert len(result.errors) == 0
    
    def test_ingest_batch_partial_failure(self, service, mock_session):
        """Test batch ingestion with some failures."""
        user_id = str(uuid.uuid4())
        
        nodes = [
            GraphNodeCreate(
                node_type=NodeType.PERSON,
                properties={
                    "id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "canonical_name": f"Person {i}"
                }
            )
            for i in range(5)
        ]
        
        # Simulate mixed results
        mock_session.execute_write.return_value = [
            ("success", nodes[0].properties["id"], "PERSON"),
            ("success", nodes[1].properties["id"], "PERSON"),
            ("error", nodes[2].properties["id"], "PERSON", "Constraint violation"),
            ("success", nodes[3].properties["id"], "PERSON"),
            ("error", nodes[4].properties["id"], "PERSON", "Invalid data"),
        ]
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.ingest_batch(nodes)
        
        assert result.ingested == 3
        assert result.failed == 2
        assert len(result.errors) == 2
    
    def test_ingest_relationship_success(self, service, mock_session):
        """Test successful relationship creation."""
        from_id = str(uuid.uuid4())
        to_id = str(uuid.uuid4())
        
        mock_session.execute_write.return_value = {"rel": "created"}
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.ingest_relationship(
                from_id=from_id,
                to_id=to_id,
                rel_type="AUTHORED",
                properties={"position": 1}
            )
        
        assert result is True
        assert mock_session.execute_write.called
    
    def test_ingest_batch_relationships_success(self, service, mock_session):
        """Test batch relationship creation."""
        relationships = [
            GraphRelationshipCreate(
                from_id=str(uuid.uuid4()),
                to_id=str(uuid.uuid4()),
                relationship_type=RelationshipType.AUTHORED,
                properties={"position": i}
            )
            for i in range(3)
        ]
        
        mock_session.execute_write.return_value = {"rel": "created"}
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.ingest_batch_relationships(relationships)
        
        assert result.ingested == 3
        assert result.failed == 0
    
    def test_map_type_specific_properties_person(self, service):
        """Test property mapping for PERSON nodes."""
        properties = {
            "canonical_name": "Dr. Jane Doe",
            "email": "jane@example.com",
            "affiliation": "Stanford",
            "h_index": 42,
            "extra_field": "should be preserved"
        }
        
        mapped = service._map_type_specific_properties("PERSON", properties)
        
        assert mapped["canonical_name"] == "Dr. Jane Doe"
        assert mapped["email"] == "jane@example.com"
        assert mapped["affiliation"] == "Stanford"
        assert mapped["h_index"] == 42
        assert "extra_field" not in mapped  # Extra fields filtered
    
    def test_map_type_specific_properties_paper(self, service):
        """Test property mapping for PAPER nodes."""
        properties = {
            "title": "Test Paper",
            "authors": ["Author 1", "Author 2"],
            "year": 2023,
            "doi": "10.1234/test",
            "abstract": "This is a test abstract."
        }
        
        mapped = service._map_type_specific_properties("PAPER", properties)
        
        assert mapped["title"] == "Test Paper"
        assert mapped["canonical_name"] == "Test Paper"  # Should copy title
        assert len(mapped["authors"]) == 2
        assert mapped["year"] == 2023
        assert mapped["doi"] == "10.1234/test"
    
    def test_get_node_success(self, service, mock_session):
        """Test retrieving a node."""
        node_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        
        mock_result = {
            "node": {
                "id": node_id,
                "type": "PERSON",
                "properties": {"canonical_name": "Test Person"}
            }
        }
        
        mock_session.run.return_value.single.return_value = mock_result
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.get_node(node_id, user_id)
        
        assert result is not None
        assert result["id"] == node_id
        assert result["type"] == "PERSON"
    
    def test_get_node_not_found(self, service, mock_session):
        """Test retrieving non-existent node."""
        node_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        
        mock_session.run.return_value.single.return_value = None
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.get_node(node_id, user_id)
        
        assert result is None
    
    def test_delete_node_success(self, service, mock_session):
        """Test deleting a node."""
        node_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        
        mock_session.execute_write.return_value = 1  # 1 node deleted
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.delete_node(node_id, user_id)
        
        assert result is True
    
    def test_delete_node_not_found(self, service, mock_session):
        """Test deleting non-existent node."""
        node_id = str(uuid.uuid4())
        user_id = str(uuid.uuid4())
        
        mock_session.execute_write.return_value = 0  # 0 nodes deleted
        
        with patch('backend.services.graph_ingestion.get_neo4j_session', return_value=mock_session):
            result = service.delete_node(node_id, user_id)
        
        assert result is False


class TestPropertyMapping:
    """Test property mapping for different node types."""
    
    @pytest.fixture
    def service(self):
        return GraphIngestionService()
    
    def test_all_node_types_have_canonical_name(self, service):
        """Test that all node types get canonical_name field."""
        node_types = ["PERSON", "PAPER", "TOPIC", "PROJECT", "DATASET", "INSTITUTION", "TOOL", "VENUE"]
        
        for node_type in node_types:
            properties = {"canonical_name": f"Test {node_type}"}
            if node_type == "PAPER":
                properties["title"] = f"Test {node_type}"
            elif node_type in ["PROJECT", "DATASET", "INSTITUTION", "TOOL", "VENUE"]:
                properties["name"] = f"Test {node_type}"
            
            mapped = service._map_type_specific_properties(node_type, properties)
            
            assert "canonical_name" in mapped, f"{node_type} should have canonical_name"
            assert mapped["canonical_name"] == f"Test {node_type}"
    
    def test_none_values_filtered(self, service):
        """Test that None values are filtered out."""
        properties = {
            "canonical_name": "Test",
            "email": None,
            "affiliation": "MIT",
            "h_index": None
        }
        
        mapped = service._map_type_specific_properties("PERSON", properties)
        
        assert "email" not in mapped
        assert "h_index" not in mapped
        assert "affiliation" in mapped


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
