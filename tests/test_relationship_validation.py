"""
Unit Tests for Relationship Validation and Ingestion
Tests relationship type validation, weight computation, confidence scoring, and batch ingestion.
"""

import pytest
import uuid
from unittest.mock import Mock, patch, MagicMock

from services.relationship_validator import RelationshipValidator, relationship_validator
from models.graph_models import NodeType, RelationshipType


class TestRelationshipValidator:
    """Test relationship validator functionality."""
    
    @pytest.fixture
    def validator(self):
        """Create validator instance."""
        return RelationshipValidator()
    
    def test_validate_authored_relationship_valid(self, validator):
        """Test valid AUTHORED relationship (PERSON -> PAPER)."""
        is_valid, error = validator.validate_relationship(
            "PERSON", "PAPER", "AUTHORED"
        )
        
        assert is_valid is True
        assert error is None
    
    def test_validate_authored_relationship_invalid(self, validator):
        """Test invalid AUTHORED relationship (PAPER -> PERSON)."""
        is_valid, error = validator.validate_relationship(
            "PAPER", "PERSON", "AUTHORED"
        )
        
        assert is_valid is False
        assert "Invalid node types" in error
    
    def test_validate_cites_relationship_valid(self, validator):
        """Test valid CITES relationship (PAPER -> PAPER)."""
        is_valid, error = validator.validate_relationship(
            "PAPER", "PAPER", "CITES"
        )
        
        assert is_valid is True
        assert error is None
    
    def test_validate_uses_relationship_multiple_valid(self, validator):
        """Test USES relationship with multiple valid combinations."""
        valid_combos = [
            ("PAPER", "DATASET"),
            ("PAPER", "TOOL"),
            ("PROJECT", "DATASET"),
            ("PROJECT", "TOOL")
        ]
        
        for from_type, to_type in valid_combos:
            is_valid, error = validator.validate_relationship(
                from_type, to_type, "USES"
            )
            assert is_valid is True, f"{from_type} -> {to_type} should be valid"
    
    def test_validate_invalid_relationship_type(self, validator):
        """Test invalid relationship type."""
        is_valid, error = validator.validate_relationship(
            "PERSON", "PAPER", "INVALID_TYPE"
        )
        
        assert is_valid is False
        assert "Invalid type" in error
    
    def test_validate_invalid_node_type(self, validator):
        """Test invalid node type."""
        is_valid, error = validator.validate_relationship(
            "INVALID_NODE", "PAPER", "AUTHORED"
        )
        
        assert is_valid is False
        assert "Invalid type" in error
    
    def test_compute_weight_authored_first_author(self, validator):
        """Test weight computation for first author."""
        weight = validator.compute_weight(
            "AUTHORED",
            {"position": 1}
        )
        
        assert weight == 2.0
    
    def test_compute_weight_authored_middle_author(self, validator):
        """Test weight computation for middle author."""
        weight = validator.compute_weight(
            "AUTHORED",
            {"position": 2}
        )
        
        assert weight == 1.5
    
    def test_compute_weight_authored_last_author(self, validator):
        """Test weight computation for last author."""
        weight = validator.compute_weight(
            "AUTHORED",
            {"position": 10}
        )
        
        assert weight == 1.0
    
    def test_compute_weight_collaboration(self, validator):
        """Test weight for collaboration based on num_papers."""
        weight = validator.compute_weight(
            "COLLABORATES_WITH",
            {"num_papers": 10}
        )
        
        assert weight == 5.0  # 10 * 0.5
    
    def test_compute_weight_citation(self, validator):
        """Test weight for citations."""
        weight = validator.compute_weight(
            "CITES",
            {"num_citations": 15}
        )
        
        # Should be capped at 3.0
        assert weight == 3.0
    
    def test_compute_weight_works_on(self, validator):
        """Test weight for WORKS_ON relationship."""
        weight = validator.compute_weight(
            "WORKS_ON",
            {"expertise_level": 0.8, "years": 5}
        )
        
        # 0.8 * min(3.0, 1.0 + 5*0.2) = 0.8 * 2.0 = 1.6
        assert weight == 1.6
    
    def test_compute_weight_uses_primary(self, validator):
        """Test weight for primary tool usage."""
        weight = validator.compute_weight(
            "USES",
            {"primary": True}
        )
        
        assert weight == 2.0
    
    def test_compute_weight_on_topic(self, validator):
        """Test weight for ON_TOPIC relationship."""
        weight = validator.compute_weight(
            "ON_TOPIC",
            {"relevance_score": 0.9}
        )
        
        assert weight == 1.8  # 0.9 * 2.0
    
    def test_compute_confidence_single_source_activity(self, validator):
        """Test confidence from activity source (highest)."""
        confidence = validator.compute_confidence(["activity"])
        
        assert confidence == 1.0
    
    def test_compute_confidence_single_source_api(self, validator):
        """Test confidence from API source."""
        confidence = validator.compute_confidence(["api"])
        
        assert confidence == 0.95
    
    def test_compute_confidence_single_source_inference(self, validator):
        """Test confidence from inference."""
        confidence = validator.compute_confidence(["inference"])
        
        assert confidence == 0.7
    
    def test_compute_confidence_multiple_sources(self, validator):
        """Test confidence boost for multiple sources."""
        confidence = validator.compute_confidence(["api", "inference"])
        
        # Max(0.95, 0.7) * 1.1 = 0.95 * 1.1 = 1.045, capped at 1.0
        assert confidence == 1.0
    
    def test_compute_confidence_user_confirmed_boost(self, validator):
        """Test confidence boost for user confirmation."""
        confidence = validator.compute_confidence(
            ["inference"],
            {"user_confirmed": True}
        )
        
        # 0.7 * 1.2 = 0.84
        assert confidence == 0.84
    
    def test_compute_confidence_inferred_penalty(self, validator):
        """Test confidence penalty for unvalidated inference."""
        confidence = validator.compute_confidence(
            ["inference"],
            {"inferred": True, "validated": False}
        )
        
        # 0.7 * 0.8 = 0.56
        assert confidence == 0.56
    
    def test_add_temporal_properties_new(self, validator):
        """Test adding temporal properties to new relationship."""
        props = {}
        enriched = validator.add_temporal_properties(props, update_existing=False)
        
        assert "first_seen_at" in enriched
        assert "last_updated_at" in enriched
    
    def test_add_temporal_properties_update(self, validator):
        """Test updating temporal properties."""
        props = {"first_seen_at": "2023-01-01T00:00:00"}
        enriched = validator.add_temporal_properties(props, update_existing=True)
        
        # first_seen_at should remain unchanged
        assert enriched["first_seen_at"] == "2023-01-01T00:00:00"
        # last_updated_at should be updated
        assert enriched["last_updated_at"] != "2023-01-01T00:00:00"
    
    def test_validate_required_properties_authored_valid(self, validator):
        """Test required properties for AUTHORED."""
        is_valid, error = validator.validate_required_properties(
            "AUTHORED",
            {"position": 1}
        )
        
        assert is_valid is True
        assert error is None
    
    def test_validate_required_properties_authored_missing(self, validator):
        """Test missing required properties for AUTHORED."""
        is_valid, error = validator.validate_required_properties(
            "AUTHORED",
            {}
        )
        
        assert is_valid is False
        assert "position" in error
    
    def test_validate_required_properties_collaboration_valid(self, validator):
        """Test required properties for COLLABORATES_WITH."""
        is_valid, error = validator.validate_required_properties(
            "COLLABORATES_WITH",
            {"num_papers": 5}
        )
        
        assert is_valid is True
    
    def test_validate_required_properties_collaboration_missing(self, validator):
        """Test missing required properties for COLLABORATES_WITH."""
        is_valid, error = validator.validate_required_properties(
            "COLLABORATES_WITH",
            {}
        )
        
        assert is_valid is False
        assert "num_papers" in error
    
    def test_enrich_relationship_properties_complete(self, validator):
        """Test complete property enrichment."""
        props = {
            "position": 1,
            "source": ["activity", "api"]
        }
        
        enriched = validator.enrich_relationship_properties(
            "PERSON", "PAPER", "AUTHORED", props
        )
        
        # Should have all enriched fields
        assert "weight" in enriched
        assert "confidence" in enriched
        assert "first_seen_at" in enriched
        assert "last_updated_at" in enriched
        assert isinstance(enriched["source"], list)
        
        # Weight should be 2.0 for first author
        assert enriched["weight"] == 2.0
        # Confidence should be 1.0 (activity source with  multi-source boost)
        assert enriched["confidence"] == 1.0
    
    def test_enrich_relationship_properties_defaults(self, validator):
        """Test enrichment with minimal properties."""
        props = {}
        
        enriched = validator.enrich_relationship_properties(
            "PAPER", "PAPER", "CITES", props
        )
        
        # Should have defaults
        assert enriched["weight"] >= 1.0
        assert 0.0 <= enriched["confidence"] <= 1.0
        assert enriched["source"] == ["default"]
    
    def test_source_list_conversion(self, validator):
        """Test source is converted to list."""
        # String source
        props = {"source": "api"}
        enriched = validator.enrich_relationship_properties(
            "PERSON", "PAPER", "AUTHORED", props
        )
        assert isinstance(enriched["source"], list)
        assert "api" in enriched["source"]


class TestRelationshipTypes:
    """Test all 12 relationship types are supported."""
    
    @pytest.fixture
    def validator(self):
        return RelationshipValidator()
    
    def test_all_relationship_types_defined(self, validator):
        """Test that all relationship types have validation rules."""
        expected_types = [
            "AUTHORED",
            "COLLABORATES_WITH",
            "WORKS_ON",
            "CONTRIBUTES_TO",
            "AFFILIATED_WITH",
            "CITES",
            "USES",
            "ON_TOPIC",
            "PUBLISHED_AT",
            "RELATED_TO",
            "DEPENDS_ON"
        ]
        
        for rel_type in expected_types:
            assert RelationshipType(rel_type) in validator.VALID_RELATIONSHIPS
    
    def test_all_relationship_types_have_weight_logic(self, validator):
        """Test that weight computation handles all types."""
        test_cases = {
            "AUTHORED": {"position": 1},
            "COLLABORATES_WITH": {"num_papers": 3},
            "WORKS_ON": {"expertise_level": 0.7, "years": 2},
            "CONTRIBUTES_TO": {},
            "AFFILIATED_WITH": {},
            "CITES": {"num_citations": 5},
            "USES": {"primary": True},
            "ON_TOPIC": {"relevance_score": 0.8},
            "PUBLISHED_AT": {},
            "RELATED_TO": {"similarity_score": 0.9},
            "DEPENDS_ON": {}
        }
        
        for rel_type, props in test_cases.items():
            weight = validator.compute_weight(rel_type, props)
            assert weight > 0, f"{rel_type} should have positive weight"


class TestGlobalValidator:
    """Test  global validator instance."""
    
    def test_global_validator_exists(self):
        """Test that global validator is accessible."""
        assert relationship_validator is not None
        assert isinstance(relationship_validator, RelationshipValidator)
    
    def test_global_validator_functional(self):
        """Test that global validator works."""
        is_valid, error = relationship_validator.validate_relationship(
            "PERSON", "PAPER", "AUTHORED"
        )
        assert is_valid is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
