"""
Unit Tests for Relationship Inference Engine
Tests co-occurrence detection, weight computation, citation inference, and confidence scoring.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from services.relationship_inference import (
    RelationshipInferenceService,
    ENTITY_PAIR_TO_RELATIONSHIP,
    relationship_inference_service
)
from models.graph_models import NodeType, RelationshipType


class TestRelationshipInferenceService:
    """Test relationship inference service."""
    
    @pytest.fixture
    def service(self):
        """Create inference service instance."""
        return RelationshipInferenceService(
            min_confidence=0.5,
            recency_decay_days=365,
            min_co_occurrences=2
        )
    
    def test_initialization(self, service):
        """Test service initialization with parameters."""
        assert service.min_confidence == 0.5
        assert service.recency_decay_days == 365
        assert service.min_co_occurrences == 2
    
    def test_compute_weight_frequency(self, service):
        """Test weight computation with frequency factor."""
        # 1 occurrence
        weight1 = service._compute_weight(
            co_occurrence_count=1,
            timestamp=datetime.utcnow(),
            context_strength=1.0
        )
        
        # 10 occurrences
        weight10 = service._compute_weight(
            co_occurrence_count=10,
            timestamp=datetime.utcnow(),
            context_strength=1.0
        )
        
        # More occurrences should have higher weight
        assert weight10 > weight1
    
    def test_compute_weight_recency_decay(self, service):
        """Test weight computation with recency decay."""
        # Recent (today)
        weight_recent = service._compute_weight(
            co_occurrence_count=5,
            timestamp=datetime.utcnow(),
            context_strength=1.0
        )
        
        # Old (1 year ago)
        weight_old = service._compute_weight(
            co_occurrence_count=5,
            timestamp=datetime.utcnow() - timedelta(days=365),
            context_strength=1.0
        )
        
        # Recent should have higher weight
        assert weight_recent > weight_old
        
        # Old weight should be approximately half (half-life = 365 days)
        assert 0.4 < (weight_old / weight_recent) < 0.6
    
    def test_compute_weight_context_strength(self, service):
        """Test weight computation with context strength."""
        # Strong context
        weight_strong = service._compute_weight(
            co_occurrence_count=5,
            timestamp=datetime.utcnow(),
            context_strength=1.0
        )
        
        # Weak context
        weight_weak = service._compute_weight(
            co_occurrence_count=5,
            timestamp=datetime.utcnow(),
            context_strength=0.5
        )
        
        # Strong context should have higher weight
        assert weight_strong > weight_weak
        assert abs(weight_strong - 2 * weight_weak) < 0.1  # Approximately 2x
    
    def test_compute_weight_bounds(self, service):
        """Test weight is clamped to valid range."""
        # Very high frequency + recent
        weight = service._compute_weight(
            co_occurrence_count=10000,
            timestamp=datetime.utcnow(),
            context_strength=10.0
        )
        
        # Should be capped at 5.0
        assert weight <= 5.0
        
        # Very low frequency + old
        weight = service._compute_weight(
            co_occurrence_count=1,
            timestamp=datetime.utcnow() - timedelta(days=3650),  # 10 years
            context_strength=0.1
        )
        
        # Should be at least 0.1
        assert weight >= 0.1
    
    def test_compute_confidence_method_base(self, service):
        """Test confidence base values by method."""
        entity_a = {"id": "1", "type": "PERSON"}
        entity_b = {"id": "2", "type": "PAPER"}
        
        # Co-occurrence
        conf_cooc = service._compute_confidence(
            method="co_occurrence",
            weight=1.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={}
        )
        
        # Citation
        conf_cite = service._compute_confidence(
            method="citation",
            weight=1.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={}
        )
        
        # Mention
        conf_mention = service._compute_confidence(
            method="mention",
            weight=1.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={}
        )
        
        # Citation should be most confident
        assert conf_cite > conf_mention > conf_cooc
    
    def test_compute_confidence_weight_boost(self, service):
        """Test confidence boost from higher weight."""
        entity_a = {"id": "1"}
        entity_b = {"id": "2"}
        
        # Low weight
        conf_low = service._compute_confidence(
            method="co_occurrence",
            weight=1.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={}
        )
        
        # High weight
        conf_high = service._compute_confidence(
            method="co_occurrence",
            weight=3.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={}
        )
        
        assert conf_high > conf_low
    
    def test_compute_confidence_entity_quality_boost(self, service):
        """Test confidence boost for well-established entities."""
        # Without metadata
        conf_no_meta = service._compute_confidence(
            method="co_occurrence",
            weight=1.0,
            entity_a={"id": "1"},
            entity_b={"id": "2"},
            context={}
        )
        
        # With metadata
        conf_with_meta = service._compute_confidence(
            method="co_occurrence",
            weight=1.0,
            entity_a={"id": "1", "metadata": {"source": "api"}},
            entity_b={"id": "2", "metadata": {"source": "api"}},
            context={}
        )
        
        assert conf_with_meta > conf_no_meta
    
    def test_compute_confidence_context_boosts(self, service):
        """Test confidence boosts from context signals."""
        entity_a = {"id": "1"}
        entity_b = {"id": "2"}
        
        # No context
        conf_base = service._compute_confidence(
            method="co_occurrence",
            weight=1.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={}
        )
        
        # Verified
        conf_verified = service._compute_confidence(
            method="co_occurrence",
            weight=1.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={"verified": True}
        )
        
        # User action
        conf_user = service._compute_confidence(
            method="co_occurrence",
            weight=1.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={"user_action": True}
        )
        
        # Both
        conf_both = service._compute_confidence(
            method="co_occurrence",
            weight=1.0,
            entity_a=entity_a,
            entity_b=entity_b,
            context={"verified": True, "user_action": True}
        )
        
        assert conf_verified > conf_base
        assert conf_user > conf_base
        assert conf_both > conf_verified
        assert conf_both > conf_user
    
    def test_infer_from_co_occurrence_person_paper(self, service):
        """Test inferring PERSON-PAPER relationships."""
        entities = [
            {"id": "person-1", "type": "PERSON"},
            {"id": "paper-1", "type": "PAPER"}
        ]
        
        inferred = service.infer_relationships_from_co_occurrence(
            user_id="user-1",
            entities=entities,
            context={"frequency": 1}
        )
        
        # Should infer AUTHORED or WORKS_ON
        assert len(inferred) > 0
        
        # All should have required fields
        for rel in inferred:
            assert "from_id" in rel
            assert "to_id" in rel
            assert "rel_type" in rel
            assert "weight" in rel
            assert "confidence" in rel
            assert rel["confidence"] >= 0.5  # Above threshold
    
    def test_infer_from_co_occurrence_person_person(self, service):
        """Test inferring PERSON-PERSON collaboration."""
        entities = [
            {"id": "person-1", "type": "PERSON"},
            {"id": "person-2", "type": "PERSON"}
        ]
        
        inferred = service.infer_relationships_from_co_occurrence(
            user_id="user-1",
            entities=entities,
            context={"frequency": 5}
        )
        
        # Should infer COLLABORATES_WITH
        assert len(inferred) > 0
        
        collab_rels = [r for r in inferred if r["rel_type"] == "COLLABORATES_WITH"]
        assert len(collab_rels) > 0
    
    def test_infer_from_co_occurrence_below_threshold(self, service):
        """Test that low-confidence relationships are filtered."""
        service.min_confidence = 0.9  # Very high threshold
        
        entities = [
            {"id": "person-1", "type": "PERSON"},
            {"id": "paper-1", "type": "PAPER"}
        ]
        
        inferred = service.infer_relationships_from_co_occurrence(
            user_id="user-1",
            entities=entities,
            context={"frequency": 1}  # Low frequency
        )
        
        # Should filter out low-confidence relationships
        assert len(inferred) == 0
    
    def test_infer_citations_from_paper_content_title_match(self, service):
        """Test citation inference from paper title mention."""
        cited_papers = [
            {
                "id": "paper-2",
                "title": "Deep Learning for Computer Vision",
                "authors": ["John Doe"],
                "doi": "10.1234/abcd"
            }
        ]
        
        content = """
        This work builds upon Deep Learning for Computer Vision, 
        which showed that convolutional networks can achieve...
        """
        
        citations = service.infer_citations_from_paper_content(
            paper_id="paper-1",
            paper_content=content,
            known_papers=cited_papers
        )
        
        assert len(citations) == 1
        assert citations[0]["rel_type"] == "CITES"
        assert citations[0]["from_id"] == "paper-1"
        assert citations[0]["to_id"] == "paper-2"
        assert "title" in citations[0]["mention_signals"]
    
    def test_infer_citations_from_paper_content_author_match(self, service):
        """Test citation inference from author mention."""
        cited_papers = [
            {
                "id": "paper-2",
                "title": "Neural Networks",
                "authors": ["Jane Smith", "Bob Johnson"],
                "doi": "10.1234/xyz"
            }
        ]
        
        content = """
        Jane Smith et al. demonstrated that neural networks can...
        """
        
        citations = service.infer_citations_from_paper_content(
            paper_id="paper-1",
            paper_content=content,
            known_papers=cited_papers
        )
        
        assert len(citations) == 1
        assert "author" in citations[0]["mention_signals"]
    
    def test_infer_citations_from_paper_content_doi_match(self, service):
        """Test citation inference from DOI mention (highest confidence)."""
        cited_papers = [
            {
                "id": "paper-2",
                "title": "Some Paper",
                "authors": ["Author"],
                "doi": "10.1234/very-specific-doi"
            }
        ]
        
        content = "See DOI: 10.1234/very-specific-doi for details."
        
        citations = service.infer_citations_from_paper_content(
            paper_id="paper-1",
            paper_content=content,
            known_papers=cited_papers
        )
        
        assert len(citations) == 1
        assert citations[0]["confidence"] >= 0.9  # DOI = high confidence
        assert "doi" in citations[0]["mention_signals"]
    
    def test_infer_citations_multiple_signals(self, service):
        """Test citation with multiple mention signals."""
        cited_papers = [
            {
                "id": "paper-2",
                "title": "Machine Learning Basics",
                "authors": ["Alice Johnson"],
                "doi": "10.1234/ml"
            }
        ]
        
        content = """
        Alice Johnson's work on Machine Learning Basics (DOI: 10.1234/ml)
        is foundational to this field.
        """
        
        citations = service.infer_citations_from_paper_content(
            paper_id="paper-1",
            paper_content=content,
            known_papers=cited_papers
        )
        
        assert len(citations) == 1
        # Should have multiple signals
        assert len(citations[0]["mention_signals"]) >= 2
        # Higher confidence with multiple signals
        assert citations[0]["confidence"] >= 0.85
    
    def test_infer_citations_no_self_citation(self, service):
        """Test that self-citations are excluded."""
        papers = [
            {"id": "paper-1", "title": "My Own Paper", "authors": ["Me"]}
        ]
        
        content = "This paper (My Own Paper) shows..."
        
        citations = service.infer_citations_from_paper_content(
            paper_id="paper-1",
            paper_content=content,
            known_papers=papers
        )
        
        # Should not cite itself
        assert len(citations) == 0
    
    def test_infer_tool_usage_from_text(self, service):
        """Test tool/dataset usage inference from text."""
        known_tools = [
            {"id": "tool-1", "name": "PyTorch", "type": "TOOL"},
            {"id": "tool-2", "name": "TensorFlow", "type": "TOOL"}
        ]
        
        text = "We implemented our model using PyTorch framework."
        
        uses = service.infer_tool_usage_from_text(
            entity_id="paper-1",
            entity_type="PAPER",
            text_content=text,
            known_tools=known_tools
        )
        
        assert len(uses) == 1
        assert uses[0]["rel_type"] == "USES"
        assert uses[0]["to_id"] == "tool-1"
    
    def test_infer_tool_usage_primary(self, service):
        """Test detection of primary tool usage."""
        known_tools = [
            {"id": "tool-1", "canonical_name": "PyTorch", "type": "TOOL"}
        ]
        
        text = "Our system is primarily built with PyTorch."
        
        uses = service.infer_tool_usage_from_text(
            entity_id="project-1",
            entity_type="PROJECT",
            text_content=text,
            known_tools=known_tools
        )
        
        assert len(uses) == 1
        assert uses[0]["primary"] is True
        assert uses[0]["weight"] == 2.0  # Primary = higher weight
    
    def test_apply_confidence_thresholds_default(self, service):
        """Test applying default confidence thresholds."""
        relationships = [
            {"rel_type": "AUTHORED", "confidence": 0.8},
            {"rel_type": "AUTHORED", "confidence": 0.6},  # Below threshold (0.7)
            {"rel_type": "CITES", "confidence": 0.7},
            {"rel_type": "CITES", "confidence": 0.5},  # Below threshold (0.6)
        ]
        
        filtered = service.apply_confidence_thresholds(relationships)
        
        # Should keep only those above threshold
        assert len(filtered) == 2
        assert all(r["confidence"] >= 0.6 for r in filtered)
    
    def test_apply_confidence_thresholds_custom(self, service):
        """Test applying custom confidence thresholds."""
        relationships = [
            {"rel_type": "AUTHORED", "confidence": 0.8},
            {"rel_type": "AUTHORED", "confidence": 0.6},
        ]
        
        custom_thresholds = {
            "AUTHORED": 0.9  # Very high threshold
        }
        
        filtered = service.apply_confidence_thresholds(
            relationships,
            thresholds=custom_thresholds
        )
        
        # Only 0.8 should pass (below 0.9, but let's say we're strict)
        # Actually 0.8 < 0.9, so it should be filtered out
        assert len(filtered) == 0
    
    def test_batch_infer_from_activity_log(self, service):
        """Test batch inference from activity log."""
        activity_log = [
            {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "paper_read",
                "entities": [
                    {"id": "person-1", "type": "PERSON"},
                    {"id": "paper-1", "type": "PAPER"}
                ],
                "importance": 1.0
            },
            {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "paper_read",
                "entities": [
                    {"id": "person-1", "type": "PERSON"},
                    {"id": "paper-1", "type": "PAPER"}
                ],
                "importance": 1.0
            },
            # Same pair twice = meets min_co_occurrences=2
        ]
        
        result = service.batch_infer_from_activity_log(
            user_id="user-1",
            activity_log=activity_log,
            lookback_days=90
        )
        
        assert "inferred_count" in result
        assert "co_occurrence_pairs" in result
        assert "execution_time_sec" in result
        assert result["activities_analyzed"] == 2


class TestEntityPairMappings:
    """Test entity pair to relationship type mappings."""
    
    def test_all_relationship_types_mapped(self):
        """Test that all important combinations are mapped."""
        # Check key combinations exist
        assert (NodeType.PERSON, NodeType.PERSON) in ENTITY_PAIR_TO_RELATIONSHIP
        assert (NodeType.PERSON, NodeType.PAPER) in ENTITY_PAIR_TO_RELATIONSHIP
        assert (NodeType.PAPER, NodeType.PAPER) in ENTITY_PAIR_TO_RELATIONSHIP
        assert (NodeType.PAPER, NodeType.TOPIC) in ENTITY_PAIR_TO_RELATIONSHIP
    
    def test_mappings_have_valid_relationship_types(self):
        """Test that all mappings use valid relationship types."""
        for pair, mappings in ENTITY_PAIR_TO_RELATIONSHIP.items():
            for rel_type, method in mappings:
                # Should be valid RelationshipType enum
                assert isinstance(rel_type, RelationshipType)
                # Method should be one of known methods
                assert method in ["co_occurrence", "citation", "mention"]


class TestGlobalInstance:
    """Test global inference service instance."""
    
    def test_global_instance_exists(self):
        """Test that global instance is accessible."""
        assert relationship_inference_service is not None
        assert isinstance(relationship_inference_service, RelationshipInferenceService)
    
    def test_global_instance_has_defaults(self):
        """Test that global instance has sensible defaults."""
        assert relationship_inference_service.min_confidence > 0
        assert relationship_inference_service.recency_decay_days > 0
        assert relationship_inference_service.min_co_occurrences >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
