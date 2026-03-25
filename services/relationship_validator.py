"""
Relationship Validation and Scoring for Knowledge Graph
Validates relationship types, computes weights, and assigns confidence scores.
"""

from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import logging

from models.graph_models import RelationshipType, NodeType

logger = logging.getLogger(__name__)


class RelationshipValidator:
    """Validates and scores relationships for the knowledge graph."""
    
    # Valid source→target node type combinations for each relationship
    VALID_RELATIONSHIPS = {
        RelationshipType.AUTHORED: [
            (NodeType.PERSON, NodeType.PAPER)
        ],
        RelationshipType.COLLABORATES_WITH: [
            (NodeType.PERSON, NodeType.PERSON)
        ],
        RelationshipType.WORKS_ON: [
            (NodeType.PERSON, NodeType.TOPIC),
            (NodeType.PERSON, NodeType.PROJECT)
        ],
        RelationshipType.CONTRIBUTES_TO: [
            (NodeType.PERSON, NodeType.PROJECT)
        ],
        RelationshipType.AFFILIATED_WITH: [
            (NodeType.PERSON, NodeType.INSTITUTION)
        ],
        RelationshipType.CITES: [
            (NodeType.PAPER, NodeType.PAPER)
        ],
        RelationshipType.USES: [
            (NodeType.PAPER, NodeType.DATASET),
            (NodeType.PAPER, NodeType.TOOL),
            (NodeType.PROJECT, NodeType.DATASET),
            (NodeType.PROJECT, NodeType.TOOL)
        ],
        RelationshipType.ON_TOPIC: [
            (NodeType.PAPER, NodeType.TOPIC),
            (NodeType.PROJECT, NodeType.TOPIC)
        ],
        RelationshipType.PUBLISHED_AT: [
            (NodeType.PAPER, NodeType.VENUE)
        ],
        RelationshipType.RELATED_TO: [
            (NodeType.TOPIC, NodeType.TOPIC),
            (NodeType.PAPER, NodeType.PAPER)
        ],
        RelationshipType.DEPENDS_ON: [
            (NodeType.TOOL, NodeType.TOOL),
            (NodeType.PROJECT, NodeType.PROJECT)
        ]
    }
    
    # Source strength for confidence scoring
    SOURCE_STRENGTH = {
        "activity": 1.0,      # Direct user activity (highest confidence)
        "api": 0.95,          # External API (GitHub, Scholar, etc.)
        "inference": 0.7,     # NLP inference
        "embedding": 0.6,     # Similarity-based
        "user_input": 0.85,   # Manual user entry
        "default": 0.5        # Unknown source
    }
    
    def __init__(self):
        """Initialize relationship validator."""
        self.logger = logging.getLogger(__name__)
    
    def validate_relationship(
        self,
        from_type: str,
        to_type: str,
        rel_type: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate if relationship type is valid for given node types.
        
        Args:
            from_type: Source node type (e.g., "PERSON")
            to_type: Target node type (e.g., "PAPER")
            rel_type: Relationship type (e.g., "AUTHORED")
            
        Returns:
            (is_valid, error_message)
        """
        try:
            # Convert strings to enums
            rel_enum = RelationshipType(rel_type)
            from_enum = NodeType(from_type)
            to_enum = NodeType(to_type)
            
        except ValueError as e:
            return False, f"Invalid type: {e}"
        
        # Check if relationship type exists
        if rel_enum not in self.VALID_RELATIONSHIPS:
            return False, f"Unknown relationship type: {rel_type}"
        
        # Check if node type combination is valid
        valid_combos = self.VALID_RELATIONSHIPS[rel_enum]
        
        if (from_enum, to_enum) in valid_combos:
            return True, None
        
        # Build helpful error message
        valid_str = ", ".join([
            f"{f.value}->{t.value}" 
            for f, t in valid_combos
        ])
        
        return False, (
            f"Invalid node types for {rel_type}: "
            f"{from_type}->{to_type}. "
            f"Valid combinations: {valid_str}"
        )
    
    def compute_weight(
        self,
        rel_type: str,
        properties: Dict[str, Any]
    ) -> float:
        """
        Compute relationship weight based on type and properties.
        
        Args:
            rel_type: Relationship type
            properties: Relationship properties
            
        Returns:
            Weight value (0.0 to 1.0+)
        """
        weight = 1.0  # Default weight
        
        # Type-specific weight computation
        if rel_type == "AUTHORED":
            # Higher weight for first authors
            position = properties.get("position", 999)
            if position == 1:
                weight = 2.0
            elif position <= 3:
                weight = 1.5
            else:
                weight = 1.0
        
        elif rel_type == "CITES":
            # Weight based on citation context
            num_citations = properties.get("num_citations", 1)
            weight = min(3.0, 1.0 + (num_citations * 0.1))
        
        elif rel_type == "COLLABORATES_WITH":
            # Weight based on number of shared papers
            num_papers = properties.get("num_papers", 1)
            weight = min(5.0, num_papers * 0.5)
        
        elif rel_type == "WORKS_ON":
            # Weight based on expertise level
            expertise = properties.get("expertise_level", 0.5)
            years = properties.get("years", 1)
            weight = expertise * min(3.0, 1.0 + (years * 0.2))
        
        elif rel_type == "USES":
            # Higher weight if primary or essential
            if properties.get("primary"):
                weight = 2.0
            elif properties.get("essential"):
                weight = 1.5
            else:
                weight = 1.0
        
        elif rel_type == "ON_TOPIC":
            # Weight based on relevance score
            relevance = properties.get("relevance_score", 0.5)
            weight = relevance * 2.0
        
        elif rel_type == "RELATED_TO":
            # Weight based on similarity score
            similarity = properties.get("similarity_score", 0.5)
            weight = similarity * 2.0
        
        # Ensure weight is positive
        return max(0.1, weight)
    
    def compute_confidence(
        self,
        sources: List[str],
        properties: Dict[str, Any] = None
    ) -> float:
        """
        Compute confidence score based on sources and properties.
        
        Args:
            sources: List of data sources (e.g., ["activity", "api"])
            properties: Additional properties for confidence boost
            
        Returns:
            Confidence score (0.0 to 1.0)
        """
        if not sources:
            return self.SOURCE_STRENGTH["default"]
        
        # Take maximum confidence from all sources
        max_confidence = max(
            self.SOURCE_STRENGTH.get(source, self.SOURCE_STRENGTH["default"])
            for source in sources
        )
        
        # Boost confidence if multiple sources agree
        if len(sources) > 1:
            max_confidence = min(1.0, max_confidence * 1.1)
        
        # Additional boosts for specific properties
        if properties:
            # Boost for explicit user confirmation
            if properties.get("user_confirmed"):
                max_confidence = min(1.0, max_confidence * 1.2)
            
            # Reduce for inferred relationships without validation
            if properties.get("inferred") and not properties.get("validated"):
                max_confidence *= 0.8
        
        return round(max_confidence, 2)
    
    def add_temporal_properties(
        self,
        properties: Dict[str, Any],
        update_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Add or update temporal properties.
        
        Args:
            properties: Existing properties
            update_existing: If True, update last_updated_at
            
        Returns:
            Properties with temporal fields
        """
        now = datetime.utcnow().isoformat()
        
        if not update_existing and "first_seen_at" not in properties:
            properties["first_seen_at"] = now
        
        properties["last_updated_at"] = now
        
        return properties
    
    def validate_required_properties(
        self,
        rel_type: str,
        properties: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate that required properties exist for relationship type.
        
        Args:
            rel_type: Relationship type
            properties: Properties to validate
            
        Returns:
            (is_valid, error_message)
        """
        required_fields = {
            "AUTHORED": ["position"],
            "COLLABORATES_WITH": ["num_papers"],
            "WORKS_ON": ["expertise_level"],
            "PUBLISHED_AT": [],
            "CITES": [],
            "USES": [],
            "ON_TOPIC": ["relevance_score"],
            "RELATED_TO": ["similarity_score"],
            "CONTRIBUTES_TO": [],
            "AFFILIATED_WITH": [],
            "DEPENDS_ON": []
        }
        
        required = required_fields.get(rel_type, [])
        
        missing = [field for field in required if field not in properties]
        
        if missing:
            return False, f"Missing required properties for {rel_type}: {', '.join(missing)}"
        
        return True, None
    
    def enrich_relationship_properties(
        self,
        from_type: str,
        to_type: str,
        rel_type: str,
        properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enrich relationship properties with computed values.
        
        Args:
            from_type: Source node type
            to_type: Target node type
            rel_type: Relationship type
            properties: Base properties
            
        Returns:
            Enriched properties with weight, confidence, temporal data
        """
        enriched = properties.copy()
        
        # Add temporal properties
        enriched = self.add_temporal_properties(enriched)
        
        # Compute weight if not provided
        if "weight" not in enriched:
            enriched["weight"] = self.compute_weight(rel_type, enriched)
        
        # Compute confidence if sources provided
        sources = enriched.get("source", [])
        if not isinstance(sources, list):
            sources = [sources] if sources else []
        
        if "confidence" not in enriched:
            enriched["confidence"] = self.compute_confidence(sources, enriched)
        
        # Ensure source is a list
        if "source" not in enriched:
            enriched["source"] = ["default"]
        elif not isinstance(enriched["source"], list):
            enriched["source"] = [enriched["source"]]
        
        return enriched


# Global validator instance
relationship_validator = RelationshipValidator()
