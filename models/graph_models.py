"""
Pydantic Models for Knowledge Graph Entities and Relationships
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class NodeType(str, Enum):
    """Valid node types in the knowledge graph."""
    PERSON = "PERSON"
    PAPER = "PAPER"
    TOPIC = "TOPIC"
    PROJECT = "PROJECT"
    DATASET = "DATASET"
    INSTITUTION = "INSTITUTION"
    ORGANIZATION = "ORGANIZATION"  # Generic org: company, media, cloud, etc.
    TOOL = "TOOL"
    VENUE = "VENUE"


class RelationshipType(str, Enum):
    """Valid relationship types in the knowledge graph."""
    AUTHORED = "AUTHORED"
    COLLABORATES_WITH = "COLLABORATES_WITH"
    WORKS_ON = "WORKS_ON"
    CONTRIBUTES_TO = "CONTRIBUTES_TO"
    AFFILIATED_WITH = "AFFILIATED_WITH"
    CITES = "CITES"
    USES = "USES"
    ON_TOPIC = "ON_TOPIC"
    PUBLISHED_AT = "PUBLISHED_AT"
    RELATED_TO = "RELATED_TO"
    DEPENDS_ON = "DEPENDS_ON"
    # Activity-inferred relationship types
    LEARNED_FROM = "LEARNED_FROM"      # User learned from an org/domain
    USED_TOGETHER = "USED_TOGETHER"    # Two entities co-occur in the same session


# ============================================================================
# NODE SCHEMAS
# ============================================================================

class PersonNode(BaseModel):
    """PERSON node in knowledge graph."""
    id: str = Field(..., description="UUID from entity system")
    user_id: str
    canonical_name: str
    aliases: List[str] = []
    email: Optional[str] = None
    affiliation: Optional[str] = None
    h_index: int = 0
    num_publications: int = 0
    research_interests: List[str] = []
    orcid: Optional[str] = None
    external_links: Dict[str, str] = {}
    metadata: Dict[str, Any] = {}


class PaperNode(BaseModel):
    """PAPER node in knowledge graph."""
    id: str
    user_id: str
    title: str
    authors: List[str] = []
    abstract: str = ""
    year: Optional[int] = None
    doi: Optional[str] = None
    arxiv_id: Optional[str] = None
    venue: Optional[str] = None
    keywords: List[str] = []
    num_citations: int = 0
    external_url: Optional[str] = None
    metadata: Dict[str, Any] = {}


class TopicNode(BaseModel):
    """TOPIC node in knowledge graph."""
    id: str
    user_id: str
    canonical_name: str
    aliases: List[str] = []
    description: str = ""
    parent_topic_id: Optional[str] = None
    num_papers: int = 0
    num_people: int = 0
    embedding: Optional[List[float]] = None
    metadata: Dict[str, Any] = {}


class ProjectNode(BaseModel):
    """PROJECT node in knowledge graph."""
    id: str
    user_id: str
    name: str
    description: str = ""
    start_date: Optional[str] = None
    status: str = "active"  # active, paused, completed
    repository: Optional[str] = None
    num_collaborators: int = 0
    num_papers: int = 0
    metadata: Dict[str, Any] = {}


class DatasetNode(BaseModel):
    """DATASET node in knowledge graph."""
    id: str
    user_id: str
    name: str
    description: str = ""
    num_samples: Optional[int] = None
    url: Optional[str] = None
    license: Optional[str] = None
    papers_using: int = 0
    metadata: Dict[str, Any] = {}


class InstitutionNode(BaseModel):
    """INSTITUTION node in knowledge graph."""
    id: str
    user_id: str
    name: str
    city: Optional[str] = None
    country: Optional[str] = None
    type: str = "university"  # university, lab, company, other
    num_affiliations: int = 0
    website: Optional[str] = None
    metadata: Dict[str, Any] = {}


class ToolNode(BaseModel):
    """TOOL node in knowledge graph."""
    id: str
    user_id: str
    name: str
    type: str = "framework"  # framework, library, software, hardware
    version: Optional[str] = None
    language: Optional[str] = None
    papers_using: int = 0
    github_url: Optional[str] = None
    metadata: Dict[str, Any] = {}


class VenueNode(BaseModel):
    """VENUE node in knowledge graph."""
    id: str
    user_id: str
    name: str
    type: str = "conference"  # conference, journal, workshop, other
    acronym: Optional[str] = None
    year: Optional[int] = None
    papers_in_venue: int = 0
    h5_index: Optional[int] = None
    location: Optional[str] = None
    metadata: Dict[str, Any] = {}


# ============================================================================
# RELATIONSHIP SCHEMAS
# ============================================================================

class RelationshipProperties(BaseModel):
    """Base properties for all relationships."""
    weight: float = 1.0
    confidence: float = 1.0
    first_seen_at: datetime = Field(default_factory=datetime.utcnow)
    last_updated_at: datetime = Field(default_factory=datetime.utcnow)
    source: List[str] = []  # paper, github, activity, etc.


class AuthoredRelationship(RelationshipProperties):
    """AUTHORED relationship (PERSON -> PAPER)."""
    position: Optional[int] = None  # 1st author, 2nd, etc.


class CollaboratesWithRelationship(RelationshipProperties):
    """COLLABORATES_WITH relationship (PERSON -> PERSON)."""
    num_papers: int = 1
    projects: List[str] = []


class WorksOnRelationship(RelationshipProperties):
    """WORKS_ON relationship (PERSON -> TOPIC)."""
    expertise_level: float = 0.5  # 0-1
    years: Optional[int] = None


class ContributesToRelationship(RelationshipProperties):
    """CONTRIBUTES_TO relationship (PERSON -> PROJECT)."""
    role: Optional[str] = None  # lead, contributor, etc.
    hours: Optional[int] = None


class AffiliatedWithRelationship(RelationshipProperties):
    """AFFILIATED_WITH relationship (PERSON -> INSTITUTION)."""
    position: Optional[str] = None  # professor, student, etc.
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class CitesRelationship(RelationshipProperties):
    """CITES relationship (PAPER -> PAPER)."""
    context: Optional[str] = None  # excerpt where citation appears
    num_citations: int = 1


class UsesRelationship(RelationshipProperties):
    """USES relationship (PAPER -> DATASET/TOOL, PROJECT -> DATASET/TOOL)."""
    primary: bool = False
    essential: bool = False
    version: Optional[str] = None


class OnTopicRelationship(RelationshipProperties):
    """ON_TOPIC relationship (PAPER -> TOPIC)."""
    relevance_score: float = 1.0  # 0-1


class PublishedAtRelationship(RelationshipProperties):
    """PUBLISHED_AT relationship (PAPER -> VENUE)."""
    page: Optional[str] = None
    doi: Optional[str] = None


class RelatedToRelationship(RelationshipProperties):
    """RELATED_TO relationship (TOPIC -> TOPIC)."""
    similarity_score: float = 0.5  # 0-1


class DependsOnRelationship(RelationshipProperties):
    """DEPENDS_ON relationship (TOOL -> TOOL)."""
    version_requirement: Optional[str] = None


# ============================================================================
# GRAPH INGESTION MODELS
# ============================================================================

class GraphNodeCreate(BaseModel):
    """Request to create a graph node."""
    node_type: NodeType
    properties: Dict[str, Any]


class GraphRelationshipCreate(BaseModel):
    """Request to create a graph relationship."""
    from_id: str
    to_id: str
    relationship_type: RelationshipType
    properties: Dict[str, Any] = {}


class BatchIngestionRequest(BaseModel):
    """Batch ingestion of nodes and relationships."""
    nodes: List[GraphNodeCreate]
    relationships: List[GraphRelationshipCreate] = []


class IngestionResult(BaseModel):
    """Result of ingestion operation."""
    ingested: int
    failed: int
    errors: List[Dict[str, str]] = []
    execution_time_sec: float
