"""
Pydantic Models for Graph API
Request/response schemas with validation.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator


# ============================================================================
# NODE MODELS
# ============================================================================

class NodeMetrics(BaseModel):
    """Node centrality and embedding metrics."""
    degree_centrality: Optional[float] = None
    betweenness_centrality: Optional[float] = None
    closeness_centrality: Optional[float] = None
    eigenvector_centrality: Optional[float] = None
    pagerank: Optional[float] = None
    community_id: Optional[int] = None
    embedding_reduced: Optional[List[float]] = None


class NodeDetail(BaseModel):
    """Detailed node information."""
    node_id: int
    labels: List[str]
    properties: Dict[str, Any]
    metrics: NodeMetrics
    neighbor_count: int


class NeighborNode(BaseModel):
    """Neighbor node with relationship info."""
    node_id: int
    labels: List[str]
    name: str
    relationship_type: str
    relationship_weight: Optional[float] = None


class NeighborResponse(BaseModel):
    """Response for neighbor query."""
    node_id: int
    neighbors: List[NeighborNode]
    total_neighbors: int


# ============================================================================
# EXPERT MODELS
# ============================================================================

class Expert(BaseModel):
    """Expert ranking."""
    node_id: int
    name: str
    node_type: str
    pagerank: float
    h_index: Optional[int] = None
    paper_count: Optional[int] = None
    community_id: Optional[int] = None


class ExpertListResponse(BaseModel):
    """Response for expert listings."""
    experts: List[Expert]
    total_count: int
    page: int
    page_size: int
    topic_id: Optional[int] = None


# ============================================================================
# RECOMMENDATION MODELS
# ============================================================================

class CollaboratorRecommendation(BaseModel):
    """Recommended collaborator."""
    node_id: int
    name: str
    similarity_score: float
    shared_topics: List[str]
    pagerank: Optional[float] = None
    reason: str  # "embedding_similarity", "community_bridge", etc.


class CollaboratorRecommendationResponse(BaseModel):
    """Response for collaborator recommendations."""
    recommendations: List[CollaboratorRecommendation]
    total_count: int


# ============================================================================
# LEARNING PATH MODELS
# ============================================================================

class LearningPathNode(BaseModel):
    """Node in learning path."""
    node_id: int
    name: str
    node_type: str
    depth: int
    prerequisites: List[int] = []


class LearningPath(BaseModel):
    """Learning path from topic A to topic B."""
    path: List[LearningPathNode]
    total_steps: int
    difficulty: str  # "beginner", "intermediate", "advanced"


class LearningPathResponse(BaseModel):
    """Response for learning path query."""
    paths: List[LearningPath]
    source_topic_id: int
    target_topic_id: Optional[int] = None


# ============================================================================
# COMMUNITY MODELS
# ============================================================================

class CommunityMember(BaseModel):
    """Community member."""
    node_id: int
    name: str
    node_type: str


class Community(BaseModel):
    """Community with statistics."""
    community_id: int
    size: int
    modularity_contribution: Optional[float] = None
    dominant_node_types: List[str]
    sample_members: List[CommunityMember]


class CommunityListResponse(BaseModel):
    """Response for community listing."""
    communities: List[Community]
    total_communities: int
    overall_modularity: float
    page: int
    page_size: int


# ============================================================================
# EMBEDDING MODELS
# ============================================================================

class EmbeddingSearchRequest(BaseModel):
    """Request for embedding similarity search."""
    node_id: Optional[int] = None
    embedding_vector: Optional[List[float]] = None
    top_k: int = Field(default=10, ge=1, le=100)
    min_similarity: float = Field(default=0.5, ge=0.0, le=1.0)
    node_types: Optional[List[str]] = None
    
    @validator('embedding_vector')
    def validate_embedding_vector(cls, v, values):
        if v is not None and len(v) != 128:
            raise ValueError("Embedding vector must be 128 dimensions")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "node_id": 42,
                "top_k": 10,
                "min_similarity": 0.7,
                "node_types": ["PERSON", "PAPER"]
            }
        }


class SimilarNode(BaseModel):
    """Similar node from embedding search."""
    node_id: int
    name: str
    node_type: str
    similarity: float
    community_id: Optional[int] = None


class EmbeddingSearchResponse(BaseModel):
    """Response for embedding search."""
    similar_nodes: List[SimilarNode]
    query_node_id: Optional[int] = None
    total_results: int


# ============================================================================
# GRAPH EXPORT MODELS
# ============================================================================

class GraphNode(BaseModel):
    """Node for graph export."""
    id: int
    labels: List[str]
    properties: Dict[str, Any]


class GraphRelationship(BaseModel):
    """Relationship for graph export."""
    source: int
    target: int
    type: str
    properties: Dict[str, Any]


class GraphExportResponse(BaseModel):
    """Response for graph export."""
    nodes: List[GraphNode]
    relationships: List[GraphRelationship]
    total_nodes: int
    total_relationships: int
    exported_at: datetime


# ============================================================================
# PAGINATION & FILTERING
# ============================================================================

class PaginationParams(BaseModel):
    """Pagination parameters."""
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    
    @property
    def offset(self) -> int:
        """Calculate offset for database queries."""
        return (self.page - 1) * self.page_size


class FilterParams(BaseModel):
    """Filtering parameters."""
    node_types: Optional[List[str]] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    min_pagerank: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    community_id: Optional[int] = None


# ============================================================================
# ERROR MODELS
# ============================================================================

class ErrorResponse(BaseModel):
    """Error response."""
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ValidationErrorResponse(BaseModel):
    """Validation error response."""
    error: str = "Validation error"
    detail: List[Dict[str, Any]]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
