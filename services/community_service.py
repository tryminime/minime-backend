"""
Community Detection Service
Detect communities/clusters in knowledge graphs using Louvain algorithm.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import time
from collections import defaultdict

from prometheus_client import Counter, Histogram, Gauge

from config.neo4j_config import get_neo4j_session, get_neo4j_driver

logger = logging.getLogger(__name__)


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

community_detection_total = Counter(
    'community_detection_total',
    'Total community detection operations',
    ['status']
)

community_detection_duration = Histogram(
    'community_detection_duration_seconds',
    'Community detection duration',
    buckets=[0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
)

community_count = Gauge(
    'community_count',
    'Number of communities detected',
    ['user_id']
)

community_modularity = Gauge(
    'community_modularity_score',
    'Modularity score of detected communities',
    ['user_id']
)


class CommunityService:
    """
    Service for detecting and managing graph communities.
    
    Features:
    - Louvain algorithm (Neo4j GDS)
    - Community assignment to nodes
    - Modularity scoring
    - Community statistics
    - Community queries
    """
    
    def __init__(self):
        """Initialize community detection service."""
        self.logger = logging.getLogger(__name__)
        self._driver = None  # Lazy init — Neo4j may not be available
    
    @property
    def driver(self):
        """Lazy Neo4j driver initialization."""
        if self._driver is None:
            self._driver = get_neo4j_driver()
        return self._driver
    
    def detect_communities(
        self,
        user_id: str,
        graph_name: str = None,
        relationship_weight_property: str = "weight",
        store_results: bool = True
    ) -> Dict[str, Any]:
        """
        Detect communities using Louvain algorithm.
        
        Args:
            user_id: User ID for filtering
            graph_name: Projected graph name (or create new one)
            relationship_weight_property: Property name for edge weights
            store_results: If True, write community_id to nodes
            
        Returns:
            Community detection results
        """
        start_time = time.time()
        
        try:
            # Project graph if needed
            if graph_name is None:
                graph_name = self._project_graph(
                    user_id=user_id,
                    weight_property=relationship_weight_property
                )
                should_drop = True
            else:
                should_drop = False
            
            with get_neo4j_session() as session:
                if store_results:
                    # Write mode: store community_id on nodes
                    query = """
                    CALL gds.louvain.write($graphName, {
                        writeProperty: 'community_id',
                        relationshipWeightProperty: $weightProperty,
                        includeIntermediateCommunities: false
                    })
                    YIELD 
                        modularity,
                        modularities,
                        ranLevels,
                        communityCount,
                        communityDistribution,
                        nodePropertiesWritten
                    RETURN 
                        modularity,
                        modularities,
                        ranLevels,
                        communityCount,
                        communityDistribution,
                        nodePropertiesWritten
                    """
                else:
                    # Stream mode: just compute
                    query = """
                    CALL gds.louvain.stream($graphName, {
                        relationshipWeightProperty: $weightProperty
                    })
                    YIELD nodeId, communityId
                    RETURN 
                        count(DISTINCT communityId) as communityCount,
                        count(*) as nodeCount
                    """
                
                result = session.run(
                    query,
                    graphName=graph_name,
                    weightProperty=relationship_weight_property
                ).single()
                
                elapsed = time.time() - start_time
                
                if result:
                    num_communities = result.get("communityCount", 0)
                    modularity_score = result.get("modularity", 0.0)
                    nodes_written = result.get("nodePropertiesWritten", 0)
                    
                    # Track metrics
                    community_detection_total.labels(status='success').inc()
                    community_detection_duration.observe(elapsed)
                    community_count.labels(user_id=user_id).set(num_communities)
                    
                    if modularity_score:
                        community_modularity.labels(user_id=user_id).set(modularity_score)
                    
                    self.logger.info(
                        f"Detected {num_communities} communities for user {user_id} "
                        f"(modularity={modularity_score:.3f}) in {elapsed:.2f}s"
                    )
                    
                    return {
                        "user_id": user_id,
                        "status": "success",
                        "num_communities": num_communities,
                        "modularity": modularity_score,
                        "nodes_processed": nodes_written,
                        "execution_time_sec": elapsed,
                        "details": result.data() if result else {}
                    }
                
                raise RuntimeError("No result from Louvain algorithm")
                
        except Exception as e:
            elapsed = time.time() - start_time
            
            community_detection_total.labels(status='error').inc()
            
            self.logger.error(f"Community detection failed: {e}", exc_info=True)
            
            return {
                "user_id": user_id,
                "status": "error",
                "error": str(e),
                "execution_time_sec": elapsed
            }
        
        finally:
            if should_drop and graph_name:
                self._drop_graph(graph_name)
    
    def _project_graph(
        self,
        user_id: str,
        weight_property: str = "weight"
    ) -> str:
        """
        Project graph for community detection.
        
        Args:
            user_id: User ID
            weight_property: Relationship weight property
            
        Returns:
            Graph name
        """
        graph_name = f"community_user_{user_id}_{int(time.time())}"
        
        with get_neo4j_session() as session:
            # Drop if exists
            try:
                session.run(
                    "CALL gds.graph.drop($graphName)",
                    graphName=graph_name
                )
            except Exception:
                pass
            
            # Project all node types and relationships
            query = """
            CALL gds.graph.project(
                $graphName,
                '*',  // All node labels
                {
                    ALL: {
                        type: '*',  // All relationship types
                        orientation: 'UNDIRECTED',
                        properties: {
                            weight: {
                                property: $weightProperty,
                                defaultValue: 1.0
                            }
                        }
                    }
                }
            )
            YIELD graphName, nodeCount, relationshipCount
            RETURN graphName, nodeCount, relationshipCount
            """
            
            result = session.run(
                query,
                graphName=graph_name,
                weightProperty=weight_property
            ).single()
            
            if result:
                self.logger.info(
                    f"Projected graph '{graph_name}': "
                    f"{result['nodeCount']} nodes, "
                    f"{result['relationshipCount']} relationships"
                )
                return graph_name
            
            raise RuntimeError(f"Failed to project graph '{graph_name}'")
    
    def _drop_graph(self, graph_name: str):
        """Drop projected graph."""
        try:
            with get_neo4j_session() as session:
                session.run(
                    "CALL gds.graph.drop($graphName)",
                    graphName=graph_name
                )
            self.logger.info(f"Dropped graph '{graph_name}'")
        except Exception as e:
            self.logger.warning(f"Failed to drop graph '{graph_name}': {e}")
    
    def get_communities(
        self,
        user_id: str,
        min_size: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Get all communities for a user.
        
        Args:
            user_id: User ID
            min_size: Minimum community size
            
        Returns:
            List of communities with members
        """
        with get_neo4j_session() as session:
            query = """
            MATCH (n)
            WHERE n.user_id = $userId
              AND n.community_id IS NOT NULL
            RETURN n.community_id as communityId,
                   collect({
                       nodeId: id(n),
                       labels: labels(n),
                       name: coalesce(n.canonical_name, n.name, 'Unknown')
                   }) as members,
                   count(n) as size
            ORDER BY size DESC
            """
            
            results = session.run(query, userId=user_id).data()
            
            # Filter by min size
            communities = [
                {
                    "community_id": r["communityId"],
                    "size": r["size"],
                    "members": r["members"]
                }
                for r in results
                if r["size"] >= min_size
            ]
            
            self.logger.info(
                f"Retrieved {len(communities)} communities for user {user_id}"
            )
            
            return communities
    
    def get_community_by_id(
        self,
        user_id: str,
        community_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get specific community details.
        
        Args:
            user_id: User ID
            community_id: Community ID
            
        Returns:
            Community details or None
        """
        with get_neo4j_session() as session:
            query = """
            MATCH (n)
            WHERE n.user_id = $userId
              AND n.community_id = $communityId
            RETURN n.community_id as communityId,
                   collect({
                       nodeId: id(n),
                       labels: labels(n),
                       name: coalesce(n.canonical_name, n.name, 'Unknown'),
                       properties: properties(n)
                   }) as members,
                   count(n) as size
            """
            
            result = session.run(
                query,
                userId=user_id,
                communityId=community_id
            ).single()
            
            if result and result["size"] > 0:
                return {
                    "community_id": result["communityId"],
                    "size": result["size"],
                    "members": result["members"]
                }
            
            return None
    
    def calculate_modularity(
        self,
        user_id: str,
        graph_name: str = None
    ) -> float:
        """
        Calculate modularity score for existing community assignments.
        
        Modularity measures quality of community division.
        Range: [-0.5, 1.0]
        - Higher is better
        - > 0.3 indicates significant community structure
        
        Args:
            user_id: User ID
            graph_name: Projected graph name
            
        Returns:
            Modularity score
        """
        try:
            if graph_name is None:
                graph_name = self._project_graph(user_id)
                should_drop = True
            else:
                should_drop = False
            
            with get_neo4j_session() as session:
                # Use GDS to compute modularity
                query = """
                CALL gds.louvain.stream($graphName)
                YIELD communityId
                WITH count(DISTINCT communityId) as numCommunities
                
                CALL gds.louvain.stream($graphName)
                YIELD nodeId, communityId
                WITH numCommunities, 
                     collect({nodeId: nodeId, communityId: communityId}) as assignments
                
                // Get modularity from algorithm metadata
                CALL gds.louvain.stats($graphName)
                YIELD modularity
                RETURN modularity
                """
                
                result = session.run(query, graphName=graph_name).single()
                
                if result:
                    modularity = result["modularity"]
                    
                    community_modularity.labels(user_id=user_id).set(modularity)
                    
                    self.logger.info(f"Modularity for user {user_id}: {modularity:.3f}")
                    
                    return modularity
                
                return 0.0
                
        except Exception as e:
            self.logger.error(f"Modularity calculation failed: {e}", exc_info=True)
            return 0.0
        
        finally:
            if should_drop and graph_name:
                self._drop_graph(graph_name)
    
    def get_community_statistics(
        self,
        user_id: str
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive community statistics.
        
        Args:
            user_id: User ID
            
        Returns:
            Community statistics
        """
        with get_neo4j_session() as session:
            # Get size distribution
            size_query = """
            MATCH (n)
            WHERE n.user_id = $userId
              AND n.community_id IS NOT NULL
            WITH n.community_id as communityId, count(n) as size
            RETURN 
                count(communityId) as numCommunities,
                avg(size) as avgSize,
                min(size) as minSize,
                max(size) as maxSize,
                stdev(size) as stddevSize,
                collect(size) as sizes
            """
            
            size_result = session.run(size_query, userId=user_id).single()
            
            if not size_result or size_result["numCommunities"] == 0:
                return {
                    "num_communities": 0,
                    "total_nodes": 0,
                    "avg_community_size": 0,
                    "min_community_size": 0,
                    "max_community_size": 0,
                    "stddev_community_size": 0,
                    "size_distribution": {}
                }
            
            # Calculate size distribution
            sizes = size_result["sizes"]
            size_distribution = defaultdict(int)
            for size in sizes:
                # Group by bins: 1-5, 6-10, 11-20, 21-50, 51-100, 100+
                if size <= 5:
                    bin_name = "1-5"
                elif size <= 10:
                    bin_name = "6-10"
                elif size <= 20:
                    bin_name = "11-20"
                elif size <= 50:
                    bin_name = "21-50"
                elif size <= 100:
                    bin_name = "51-100"
                else:
                    bin_name = "100+"
                
                size_distribution[bin_name] += 1
            
            # Get total nodes
            total_query = """
            MATCH (n)
            WHERE n.user_id = $userId
              AND n.community_id IS NOT NULL
            RETURN count(n) as totalNodes
            """
            
            total_result = session.run(total_query, userId=user_id).single()
            
            statistics = {
                "num_communities": size_result["numCommunities"],
                "total_nodes": total_result["totalNodes"],
                "avg_community_size": round(size_result["avgSize"], 2),
                "min_community_size": size_result["minSize"],
                "max_community_size": size_result["maxSize"],
                "stddev_community_size": round(size_result["stddevSize"], 2) if size_result["stddevSize"] else 0,
                "size_distribution": dict(size_distribution),
                "largest_communities": self._get_largest_communities(user_id, top_n=5)
            }
            
            self.logger.info(
                f"Community statistics for user {user_id}: "
                f"{statistics['num_communities']} communities, "
                f"avg size={statistics['avg_community_size']}"
            )
            
            return statistics
    
    def _get_largest_communities(
        self,
        user_id: str,
        top_n: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get largest communities by size.
        
        Args:
            user_id: User ID
            top_n: Number of communities to return
            
        Returns:
            List of largest communities
        """
        with get_neo4j_session() as session:
            query = """
            MATCH (n)
            WHERE n.user_id = $userId
              AND n.community_id IS NOT NULL
            WITH n.community_id as communityId, count(n) as size
            ORDER BY size DESC
            LIMIT $topN
            
            MATCH (m)
            WHERE m.user_id = $userId
              AND m.community_id = communityId
            WITH communityId, size,
                 collect(DISTINCT labels(m)[0]) as nodeTypes,
                 collect(coalesce(m.canonical_name, m.name))[0..3] as sampleMembers
            RETURN communityId, size, nodeTypes, sampleMembers
            ORDER BY size DESC
            """
            
            results = session.run(
                query,
                userId=user_id,
                topN=top_n
            ).data()
            
            return [
                {
                    "community_id": r["communityId"],
                    "size": r["size"],
                    "node_types": r["nodeTypes"],
                    "sample_members": r["sampleMembers"]
                }
                for r in results
            ]
    
    def get_node_community(
        self,
        user_id: str,
        node_id: int
    ) -> Optional[int]:
        """
        Get community ID for a specific node.
        
        Args:
            user_id: User ID
            node_id: Node ID
            
        Returns:
            Community ID or None
        """
        with get_neo4j_session() as session:
            query = """
            MATCH (n)
            WHERE id(n) = $nodeId
              AND n.user_id = $userId
            RETURN n.community_id as communityId
            """
            
            result = session.run(
                query,
                nodeId=node_id,
                userId=user_id
            ).single()
            
            if result:
                return result["communityId"]
            
            return None
    
    def find_community_bridges(
        self,
        user_id: str,
        community_id: int,
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find nodes that bridge a community to other communities.
        
        Bridge nodes have connections to multiple communities.
        
        Args:
            user_id: User ID
            community_id: Source community
            top_k: Number of bridge nodes to return
            
        Returns:
            List of bridge nodes with connection counts
        """
        with get_neo4j_session() as session:
            query = """
            MATCH (n)
            WHERE n.user_id = $userId
              AND n.community_id = $communityId
            
            MATCH (n)-[r]-(other)
            WHERE other.community_id IS NOT NULL
              AND other.community_id <> $communityId
            
            WITH n, other.community_id as targetCommunity, count(r) as connections
            
            WITH n,
                 count(DISTINCT targetCommunity) as numTargetCommunities,
                 sum(connections) as totalExternalConnections,
                 collect({
                     communityId: targetCommunity,
                     connections: connections
                 }) as targets
            
            RETURN 
                id(n) as nodeId,
                coalesce(n.canonical_name, n.name, 'Unknown') as nodeName,
                labels(n)[0] as nodeType,
                numTargetCommunities,
                totalExternalConnections,
                targets
            
            ORDER BY numTargetCommunities DESC, totalExternalConnections DESC
            LIMIT $topK
            """
            
            results = session.run(
                query,
                userId=user_id,
                communityId=community_id,
                topK=top_k
            ).data()
            
            return [
                {
                    "node_id": r["nodeId"],
                    "node_name": r["nodeName"],
                    "node_type": r["nodeType"],
                    "num_target_communities": r["numTargetCommunities"],
                    "total_external_connections": r["totalExternalConnections"],
                    "target_communities": r["targets"]
                }
                for r in results
            ]


# Global service instance
community_service = CommunityService()
