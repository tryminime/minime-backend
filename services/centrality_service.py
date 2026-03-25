"""
Centrality Metrics Service
Computes graph centrality metrics using Neo4j Graph Data Science library.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import time

from prometheus_client import Counter, Histogram, Gauge

from config.neo4j_config import get_neo4j_session, get_neo4j_driver

logger = logging.getLogger(__name__)


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

centrality_computation_total = Counter(
    'centrality_computation_total',
    'Total centrality metric computations',
    ['metric_type', 'status']
)

centrality_computation_duration = Histogram(
    'centrality_computation_duration_seconds',
    'Centrality computation duration',
    ['metric_type'],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0]
)

centrality_nodes_processed = Gauge(
    'centrality_nodes_processed',
    'Number of nodes processed in last centrality computation',
    ['user_id', 'metric_type']
)

centrality_graph_size = Gauge(
    'centrality_graph_size_nodes',
    'Size of projected graph for centrality',
    ['user_id']
)


class CentralityService:
    """
    Service for computing graph centrality metrics using Neo4j GDS.
    
    Supports:
    - Degree Centrality
    - Betweenness Centrality
    - Closeness Centrality
    - Eigenvector Centrality
    - PageRank (weighted)
    """
    
    def __init__(self):
        """Initialize centrality service."""
        self.logger = logging.getLogger(__name__)
        self.driver = get_neo4j_driver()
    
    def _project_user_graph(
        self,
        user_id: str,
        graph_name: str = None,
        node_types: List[str] = None,
        relationship_types: List[str] = None
    ) -> str:
        """
        Project user's graph into GDS graph catalog.
        
        Args:
            user_id: User ID for filtering
            graph_name: Name for projected graph (default: user_id + timestamp)
            node_types: Node types to include (default: all)
            relationship_types: Relationship types to include (default: all)
            
        Returns:
            Name of projected graph
        """
        if graph_name is None:
            graph_name = f"user_{user_id}_{int(time.time())}"
        
        # Default to all types
        if node_types is None:
            node_types = [
                "PERSON", "PAPER", "TOPIC", "PROJECT",
                "DATASET", "INSTITUTION", "TOOL", "VENUE"
            ]
        
        if relationship_types is None:
            relationship_types = [
                "AUTHORED", "COLLABORATES_WITH", "WORKS_ON",
                "CONTRIBUTES_TO", "AFFILIATED_WITH", "CITES",
                "USES", "ON_TOPIC", "PUBLISHED_AT", "RELATED_TO", "DEPENDS_ON"
            ]
        
        # Build node projection
        node_projection = {}
        for node_type in node_types:
            node_projection[node_type] = {
                "properties": ["canonical_name"]
            }
        
        # Build relationship projection with weights
        rel_projection = {}
        for rel_type in relationship_types:
            rel_projection[rel_type] = {
                "type": rel_type,
                "orientation": "NATURAL",
                "properties": {
                    "weight": {
                        "property": "weight",
                        "defaultValue": 1.0
                    }
                }
            }
        
        with get_neo4j_session() as session:
            # Drop existing graph if it exists
            try:
                session.run(
                    f"CALL gds.graph.drop($graphName)",
                    graphName=graph_name
                )
            except Exception:
                pass  # Graph doesn't exist, that's fine
            
            # Project graph
            query = """
            CALL gds.graph.project(
                $graphName,
                $nodeProjection,
                $relationshipProjection,
                {
                    nodeProperties: [],
                    relationshipProperties: ['weight']
                }
            )
            YIELD graphName, nodeCount, relationshipCount
            RETURN graphName, nodeCount, relationshipCount
            """
            
            result = session.run(
                query,
                graphName=graph_name,
                nodeProjection=node_projection,
                relationshipProjection=rel_projection
            ).single()
            
            if result:
                node_count = result["nodeCount"]
                rel_count = result["relationshipCount"]
                
                self.logger.info(
                    f"Projected graph '{graph_name}': {node_count} nodes, {rel_count} relationships"
                )
                
                centrality_graph_size.labels(user_id=user_id).set(node_count)
                
                return graph_name
            
            raise RuntimeError(f"Failed to project graph '{graph_name}'")
    
    def _drop_graph(self, graph_name: str):
        """Drop projected graph from catalog."""
        try:
            with get_neo4j_session() as session:
                session.run(
                    "CALL gds.graph.drop($graphName)",
                    graphName=graph_name
                )
            self.logger.info(f"Dropped graph '{graph_name}'")
        except Exception as e:
            self.logger.warning(f"Failed to drop graph '{graph_name}': {e}")
    
    def compute_degree_centrality(
        self,
        user_id: str,
        graph_name: str = None,
        store_results: bool = True
    ) -> Dict[str, Any]:
        """
        Compute Degree Centrality using Neo4j GDS.
        
        Measures: Number of direct connections (neighbors).
        
        Args:
            user_id: User ID
            graph_name: Projected graph name (or project new one)
            store_results: If True, write results back to Neo4j
            
        Returns:
            Computation statistics
        """
        start_time = time.time()
        
        try:
            # Project graph if needed
            if graph_name is None:
                graph_name = self._project_user_graph(user_id)
                should_drop = True
            else:
                should_drop = False
            
            with get_neo4j_session() as session:
                if store_results:
                    # Write mode: store results as node properties
                    query = """
                    CALL gds.degree.write($graphName, {
                        writeProperty: 'degree_centrality'
                    })
                    YIELD centralityDistribution, nodePropertiesWritten
                    RETURN centralityDistribution, nodePropertiesWritten
                    """
                else:
                    # Stream mode: just compute
                    query = """
                    CALL gds.degree.stream($graphName)
                    YIELD nodeId, score
                    RETURN count(*) as nodeCount, 
                           avg(score) as avgScore,
                           max(score) as maxScore
                    """
                
                result = session.run(query, graphName=graph_name).single()
                
                elapsed = time.time() - start_time
                
                # Track metrics
                centrality_computation_total.labels(
                    metric_type='degree',
                    status='success'
                ).inc()
                
                centrality_computation_duration.labels(
                    metric_type='degree'
                ).observe(elapsed)
                
                if store_results and result:
                    nodes_written = result.get("nodePropertiesWritten", 0)
                    centrality_nodes_processed.labels(
                        user_id=user_id,
                        metric_type='degree'
                    ).set(nodes_written)
                
                self.logger.info(
                    f"Degree centrality computed for user {user_id} in {elapsed:.2f}s"
                )
                
                return {
                    "metric": "degree_centrality",
                    "user_id": user_id,
                    "execution_time_sec": elapsed,
                    "status": "success",
                    "result": result.data() if result else {}
                }
                
        except Exception as e:
            elapsed = time.time() - start_time
            
            centrality_computation_total.labels(
                metric_type='degree',
                status='error'
            ).inc()
            
            self.logger.error(f"Degree centrality computation failed: {e}", exc_info=True)
            
            return {
                "metric": "degree_centrality",
                "user_id": user_id,
                "execution_time_sec": elapsed,
                "status": "error",
                "error": str(e)
            }
        
        finally:
            if should_drop:
                self._drop_graph(graph_name)
    
    def compute_betweenness_centrality(
        self,
        user_id: str,
        graph_name: str = None,
        store_results: bool = True
    ) -> Dict[str, Any]:
        """
        Compute Betweenness Centrality using Neo4j GDS.
        
        Measures: How often a node appears on shortest paths between other nodes.
        
        Args:
            user_id: User ID
            graph_name: Projected graph name
            store_results: If True, write results back to Neo4j
            
        Returns:
            Computation statistics
        """
        start_time = time.time()
        
        try:
            if graph_name is None:
                graph_name = self._project_user_graph(user_id)
                should_drop = True
            else:
                should_drop = False
            
            with get_neo4j_session() as session:
                if store_results:
                    query = """
                    CALL gds.betweenness.write($graphName, {
                        writeProperty: 'betweenness_centrality'
                    })
                    YIELD centralityDistribution, nodePropertiesWritten
                    RETURN centralityDistribution, nodePropertiesWritten
                    """
                else:
                    query = """
                    CALL gds.betweenness.stream($graphName)
                    YIELD nodeId, score
                    RETURN count(*) as nodeCount,
                           avg(score) as avgScore,
                           max(score) as maxScore
                    """
                
                result = session.run(query, graphName=graph_name).single()
                
                elapsed = time.time() - start_time
                
                centrality_computation_total.labels(
                    metric_type='betweenness',
                    status='success'
                ).inc()
                
                centrality_computation_duration.labels(
                    metric_type='betweenness'
                ).observe(elapsed)
                
                if store_results and result:
                    nodes_written = result.get("nodePropertiesWritten", 0)
                    centrality_nodes_processed.labels(
                        user_id=user_id,
                        metric_type='betweenness'
                    ).set(nodes_written)
                
                self.logger.info(
                    f"Betweenness centrality computed for user {user_id} in {elapsed:.2f}s"
                )
                
                return {
                    "metric": "betweenness_centrality",
                    "user_id": user_id,
                    "execution_time_sec": elapsed,
                    "status": "success",
                    "result": result.data() if result else {}
                }
                
        except Exception as e:
            elapsed = time.time() - start_time
            
            centrality_computation_total.labels(
                metric_type='betweenness',
                status='error'
            ).inc()
            
            self.logger.error(f"Betweenness centrality computation failed: {e}", exc_info=True)
            
            return {
                "metric": "betweenness_centrality",
                "user_id": user_id,
                "execution_time_sec": elapsed,
                "status": "error",
                "error": str(e)
            }
        
        finally:
            if should_drop:
                self._drop_graph(graph_name)
    
    def compute_closeness_centrality(
        self,
        user_id: str,
        graph_name: str = None,
        store_results: bool = True
    ) -> Dict[str, Any]:
        """
        Compute Closeness Centrality using Neo4j GDS.
        
        Measures: Average distance to all other nodes (how quickly info spreads).
        
        Args:
            user_id: User ID
            graph_name: Projected graph name
            store_results: If True, write results back to Neo4j
            
        Returns:
            Computation statistics
        """
        start_time = time.time()
        
        try:
            if graph_name is None:
                graph_name = self._project_user_graph(user_id)
                should_drop = True
            else:
                should_drop = False
            
            with get_neo4j_session() as session:
                if store_results:
                    query = """
                    CALL gds.closeness.write($graphName, {
                        writeProperty: 'closeness_centrality'
                    })
                    YIELD centralityDistribution, nodePropertiesWritten
                    RETURN centralityDistribution, nodePropertiesWritten
                    """
                else:
                    query = """
                    CALL gds.closeness.stream($graphName)
                    YIELD nodeId, score
                    RETURN count(*) as nodeCount,
                           avg(score) as avgScore,
                           max(score) as maxScore
                    """
                
                result = session.run(query, graphName=graph_name).single()
                
                elapsed = time.time() - start_time
                
                centrality_computation_total.labels(
                    metric_type='closeness',
                    status='success'
                ).inc()
                
                centrality_computation_duration.labels(
                    metric_type='closeness'
                ).observe(elapsed)
                
                if store_results and result:
                    nodes_written = result.get("nodePropertiesWritten", 0)
                    centrality_nodes_processed.labels(
                        user_id=user_id,
                        metric_type='closeness'
                    ).set(nodes_written)
                
                self.logger.info(
                    f"Closeness centrality computed for user {user_id} in {elapsed:.2f}s"
                )
                
                return {
                    "metric": "closeness_centrality",
                    "user_id": user_id,
                    "execution_time_sec": elapsed,
                    "status": "success",
                    "result": result.data() if result else {}
                }
                
        except Exception as e:
            elapsed = time.time() - start_time
            
            centrality_computation_total.labels(
                metric_type='closeness',
                status='error'
            ).inc()
            
            self.logger.error(f"Closeness centrality computation failed: {e}", exc_info=True)
            
            return {
                "metric": "closeness_centrality",
                "user_id": user_id,
                "execution_time_sec": elapsed,
                "status": "error",
                "error": str(e)
            }
        
        finally:
            if should_drop:
                self._drop_graph(graph_name)
    
    def compute_eigenvector_centrality(
        self,
        user_id: str,
        graph_name: str = None,
        store_results: bool = True,
        max_iterations: int = 20
    ) -> Dict[str, Any]:
        """
        Compute Eigenvector Centrality using Neo4j GDS.
        
        Measures: Influence based on connections to other influential nodes.
        
        Args:
            user_id: User ID
            graph_name: Projected graph name
            store_results: If True, write results back to Neo4j
            max_iterations: Maximum iterations for convergence
            
        Returns:
            Computation statistics
        """
        start_time = time.time()
        
        try:
            if graph_name is None:
                graph_name = self._project_user_graph(user_id)
                should_drop = True
            else:
                should_drop = False
            
            with get_neo4j_session() as session:
                if store_results:
                    query = """
                    CALL gds.eigenvector.write($graphName, {
                        writeProperty: 'eigenvector_centrality',
                        maxIterations: $maxIterations
                    })
                    YIELD centralityDistribution, nodePropertiesWritten
                    RETURN centralityDistribution, nodePropertiesWritten
                    """
                else:
                    query = """
                    CALL gds.eigenvector.stream($graphName, {
                        maxIterations: $maxIterations
                    })
                    YIELD nodeId, score
                    RETURN count(*) as nodeCount,
                           avg(score) as avgScore,
                           max(score) as maxScore
                    """
                
                result = session.run(
                    query,
                    graphName=graph_name,
                    maxIterations=max_iterations
                ).single()
                
                elapsed = time.time() - start_time
                
                centrality_computation_total.labels(
                    metric_type='eigenvector',
                    status='success'
                ).inc()
                
                centrality_computation_duration.labels(
                    metric_type='eigenvector'
                ).observe(elapsed)
                
                if store_results and result:
                    nodes_written = result.get("nodePropertiesWritten", 0)
                    centrality_nodes_processed.labels(
                        user_id=user_id,
                        metric_type='eigenvector'
                    ).set(nodes_written)
                
                self.logger.info(
                    f"Eigenvector centrality computed for user {user_id} in {elapsed:.2f}s"
                )
                
                return {
                    "metric": "eigenvector_centrality",
                    "user_id": user_id,
                    "execution_time_sec": elapsed,
                    "status": "success",
                    "result": result.data() if result else {}
                }
                
        except Exception as e:
            elapsed = time.time() - start_time
            
            centrality_computation_total.labels(
                metric_type='eigenvector',
                status='error'
            ).inc()
            
            self.logger.error(f"Eigenvector centrality computation failed: {e}", exc_info=True)
            
            return {
                "metric": "eigenvector_centrality",
                "user_id": user_id,
                "execution_time_sec": elapsed,
                "status": "error",
                "error": str(e)
            }
        
        finally:
            if should_drop:
                self._drop_graph(graph_name)
    
    def compute_pagerank(
        self,
        user_id: str,
        graph_name: str = None,
        store_results: bool = True,
        damping_factor: float = 0.85,
        max_iterations: int = 20,
        use_weights: bool = True
    ) -> Dict[str, Any]:
        """
        Compute PageRank using Neo4j GDS with relationship weights.
        
        Measures: Importance based on incoming links (weighted).
        
        Args:
            user_id: User ID
            graph_name: Projected graph name
            store_results: If True, write results back to Neo4j
            damping_factor: PageRank damping factor (default: 0.85)
            max_iterations: Maximum iterations
            use_weights: If True, use relationship weights
            
        Returns:
            Computation statistics
        """
        start_time = time.time()
        
        try:
            if graph_name is None:
                graph_name = self._project_user_graph(user_id)
                should_drop = True
            else:
                should_drop = False
            
            with get_neo4j_session() as session:
                config = {
                    "dampingFactor": damping_factor,
                    "maxIterations": max_iterations
                }
                
                if use_weights:
                    config["relationshipWeightProperty"] = "weight"
                
                if store_results:
                    query = """
                    CALL gds.pageRank.write($graphName, $config + {
                        writeProperty: 'pagerank'
                    })
                    YIELD centralityDistribution, nodePropertiesWritten
                    RETURN centralityDistribution, nodePropertiesWritten
                    """
                else:
                    query = """
                    CALL gds.pageRank.stream($graphName, $config)
                    YIELD nodeId, score
                    RETURN count(*) as nodeCount,
                           avg(score) as avgScore,
                           max(score) as maxScore
                    """
                
                result = session.run(
                    query,
                    graphName=graph_name,
                    config=config
                ).single()
                
                elapsed = time.time() - start_time
                
                centrality_computation_total.labels(
                    metric_type='pagerank',
                    status='success'
                ).inc()
                
                centrality_computation_duration.labels(
                    metric_type='pagerank'
                ).observe(elapsed)
                
                if store_results and result:
                    nodes_written = result.get("nodePropertiesWritten", 0)
                    centrality_nodes_processed.labels(
                        user_id=user_id,
                        metric_type='pagerank'
                    ).set(nodes_written)
                
                self.logger.info(
                    f"PageRank computed for user {user_id} in {elapsed:.2f}s "
                    f"(weighted={use_weights})"
                )
                
                return {
                    "metric": "pagerank",
                    "user_id": user_id,
                    "execution_time_sec": elapsed,
                    "status": "success",
                    "weighted": use_weights,
                    "result": result.data() if result else {}
                }
                
        except Exception as e:
            elapsed = time.time() - start_time
            
            centrality_computation_total.labels(
                metric_type='pagerank',
                status='error'
            ).inc()
            
            self.logger.error(f"PageRank computation failed: {e}", exc_info=True)
            
            return {
                "metric": "pagerank",
                "user_id": user_id,
                "execution_time_sec": elapsed,
                "status": "error",
                "error": str(e)
            }
        
        finally:
            if should_drop:
                self._drop_graph(graph_name)
    
    def compute_all_metrics(
        self,
        user_id: str,
        metrics: List[str] = None,
        store_results: bool = True
    ) -> Dict[str, Any]:
        """
        Orchestrator: Compute all centrality metrics efficiently.
        
        Projects graph once and reuses for all computations.
        
        Args:
            user_id: User ID
            metrics: List of metrics to compute (default: all 5)
            store_results: If True, write results back to Neo4j
            
        Returns:
            Summary of all computations
        """
        overall_start = time.time()
        
        if metrics is None:
            metrics = [
                "degree",
                "betweenness",
                "closeness",
                "eigenvector",
                "pagerank"
            ]
        
        results = {
            "user_id": user_id,
            "metrics_requested": metrics,
            "metrics_computed": [],
            "metrics_failed": [],
            "total_execution_time_sec": 0,
            "details": {}
        }
        
        try:
            # Project graph once for all computations
            self.logger.info(f"Projecting graph for user {user_id}...")
            graph_name = self._project_user_graph(user_id)
            
            # Compute each metric
            if "degree" in metrics:
                result = self.compute_degree_centrality(
                    user_id, graph_name, store_results
                )
                results["details"]["degree"] = result
                if result["status"] == "success":
                    results["metrics_computed"].append("degree")
                else:
                    results["metrics_failed"].append("degree")
            
            if "betweenness" in metrics:
                result = self.compute_betweenness_centrality(
                    user_id, graph_name, store_results
                )
                results["details"]["betweenness"] = result
                if result["status"] == "success":
                    results["metrics_computed"].append("betweenness")
                else:
                    results["metrics_failed"].append("betweenness")
            
            if "closeness" in metrics:
                result = self.compute_closeness_centrality(
                    user_id, graph_name, store_results
                )
                results["details"]["closeness"] = result
                if result["status"] == "success":
                    results["metrics_computed"].append("closeness")
                else:
                    results["metrics_failed"].append("closeness")
            
            if "eigenvector" in metrics:
                result = self.compute_eigenvector_centrality(
                    user_id, graph_name, store_results
                )
                results["details"]["eigenvector"] = result
                if result["status"] == "success":
                    results["metrics_computed"].append("eigenvector")
                else:
                    results["metrics_failed"].append("eigenvector")
            
            if "pagerank" in metrics:
                result = self.compute_pagerank(
                    user_id, graph_name, store_results
                )
                results["details"]["pagerank"] = result
                if result["status"] == "success":
                    results["metrics_computed"].append("pagerank")
                else:
                    results["metrics_failed"].append("pagerank")
            
            # Drop graph
            self._drop_graph(graph_name)
            
        except Exception as e:
            self.logger.error(f"compute_all_metrics failed: {e}", exc_info=True)
            results["error"] = str(e)
        
        results["total_execution_time_sec"] = time.time() - overall_start
        
        self.logger.info(
            f"Centrality computation complete for user {user_id}: "
            f"{len(results['metrics_computed'])}/{len(metrics)} succeeded "
            f"in {results['total_execution_time_sec']:.2f}s"
        )
        
        return results


# Global service instance
centrality_service = CentralityService()
