"""
Subgraph Extraction Service

Extracts meaningful subgraphs from the knowledge graph:
- Ego network (k-hop neighborhood around a focal node)
- Path-based subgraph (all paths between two nodes)
- Topic/community subgraph (nodes related to a category)
- Temporal subgraph (nodes within a time window)
- Filtered extraction (by type, weight, etc.)
- Subgraph statistics (density, clustering, diameter)
"""

from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict, deque
from datetime import datetime
import math
import structlog

logger = structlog.get_logger()


class SubgraphExtractionService:
    """
    Service for extracting meaningful subgraphs from the knowledge graph.

    All methods operate on in-memory graph representations
    (adjacency lists and node dicts) for testability.
    """

    def __init__(self):
        pass

    def extract_ego_network(
        self,
        focal_node: str,
        adjacency: Dict[str, Set[str]],
        k_hops: int = 2,
        max_nodes: int = 200,
        node_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Extract k-hop ego network around a focal node.

        Args:
            focal_node: Center node
            adjacency: Node → set of neighbors
            k_hops: Number of hops to expand
            max_nodes: Maximum nodes in subgraph
            node_metadata: Optional metadata per node

        Returns:
            Ego network with nodes, edges, and statistics
        """
        node_metadata = node_metadata or {}

        visited: Set[str] = set()
        layers: Dict[int, Set[str]] = defaultdict(set)
        queue: deque = deque([(focal_node, 0)])
        visited.add(focal_node)
        layers[0].add(focal_node)

        while queue and len(visited) < max_nodes:
            current, depth = queue.popleft()

            if depth >= k_hops:
                continue

            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited and len(visited) < max_nodes:
                    visited.add(neighbor)
                    layers[depth + 1].add(neighbor)
                    queue.append((neighbor, depth + 1))

        # Build edges within subgraph
        edges = []
        for node in visited:
            for neighbor in adjacency.get(node, set()):
                if neighbor in visited:
                    pair = tuple(sorted([node, neighbor]))
                    edge = {'source': pair[0], 'target': pair[1]}
                    if edge not in edges:
                        edges.append(edge)

        # Deduplicate edges
        unique_edges = []
        seen_edges: Set[Tuple[str, str]] = set()
        for edge in edges:
            pair = (edge['source'], edge['target'])
            if pair not in seen_edges:
                seen_edges.add(pair)
                unique_edges.append(edge)

        nodes = []
        for node in visited:
            depth = 0
            for d, layer_nodes in layers.items():
                if node in layer_nodes:
                    depth = d
                    break

            node_info = {
                'id': node,
                'depth': depth,
                'is_focal': node == focal_node,
            }
            if node in node_metadata:
                node_info['metadata'] = node_metadata[node]
            nodes.append(node_info)

        stats = self._compute_subgraph_stats(visited, adjacency)

        return {
            'focal_node': focal_node,
            'k_hops': k_hops,
            'nodes': nodes,
            'edges': unique_edges,
            'node_count': len(visited),
            'edge_count': len(unique_edges),
            'layers': {d: sorted(list(ns)) for d, ns in layers.items()},
            'statistics': stats,
        }

    def extract_path_subgraph(
        self,
        source: str,
        target: str,
        adjacency: Dict[str, Set[str]],
        max_depth: int = 5,
        max_paths: int = 10,
    ) -> Dict[str, Any]:
        """
        Extract subgraph containing all paths between two nodes.

        Args:
            source: Start node
            target: End node
            adjacency: Node → set of neighbors
            max_depth: Maximum path length
            max_paths: Maximum paths to find

        Returns:
            Path subgraph with nodes, edges, and paths
        """
        paths = []
        all_nodes: Set[str] = set()
        all_edges: Set[Tuple[str, str]] = set()

        # BFS to find all paths
        queue: deque = deque([(source, [source])])

        while queue and len(paths) < max_paths:
            current, path = queue.popleft()

            if len(path) > max_depth:
                continue

            if current == target:
                paths.append(list(path))
                for node in path:
                    all_nodes.add(node)
                for i in range(len(path) - 1):
                    all_edges.add(tuple(sorted([path[i], path[i + 1]])))
                continue

            for neighbor in adjacency.get(current, set()):
                if neighbor not in path:
                    queue.append((neighbor, path + [neighbor]))

        return {
            'source': source,
            'target': target,
            'paths': paths,
            'path_count': len(paths),
            'nodes': sorted(list(all_nodes)),
            'edges': [{'source': e[0], 'target': e[1]} for e in all_edges],
            'node_count': len(all_nodes),
            'edge_count': len(all_edges),
        }

    def extract_topic_subgraph(
        self,
        topic: str,
        adjacency: Dict[str, Set[str]],
        node_topics: Dict[str, List[str]],
        include_neighbors: bool = True,
    ) -> Dict[str, Any]:
        """
        Extract subgraph for nodes related to a specific topic.

        Args:
            topic: Topic/community name
            adjacency: Node → set of neighbors
            node_topics: Node → list of associated topics
            include_neighbors: Include 1-hop neighbors of topic nodes

        Returns:
            Topic-filtered subgraph
        """
        topic_lower = topic.lower()

        # Find nodes with this topic
        topic_nodes: Set[str] = set()
        for node, topics in node_topics.items():
            if any(t.lower() == topic_lower for t in topics):
                topic_nodes.add(node)

        # Optionally include 1-hop neighbors
        expanded_nodes = set(topic_nodes)
        if include_neighbors:
            for node in topic_nodes:
                for neighbor in adjacency.get(node, set()):
                    expanded_nodes.add(neighbor)

        # Build edges
        edges: Set[Tuple[str, str]] = set()
        for node in expanded_nodes:
            for neighbor in adjacency.get(node, set()):
                if neighbor in expanded_nodes:
                    edges.add(tuple(sorted([node, neighbor])))

        nodes = []
        for node in expanded_nodes:
            nodes.append({
                'id': node,
                'is_topic_member': node in topic_nodes,
                'is_neighbor': node not in topic_nodes,
                'topics': node_topics.get(node, []),
            })

        stats = self._compute_subgraph_stats(expanded_nodes, adjacency)

        return {
            'topic': topic,
            'nodes': nodes,
            'edges': [{'source': e[0], 'target': e[1]} for e in edges],
            'node_count': len(expanded_nodes),
            'edge_count': len(edges),
            'topic_member_count': len(topic_nodes),
            'neighbor_count': len(expanded_nodes) - len(topic_nodes),
            'statistics': stats,
        }

    def extract_temporal_subgraph(
        self,
        adjacency: Dict[str, Set[str]],
        node_timestamps: Dict[str, str],
        start_time: str,
        end_time: str,
        edge_timestamps: Optional[Dict[Tuple[str, str], str]] = None,
    ) -> Dict[str, Any]:
        """
        Extract subgraph with nodes/edges within a time window.

        Args:
            adjacency: Node → set of neighbors
            node_timestamps: Node → ISO timestamp
            start_time: Window start (ISO format)
            end_time: Window end (ISO format)
            edge_timestamps: Optional (source, target) → timestamp

        Returns:
            Temporal subgraph
        """
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))

        # Filter nodes by timestamp
        temporal_nodes: Set[str] = set()
        for node, ts_str in node_timestamps.items():
            try:
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                if start_dt <= ts <= end_dt:
                    temporal_nodes.add(node)
            except (ValueError, TypeError):
                continue

        # Build edges
        edges: Set[Tuple[str, str]] = set()
        for node in temporal_nodes:
            for neighbor in adjacency.get(node, set()):
                if neighbor in temporal_nodes:
                    pair = tuple(sorted([node, neighbor]))

                    # Check edge timestamp if provided
                    if edge_timestamps:
                        edge_ts_str = edge_timestamps.get(pair)
                        if edge_ts_str:
                            try:
                                edge_ts = datetime.fromisoformat(edge_ts_str.replace('Z', '+00:00'))
                                if not (start_dt <= edge_ts <= end_dt):
                                    continue
                            except (ValueError, TypeError):
                                pass

                    edges.add(pair)

        stats = self._compute_subgraph_stats(temporal_nodes, adjacency)

        return {
            'start_time': start_time,
            'end_time': end_time,
            'nodes': sorted(list(temporal_nodes)),
            'edges': [{'source': e[0], 'target': e[1]} for e in edges],
            'node_count': len(temporal_nodes),
            'edge_count': len(edges),
            'statistics': stats,
        }

    def extract_filtered_subgraph(
        self,
        adjacency: Dict[str, Set[str]],
        node_metadata: Dict[str, Dict[str, Any]],
        node_types: Optional[List[str]] = None,
        min_degree: int = 0,
        min_weight: float = 0.0,
        edge_weights: Optional[Dict[Tuple[str, str], float]] = None,
    ) -> Dict[str, Any]:
        """
        Extract subgraph with flexible filters.

        Args:
            adjacency: Full graph adjacency
            node_metadata: Node → metadata dict (must include 'type')
            node_types: Node types to include (None = all)
            min_degree: Minimum degree to include
            min_weight: Minimum edge weight to include
            edge_weights: Optional edge weight lookup

        Returns:
            Filtered subgraph
        """
        edge_weights = edge_weights or {}

        # Filter nodes
        filtered_nodes: Set[str] = set()
        for node in adjacency:
            meta = node_metadata.get(node, {})

            # Type filter
            if node_types:
                node_type = meta.get('type', '')
                if node_type not in node_types:
                    continue

            # Degree filter
            degree = len(adjacency.get(node, set()))
            if degree < min_degree:
                continue

            filtered_nodes.add(node)

        # Filter edges
        edges: Set[Tuple[str, str]] = set()
        for node in filtered_nodes:
            for neighbor in adjacency.get(node, set()):
                if neighbor in filtered_nodes:
                    pair = tuple(sorted([node, neighbor]))
                    weight = edge_weights.get(pair, 1.0)
                    if weight >= min_weight:
                        edges.add(pair)

        stats = self._compute_subgraph_stats(filtered_nodes, adjacency)

        return {
            'filters': {
                'node_types': node_types,
                'min_degree': min_degree,
                'min_weight': min_weight,
            },
            'nodes': sorted(list(filtered_nodes)),
            'edges': [{'source': e[0], 'target': e[1]} for e in edges],
            'node_count': len(filtered_nodes),
            'edge_count': len(edges),
            'statistics': stats,
        }

    # ========================================================================
    # SUBGRAPH STATISTICS
    # ========================================================================

    def _compute_subgraph_stats(
        self,
        nodes: Set[str],
        adjacency: Dict[str, Set[str]],
    ) -> Dict[str, Any]:
        """
        Compute statistics for a subgraph.

        Returns density, average degree, clustering coefficient, and diameter estimate.
        """
        n = len(nodes)
        if n <= 1:
            return {
                'density': 0.0,
                'average_degree': 0.0,
                'clustering_coefficient': 0.0,
                'diameter_estimate': 0,
            }

        # Count internal edges
        internal_edges = 0
        degree_sum = 0
        for node in nodes:
            internal_degree = len(adjacency.get(node, set()) & nodes)
            degree_sum += internal_degree
            internal_edges += internal_degree
        internal_edges //= 2  # Undirected

        max_edges = n * (n - 1) / 2
        density = internal_edges / max_edges if max_edges > 0 else 0.0
        avg_degree = degree_sum / n

        # Local clustering coefficient (sample for performance)
        cc_sum = 0.0
        cc_count = 0
        sample_nodes = list(nodes)[:50]  # Sample for large graphs

        for node in sample_nodes:
            neighbors_in_subgraph = adjacency.get(node, set()) & nodes
            k = len(neighbors_in_subgraph)
            if k < 2:
                continue

            # Count edges among neighbors
            neighbor_edges = 0
            neighbor_list = list(neighbors_in_subgraph)
            for i in range(len(neighbor_list)):
                for j in range(i + 1, len(neighbor_list)):
                    if neighbor_list[j] in adjacency.get(neighbor_list[i], set()):
                        neighbor_edges += 1

            possible = k * (k - 1) / 2
            cc_sum += neighbor_edges / possible if possible > 0 else 0.0
            cc_count += 1

        clustering = cc_sum / cc_count if cc_count > 0 else 0.0

        # Diameter estimate (BFS from a random node, if small enough)
        diameter = 0
        if n <= 100:
            start = next(iter(nodes))
            distances = self._bfs_distances(start, adjacency, nodes)
            if distances:
                diameter = max(distances.values())

        return {
            'density': round(density, 4),
            'average_degree': round(avg_degree, 2),
            'clustering_coefficient': round(clustering, 4),
            'diameter_estimate': diameter,
        }

    @staticmethod
    def _bfs_distances(
        start: str,
        adjacency: Dict[str, Set[str]],
        allowed_nodes: Set[str],
    ) -> Dict[str, int]:
        """BFS distances from start node within allowed nodes."""
        distances = {start: 0}
        queue: deque = deque([start])

        while queue:
            current = queue.popleft()
            for neighbor in adjacency.get(current, set()):
                if neighbor in allowed_nodes and neighbor not in distances:
                    distances[neighbor] = distances[current] + 1
                    queue.append(neighbor)

        return distances


# Global instance
subgraph_extraction_service = SubgraphExtractionService()
