"""
Learning Path Recommendations Service

Generates personalized learning paths through the knowledge graph:
- Prerequisite inference from co-occurrence patterns
- BFS/Dijkstra path generation between topics
- Difficulty estimation based on depth and complexity
- Personalized recommendations using user's existing skills
- Path scoring and ranking
"""

from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict, deque
import math
import heapq
import structlog

logger = structlog.get_logger()


# ============================================================================
# TOPIC DIFFICULTY WEIGHTS
# ============================================================================

TOPIC_DIFFICULTY_WEIGHTS = {
    'beginner': 1.0,
    'intermediate': 2.0,
    'advanced': 3.0,
    'expert': 4.0,
}


class LearningPathService:
    """
    Service for generating and ranking learning paths.

    Works with in-memory topic/entity data extracted from the knowledge graph.
    """

    def __init__(self):
        self.difficulty_weights = TOPIC_DIFFICULTY_WEIGHTS

    def infer_prerequisites(
        self,
        topics: List[Dict[str, Any]],
        co_occurrences: List[Dict[str, Any]],
        min_strength: float = 0.3,
    ) -> List[Dict[str, Any]]:
        """
        Infer prerequisite relationships between topics based on co-occurrence
        patterns and temporal order.

        Args:
            topics: List of topic dicts with keys: id, name, frequency, first_seen
            co_occurrences: List of co-occurrence dicts with keys:
                           source, target, count, strength
            min_strength: Minimum strength to consider as prerequisite

        Returns:
            List of inferred prerequisite relationships
        """
        prerequisites = []

        # Build temporal ordering
        topic_first_seen = {}
        for topic in topics:
            tid = topic.get('id') or topic.get('name', '')
            first_seen = topic.get('first_seen', '')
            topic_first_seen[tid] = first_seen

        for co_occ in co_occurrences:
            source = co_occ.get('source', '')
            target = co_occ.get('target', '')
            strength = co_occ.get('strength', 0.0)
            count = co_occ.get('count', 0)

            if strength < min_strength:
                continue

            # Determine prerequisite direction:
            # The topic that appeared first is likely the prerequisite
            source_first = topic_first_seen.get(source, '')
            target_first = topic_first_seen.get(target, '')

            if source_first and target_first:
                if source_first < target_first:
                    prereq, advanced = source, target
                else:
                    prereq, advanced = target, source
            else:
                # Higher frequency → more foundational → prerequisite
                source_topic = next((t for t in topics if (t.get('id') or t.get('name')) == source), {})
                target_topic = next((t for t in topics if (t.get('id') or t.get('name')) == target), {})

                source_freq = source_topic.get('frequency', 0)
                target_freq = target_topic.get('frequency', 0)

                if source_freq >= target_freq:
                    prereq, advanced = source, target
                else:
                    prereq, advanced = target, source

            confidence = min(strength * (1 + math.log1p(count) * 0.1), 1.0)

            prerequisites.append({
                'prerequisite': prereq,
                'advanced_topic': advanced,
                'strength': round(strength, 3),
                'co_occurrence_count': count,
                'confidence': round(confidence, 3),
                'direction_basis': 'temporal' if (source_first and target_first) else 'frequency',
            })

        # Sort by confidence descending
        prerequisites.sort(key=lambda x: x['confidence'], reverse=True)
        return prerequisites

    def find_learning_paths(
        self,
        source: str,
        target: str,
        adjacency: Dict[str, List[Dict[str, Any]]],
        max_depth: int = 7,
        max_paths: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Find learning paths between two topics using BFS.

        Args:
            source: Source topic ID/name
            target: Target topic ID/name
            adjacency: Adjacency list: topic → list of {neighbor, weight, type}
            max_depth: Maximum path length
            max_paths: Maximum paths to return

        Returns:
            List of paths, each with nodes, total_cost, and difficulty
        """
        if source == target:
            return [{
                'path': [source],
                'total_steps': 1,
                'total_cost': 0.0,
                'difficulty': 'beginner',
            }]

        # BFS to find all paths up to max_depth
        paths = []
        queue: deque = deque()
        queue.append((source, [source], 0.0))
        visited_paths: Set[Tuple[str, ...]] = set()

        while queue and len(paths) < max_paths:
            current, path, cost = queue.popleft()

            if len(path) > max_depth:
                continue

            if current == target:
                path_tuple = tuple(path)
                if path_tuple not in visited_paths:
                    visited_paths.add(path_tuple)
                    difficulty = self._estimate_difficulty(len(path))
                    paths.append({
                        'path': list(path),
                        'total_steps': len(path),
                        'total_cost': round(cost, 3),
                        'difficulty': difficulty,
                    })
                continue

            neighbors = adjacency.get(current, [])
            for neighbor_info in neighbors:
                neighbor = neighbor_info.get('neighbor', '')
                weight = neighbor_info.get('weight', 1.0)

                if neighbor not in path:  # Avoid cycles
                    queue.append((
                        neighbor,
                        path + [neighbor],
                        cost + weight,
                    ))

        # Sort by total_cost ascending (shortest/easiest first)
        paths.sort(key=lambda x: x['total_cost'])
        return paths[:max_paths]

    def find_shortest_path(
        self,
        source: str,
        target: str,
        adjacency: Dict[str, List[Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        """
        Find the shortest path using Dijkstra's algorithm.

        Args:
            source: Source topic
            target: Target topic
            adjacency: Topic adjacency list

        Returns:
            Shortest path or None
        """
        distances: Dict[str, float] = {source: 0}
        predecessors: Dict[str, Optional[str]] = {source: None}
        pq = [(0.0, source)]
        visited: Set[str] = set()

        while pq:
            dist, current = heapq.heappop(pq)

            if current in visited:
                continue
            visited.add(current)

            if current == target:
                # Reconstruct path
                path = []
                node: Optional[str] = target
                while node is not None:
                    path.append(node)
                    node = predecessors[node]
                path.reverse()

                return {
                    'path': path,
                    'total_steps': len(path),
                    'total_cost': round(dist, 3),
                    'difficulty': self._estimate_difficulty(len(path)),
                }

            for neighbor_info in adjacency.get(current, []):
                neighbor = neighbor_info.get('neighbor', '')
                weight = neighbor_info.get('weight', 1.0)

                if neighbor in visited:
                    continue

                new_dist = dist + weight
                if new_dist < distances.get(neighbor, float('inf')):
                    distances[neighbor] = new_dist
                    predecessors[neighbor] = current
                    heapq.heappush(pq, (new_dist, neighbor))

        return None  # No path found

    def recommend_paths(
        self,
        source: str,
        adjacency: Dict[str, List[Dict[str, Any]]],
        user_skills: Optional[Set[str]] = None,
        max_depth: int = 5,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Recommend learning paths from source, personalized by user's skills.

        Args:
            source: Starting topic
            adjacency: Topic adjacency list
            user_skills: Set of topics the user already knows
            max_depth: Maximum exploration depth
            top_k: Number of recommendations

        Returns:
            Ranked learning path recommendations
        """
        user_skills = user_skills or set()
        recommendations = []

        # BFS to find all reachable topics
        visited: Set[str] = set()
        queue: deque = deque()
        queue.append((source, [source], 0))

        reachable_paths: Dict[str, List[str]] = {}

        while queue:
            current, path, depth = queue.popleft()

            if depth > max_depth:
                continue

            if current in visited:
                continue
            visited.add(current)

            # Record path to each reachable topic
            if current != source and current not in user_skills:
                reachable_paths[current] = path

            for neighbor_info in adjacency.get(current, []):
                neighbor = neighbor_info.get('neighbor', '')
                if neighbor not in visited:
                    queue.append((neighbor, path + [neighbor], depth + 1))

        # Score and rank paths
        for target, path in reachable_paths.items():
            # Paths through known skills are easier
            known_steps = sum(1 for step in path if step in user_skills)
            unknown_steps = len(path) - known_steps

            # Score: shorter and more familiar paths score higher
            novelty_ratio = unknown_steps / max(len(path), 1)
            relevance_score = 1.0 / max(len(path), 1)
            familiarity_bonus = known_steps * 0.1

            score = relevance_score + familiarity_bonus + (novelty_ratio * 0.5)

            recommendations.append({
                'target': target,
                'path': path,
                'total_steps': len(path),
                'known_steps': known_steps,
                'new_steps': unknown_steps,
                'difficulty': self._estimate_difficulty(len(path)),
                'relevance_score': round(score, 3),
            })

        # Sort by relevance score descending
        recommendations.sort(key=lambda x: x['relevance_score'], reverse=True)
        return recommendations[:top_k]

    def score_path(
        self,
        path: List[str],
        topic_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
        user_skills: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        """
        Score a specific learning path.

        Args:
            path: List of topic IDs/names
            topic_metadata: Optional dict of topic → {difficulty, duration_hours, ...}
            user_skills: Topics the user already knows

        Returns:
            Path scoring breakdown
        """
        topic_metadata = topic_metadata or {}
        user_skills = user_skills or set()

        total_duration = 0.0
        difficulty_sum = 0.0
        known_count = 0
        step_details = []

        for i, topic in enumerate(path):
            meta = topic_metadata.get(topic, {})
            diff = meta.get('difficulty', 'intermediate')
            duration = meta.get('duration_hours', 2.0)
            is_known = topic in user_skills

            diff_weight = self.difficulty_weights.get(diff, 2.0)
            effective_duration = duration * 0.2 if is_known else duration

            total_duration += effective_duration
            difficulty_sum += diff_weight
            if is_known:
                known_count += 1

            step_details.append({
                'step': i + 1,
                'topic': topic,
                'difficulty': diff,
                'estimated_hours': round(effective_duration, 1),
                'already_known': is_known,
            })

        avg_difficulty = difficulty_sum / max(len(path), 1)

        return {
            'path': path,
            'total_steps': len(path),
            'steps': step_details,
            'total_estimated_hours': round(total_duration, 1),
            'average_difficulty': round(avg_difficulty, 2),
            'overall_difficulty': self._estimate_difficulty(len(path)),
            'known_topics': known_count,
            'new_topics': len(path) - known_count,
            'completion_percentage': round(known_count / max(len(path), 1) * 100, 1),
        }

    def _estimate_difficulty(self, path_length: int) -> str:
        """Estimate difficulty based on path length."""
        if path_length <= 2:
            return 'beginner'
        elif path_length <= 4:
            return 'intermediate'
        elif path_length <= 6:
            return 'advanced'
        else:
            return 'expert'


# Global instance
learning_path_service = LearningPathService()
