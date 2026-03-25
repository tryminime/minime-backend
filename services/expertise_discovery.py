"""
Expertise Discovery Service

Analyzes the knowledge graph to discover, rank, and profile expertise:
- Skill profiling from entity interactions
- Expertise ranking using centrality + frequency
- Skill gap analysis against target profiles
- Expertise timeline tracking
- Cross-domain bridging detection
"""

from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict
from datetime import datetime, timedelta
import math
import structlog

logger = structlog.get_logger()


# ============================================================================
# SKILL TAXONOMY
# ============================================================================

SKILL_CATEGORIES = {
    'programming_languages': [
        'python', 'javascript', 'typescript', 'java', 'go', 'rust',
        'c++', 'c#', 'ruby', 'swift', 'kotlin', 'php', 'scala',
        'r', 'julia', 'elixir', 'haskell', 'lua', 'perl', 'dart',
    ],
    'frameworks': [
        'react', 'angular', 'vue', 'nextjs', 'django', 'flask',
        'fastapi', 'express', 'spring', 'rails', 'laravel',
        'svelte', 'nuxt', 'gatsby', 'remix', 'astro',
    ],
    'databases': [
        'postgresql', 'mysql', 'mongodb', 'redis', 'neo4j',
        'elasticsearch', 'cassandra', 'dynamodb', 'sqlite',
        'qdrant', 'pinecone', 'weaviate', 'milvus',
    ],
    'cloud_platforms': [
        'aws', 'gcp', 'azure', 'heroku', 'vercel', 'netlify',
        'cloudflare', 'digitalocean', 'fly.io',
    ],
    'devops': [
        'docker', 'kubernetes', 'terraform', 'ansible', 'jenkins',
        'github actions', 'gitlab ci', 'circleci', 'prometheus',
        'grafana', 'datadog', 'nginx', 'traefik',
    ],
    'ai_ml': [
        'tensorflow', 'pytorch', 'scikit-learn', 'spacy', 'huggingface',
        'openai', 'langchain', 'llamaindex', 'stable diffusion',
        'gpt', 'bert', 'transformers', 'rag', 'embeddings',
    ],
    'data_engineering': [
        'spark', 'kafka', 'airflow', 'dbt', 'snowflake',
        'bigquery', 'redshift', 'fivetran', 'dagster',
    ],
    'design': [
        'figma', 'sketch', 'adobe xd', 'photoshop', 'illustrator',
        'blender', 'canva', 'invision',
    ],
    'project_management': [
        'jira', 'linear', 'asana', 'notion', 'confluence',
        'trello', 'monday', 'clickup', 'shortcut',
    ],
    'communication': [
        'slack', 'teams', 'discord', 'zoom', 'meet',
    ],
}

# Reverse lookup: skill → category
_SKILL_TO_CATEGORY: Dict[str, str] = {}
for cat, skills in SKILL_CATEGORIES.items():
    for skill in skills:
        _SKILL_TO_CATEGORY[skill.lower()] = cat


class ExpertiseDiscoveryService:
    """
    Service for discovering and profiling expertise from knowledge graph data.

    Operates on in-memory entity data (passed from Neo4j queries or tests)
    without requiring a live Neo4j connection.
    """

    def __init__(self):
        self.skill_taxonomy = SKILL_CATEGORIES
        self._skill_to_category = _SKILL_TO_CATEGORY

    def build_skill_profile(
        self,
        entities: List[Dict[str, Any]],
        occurrences: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Build a skill profile from a user's entity data.

        Args:
            entities: List of entity dicts with keys: text, type/label,
                      frequency, confidence, first_seen, last_seen
            occurrences: Optional entity occurrences for temporal weighting

        Returns:
            Skill profile with categories, scores, and top skills
        """
        skill_scores: Dict[str, float] = defaultdict(float)
        skill_frequency: Dict[str, int] = defaultdict(int)
        skill_last_seen: Dict[str, str] = {}
        skill_first_seen: Dict[str, str] = {}
        category_scores: Dict[str, float] = defaultdict(float)

        for entity in entities:
            text = (entity.get('text') or entity.get('canonical_name', '')).strip()
            label = entity.get('type') or entity.get('label', '')
            frequency = entity.get('frequency', 1)
            confidence = entity.get('confidence', 0.5)

            # Only consider TOOL, SKILL, FRAMEWORK, LANGUAGE type entities
            if label not in ('TOOL', 'SKILL', 'FRAMEWORK', 'LANGUAGE', 'ORG', 'PRODUCT'):
                continue

            text_lower = text.lower()

            # Calculate score: frequency * confidence with log dampening
            score = math.log1p(frequency) * confidence

            skill_scores[text_lower] += score
            skill_frequency[text_lower] += frequency

            # Track temporal bounds
            first_seen = entity.get('first_seen', '')
            last_seen = entity.get('last_seen', '')
            if first_seen:
                if text_lower not in skill_first_seen or first_seen < skill_first_seen[text_lower]:
                    skill_first_seen[text_lower] = first_seen
            if last_seen:
                if text_lower not in skill_last_seen or last_seen > skill_last_seen[text_lower]:
                    skill_last_seen[text_lower] = last_seen

            # Map to category
            category = self._skill_to_category.get(text_lower, 'other')
            category_scores[category] += score

        # Normalize scores to 0-100
        max_score = max(skill_scores.values()) if skill_scores else 1.0
        normalized_skills = {}
        for skill, score in skill_scores.items():
            normalized_skills[skill] = {
                'score': round((score / max_score) * 100, 1),
                'raw_score': round(score, 3),
                'frequency': skill_frequency[skill],
                'category': self._skill_to_category.get(skill, 'other'),
                'first_seen': skill_first_seen.get(skill, ''),
                'last_seen': skill_last_seen.get(skill, ''),
            }

        # Sort by score descending
        top_skills = sorted(
            normalized_skills.items(),
            key=lambda x: x[1]['score'],
            reverse=True
        )

        # Normalize category scores
        max_cat_score = max(category_scores.values()) if category_scores else 1.0
        normalized_categories = {
            cat: round((score / max_cat_score) * 100, 1)
            for cat, score in category_scores.items()
        }

        return {
            'skills': dict(top_skills),
            'top_skills': [s[0] for s in top_skills[:10]],
            'categories': normalized_categories,
            'primary_category': max(normalized_categories, key=normalized_categories.get) if normalized_categories else 'unknown',
            'total_skills': len(normalized_skills),
            'skill_diversity': len(set(
                self._skill_to_category.get(s, 'other')
                for s in skill_scores.keys()
            )),
        }

    def rank_expertise(
        self,
        entities: List[Dict[str, Any]],
        topic: Optional[str] = None,
        centrality_scores: Optional[Dict[str, float]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Rank entities by expertise level.

        Args:
            entities: Entity dicts with frequency, confidence, centrality
            topic: Optional topic to filter by
            centrality_scores: Optional centrality scores keyed by entity text

        Returns:
            Ranked list of expertise entries
        """
        rankings = []
        centrality_scores = centrality_scores or {}

        for entity in entities:
            text = (entity.get('text') or entity.get('canonical_name', '')).strip()
            text_lower = text.lower()
            label = entity.get('type') or entity.get('label', '')

            # Filter by topic if specified
            if topic:
                topic_lower = topic.lower()
                category = self._skill_to_category.get(text_lower)
                if topic_lower != text_lower and topic_lower != category:
                    continue

            frequency = entity.get('frequency', 1)
            confidence = entity.get('confidence', 0.5)
            centrality = centrality_scores.get(text_lower, 0.0)

            # Composite expertise score
            freq_component = math.log1p(frequency) * 0.4
            conf_component = confidence * 0.3
            cent_component = centrality * 0.3

            expertise_score = freq_component + conf_component + cent_component

            # Determine expertise level
            if expertise_score >= 2.0:
                level = 'expert'
            elif expertise_score >= 1.0:
                level = 'proficient'
            elif expertise_score >= 0.5:
                level = 'intermediate'
            else:
                level = 'beginner'

            rankings.append({
                'entity': text,
                'label': label,
                'expertise_score': round(expertise_score, 3),
                'level': level,
                'frequency': frequency,
                'confidence': confidence,
                'centrality': centrality,
                'category': self._skill_to_category.get(text_lower, 'other'),
            })

        # Sort by expertise score descending
        rankings.sort(key=lambda x: x['expertise_score'], reverse=True)
        return rankings

    def analyze_skill_gaps(
        self,
        current_skills: Dict[str, Any],
        target_profile: Dict[str, float],
    ) -> Dict[str, Any]:
        """
        Compare current skills against a target profile.

        Args:
            current_skills: Output from build_skill_profile()['skills']
            target_profile: Dict of skill_name → required_score (0-100)

        Returns:
            Gap analysis with missing, weak, and strong skills
        """
        missing_skills = []
        weak_skills = []
        strong_skills = []
        matched_skills = []

        for skill, required_score in target_profile.items():
            skill_lower = skill.lower()
            current = current_skills.get(skill_lower)

            if current is None:
                missing_skills.append({
                    'skill': skill,
                    'required_score': required_score,
                    'current_score': 0,
                    'gap': required_score,
                    'category': self._skill_to_category.get(skill_lower, 'other'),
                })
            else:
                current_score = current['score']
                gap = required_score - current_score

                entry = {
                    'skill': skill,
                    'required_score': required_score,
                    'current_score': current_score,
                    'gap': round(gap, 1),
                    'category': self._skill_to_category.get(skill_lower, 'other'),
                }

                if gap > 30:
                    weak_skills.append(entry)
                elif gap > 0:
                    matched_skills.append(entry)
                else:
                    strong_skills.append(entry)

        # Sort by gap size
        missing_skills.sort(key=lambda x: x['gap'], reverse=True)
        weak_skills.sort(key=lambda x: x['gap'], reverse=True)

        total_required = len(target_profile)
        total_met = len(strong_skills) + len(matched_skills)
        readiness_pct = round((total_met / total_required * 100), 1) if total_required > 0 else 0

        return {
            'missing_skills': missing_skills,
            'weak_skills': weak_skills,
            'matched_skills': matched_skills,
            'strong_skills': strong_skills,
            'readiness_percentage': readiness_pct,
            'total_required': total_required,
            'total_met': total_met,
            'total_gaps': len(missing_skills) + len(weak_skills),
            'recommendations': self._generate_gap_recommendations(missing_skills, weak_skills),
        }

    def detect_cross_domain_bridges(
        self,
        entities: List[Dict[str, Any]],
        min_domains: int = 2,
    ) -> Dict[str, Any]:
        """
        Identify skills/entities that bridge multiple domain categories.

        Args:
            entities: Entity dicts
            min_domains: Minimum number of domains to qualify as a bridge

        Returns:
            Cross-domain bridging analysis
        """
        # Group entities by category
        category_entities: Dict[str, Set[str]] = defaultdict(set)
        entity_categories: Dict[str, Set[str]] = defaultdict(set)

        for entity in entities:
            text = (entity.get('text') or entity.get('canonical_name', '')).strip().lower()
            label = entity.get('type') or entity.get('label', '')

            if label not in ('TOOL', 'SKILL', 'FRAMEWORK', 'LANGUAGE', 'ORG', 'PRODUCT'):
                continue

            category = self._skill_to_category.get(text, 'other')
            category_entities[category].add(text)
            entity_categories[text].add(category)

        # Find entities appearing in multiple domains (via co-occurrence context)
        # For now, analyze category coverage
        active_categories = [cat for cat, entities in category_entities.items() if len(entities) >= 1]

        # Find inter-category connections
        bridge_entities = []
        for entity_text, cats in entity_categories.items():
            if len(cats) >= min_domains:
                bridge_entities.append({
                    'entity': entity_text,
                    'domains': sorted(list(cats)),
                    'domain_count': len(cats),
                })

        # Calculate diversity score (Shannon entropy)
        total_entities = sum(len(e) for e in category_entities.values())
        diversity_score = 0.0
        if total_entities > 0:
            for cat_entities in category_entities.values():
                p = len(cat_entities) / total_entities
                if p > 0:
                    diversity_score -= p * math.log2(p)

        # Normalize to 0-100
        max_entropy = math.log2(max(len(category_entities), 1))
        normalized_diversity = round(
            (diversity_score / max_entropy * 100) if max_entropy > 0 else 0, 1
        )

        return {
            'active_domains': sorted(active_categories),
            'domain_count': len(active_categories),
            'bridge_entities': bridge_entities,
            'bridge_count': len(bridge_entities),
            'category_distribution': {
                cat: len(entities)
                for cat, entities in sorted(category_entities.items())
            },
            'diversity_score': normalized_diversity,
            'specialization': 'generalist' if normalized_diversity >= 70 else (
                'balanced' if normalized_diversity >= 40 else 'specialist'
            ),
        }

    def build_expertise_timeline(
        self,
        entities: List[Dict[str, Any]],
        interval_days: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Build a timeline showing how expertise evolves over time.

        Args:
            entities: Entity dicts with first_seen/last_seen timestamps
            interval_days: Interval size for timeline buckets

        Returns:
            List of time buckets with skill snapshots
        """
        # Collect all timestamps
        dated_entities = []
        for entity in entities:
            text = (entity.get('text') or entity.get('canonical_name', '')).strip().lower()
            first_seen = entity.get('first_seen', '') or entity.get('created_at', '')
            label = entity.get('type') or entity.get('label', '')

            if label not in ('TOOL', 'SKILL', 'FRAMEWORK', 'LANGUAGE', 'ORG', 'PRODUCT'):
                continue

            if first_seen:
                try:
                    if isinstance(first_seen, str):
                        ts = datetime.fromisoformat(first_seen.replace('Z', '+00:00'))
                    else:
                        ts = first_seen
                    dated_entities.append({'text': text, 'timestamp': ts})
                except (ValueError, TypeError):
                    continue

        if not dated_entities:
            return []

        # Sort by timestamp
        dated_entities.sort(key=lambda x: x['timestamp'])

        # Create time buckets
        start = dated_entities[0]['timestamp']
        end = dated_entities[-1]['timestamp']
        delta = timedelta(days=interval_days)

        timeline = []
        current = start
        cumulative_skills: Set[str] = set()

        while current <= end + delta:
            bucket_end = current + delta
            new_skills = []

            for de in dated_entities:
                if current <= de['timestamp'] < bucket_end:
                    if de['text'] not in cumulative_skills:
                        new_skills.append(de['text'])
                        cumulative_skills.add(de['text'])

            timeline.append({
                'period_start': current.isoformat(),
                'period_end': bucket_end.isoformat(),
                'new_skills': new_skills,
                'new_skill_count': len(new_skills),
                'cumulative_skill_count': len(cumulative_skills),
            })

            current = bucket_end

        return timeline

    def get_skill_category(self, skill_name: str) -> str:
        """Get the category for a skill name."""
        return self._skill_to_category.get(skill_name.lower(), 'other')

    def get_all_categories(self) -> List[str]:
        """Get all skill categories."""
        return sorted(self.skill_taxonomy.keys())

    def _generate_gap_recommendations(
        self,
        missing: List[Dict],
        weak: List[Dict],
    ) -> List[str]:
        """Generate actionable recommendations from skill gaps."""
        recommendations = []

        if missing:
            top_missing = missing[:3]
            for entry in top_missing:
                recommendations.append(
                    f"Learn {entry['skill']} ({entry['category']}) — "
                    f"required score: {entry['required_score']}"
                )

        if weak:
            top_weak = weak[:3]
            for entry in top_weak:
                recommendations.append(
                    f"Improve {entry['skill']} from {entry['current_score']} "
                    f"to {entry['required_score']} (gap: {entry['gap']})"
                )

        if not recommendations:
            recommendations.append("Profile meets all target requirements")

        return recommendations


# Global instance
expertise_discovery = ExpertiseDiscoveryService()
