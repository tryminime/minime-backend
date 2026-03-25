"""
NLP Service for Named Entity Recognition.

Provides a singleton service for loading and using spaCy models,
with enhanced work-entity pattern matching, batch processing,
and context enrichment.
"""

import re
import spacy
from spacy.language import Language
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
from datetime import datetime
import structlog

logger = structlog.get_logger()


# ============================================================================
# CUSTOM WORK-ENTITY PATTERNS
# ============================================================================

TECH_PATTERNS = [
    # Programming languages
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "python", "javascript", "typescript", "rust", "golang", "java",
        "kotlin", "swift", "ruby", "php", "scala", "haskell", "elixir",
        "clojure", "dart", "lua", "perl", "r", "matlab", "julia",
        "objective-c", "assembly", "fortran", "cobol", "zig", "nim",
    ]}}]},
    # Frontend frameworks
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "react", "vue", "angular", "svelte", "nextjs", "nuxt",
        "gatsby", "remix", "astro", "solidjs", "preact", "ember",
        "backbone", "alpine", "htmx", "stimulus",
    ]}}]},
    {"label": "TOOL", "pattern": [{"LOWER": "next"}, {"TEXT": "."}, {"LOWER": "js"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "vue"}, {"TEXT": "."}, {"LOWER": "js"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "node"}, {"TEXT": "."}, {"LOWER": "js"}]},
    # Backend frameworks
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "django", "flask", "fastapi", "express", "nestjs", "spring",
        "rails", "laravel", "phoenix", "actix", "axum", "gin",
        "fiber", "echo", "sinatra", "koa", "hapi", "rocket",
    ]}}]},
    # Cloud / infra
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "aws", "gcp", "azure", "kubernetes", "docker", "terraform",
        "ansible", "pulumi", "cloudflare", "vercel", "netlify",
        "heroku", "digitalocean", "linode", "flyio",
    ]}}]},
    {"label": "TOOL", "pattern": [{"LOWER": "google"}, {"LOWER": "cloud"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "amazon"}, {"LOWER": "web"}, {"LOWER": "services"}]},
    # Databases
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "postgresql", "postgres", "mysql", "mongodb", "redis",
        "elasticsearch", "cassandra", "dynamodb", "neo4j", "qdrant",
        "pinecone", "weaviate", "sqlite", "mariadb", "cockroachdb",
        "supabase", "firebase", "couchdb", "influxdb", "clickhouse",
    ]}}]},
    # DevOps / CI-CD tools
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "jenkins", "circleci", "travisci", "gitlab", "github",
        "bitbucket", "jira", "confluence", "grafana", "prometheus",
        "datadog", "sentry", "pagerduty", "opsgenie", "newrelic",
        "splunk", "kibana", "logstash", "fluentd", "jaeger",
    ]}}]},
    {"label": "TOOL", "pattern": [{"LOWER": "github"}, {"LOWER": "actions"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "gitlab"}, {"LOWER": "ci"}]},
    # AI / ML
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "pytorch", "tensorflow", "keras", "scikit-learn", "sklearn",
        "pandas", "numpy", "scipy", "matplotlib", "seaborn",
        "huggingface", "langchain", "llamaindex", "openai",
        "anthropic", "ollama", "mlflow", "wandb", "dvc",
        "spacy", "nltk", "gensim", "transformers",
    ]}}]},
    # Package managers / build tools
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "npm", "yarn", "pnpm", "pip", "cargo", "maven", "gradle",
        "webpack", "vite", "esbuild", "rollup", "parcel", "turbopack",
        "bazel", "cmake", "make", "homebrew",
    ]}}]},
    # Collaboration / messaging
    {"label": "TOOL", "pattern": [{"LOWER": {"IN": [
        "slack", "discord", "teams", "zoom", "figma", "miro",
        "notion", "obsidian", "linear", "asana", "trello",
        "monday", "clickup", "basecamp", "airtable",
    ]}}]},
    {"label": "TOOL", "pattern": [{"LOWER": "microsoft"}, {"LOWER": "teams"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "google"}, {"LOWER": "meet"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "visual"}, {"LOWER": "studio"}, {"LOWER": "code"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "vs"}, {"LOWER": "code"}]},
]

# Document type patterns for context enrichment
DOCUMENT_TYPE_PATTERNS = {
    "code": [
        r"\.(py|js|ts|rs|go|java|kt|rb|php|c|cpp|h|swift|scala)$",
        r"github\.com", r"gitlab\.com", r"bitbucket\.org",
        r"stackoverflow\.com", r"(vscode|intellij|pycharm|sublime)",
    ],
    "documentation": [
        r"docs\.", r"wiki\.", r"readme", r"confluence",
        r"notion\.so", r"gitbook\.io", r"readthedocs",
    ],
    "email": [
        r"(gmail|outlook|mail|protonmail|yahoo)\.",
        r"mail\.google\.com",
    ],
    "chat": [
        r"(slack|discord|teams|telegram|whatsapp)\.",
        r"app\.slack\.com",
    ],
    "meeting": [
        r"(zoom|meet\.google|teams\.microsoft)\.",
        r"(webex|gotomeeting|whereby)\.com",
    ],
    "design": [
        r"(figma|sketch|adobe|canva|miro)\.",
    ],
    "research": [
        r"(arxiv|scholar\.google|semanticscholar|pubmed|researchgate)\.",
        r"\.(pdf)$",
    ],
    "project_management": [
        r"(jira|linear|asana|trello|clickup|monday|basecamp)\.",
    ],
}


class NLPService:
    """
    Singleton service for spaCy model management with enhanced NER.

    Features:
    - Standard spaCy NER for generic entities (PERSON, ORG, GPE, etc.)
    - Custom EntityRuler for 100+ tech/work patterns (TOOL entity type)
    - Batch processing for bulk activity enrichment
    - Context enrichment (URL parsing, document classification, sentiment)
    """

    _instance: Optional['NLPService'] = None
    _nlp = None
    _model_name = None
    _ruler_added = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the service (model loaded lazily on first use)."""
        pass

    def load_model(self, model_name: str = "en_core_web_sm"):
        """
        Load spaCy model with custom EntityRuler patterns.

        Args:
            model_name: spaCy model to load
        """
        if self._nlp is not None and self._model_name == model_name:
            return

        try:
            logger.info("Loading spaCy model", model=model_name)
            self._nlp = spacy.load(model_name)
            self._model_name = model_name
            self._add_work_entity_patterns()
            logger.info("spaCy model loaded with custom patterns", model=model_name)
        except OSError as e:
            logger.error(
                "spaCy model not found",
                model=model_name,
                error=str(e),
                hint=f"Run: python -m spacy download {model_name}"
            )
            raise

    def _add_work_entity_patterns(self):
        """Add custom EntityRuler patterns for work/tech entities."""
        if self._ruler_added or self._nlp is None:
            return

        try:
            ruler = self._nlp.add_pipe("entity_ruler", before="ner")
            ruler.add_patterns(TECH_PATTERNS)
            self._ruler_added = True
            logger.info(
                "Custom EntityRuler added",
                pattern_count=len(TECH_PATTERNS)
            )
        except Exception as e:
            logger.warning("Failed to add EntityRuler", error=str(e))

    def extract_entities(self, text: str) -> List[Dict]:
        """
        Extract named entities from text using spaCy NER.

        Returns:
            List of entity dicts: {text, label, start, end, confidence}
        """
        if self._nlp is None:
            self.load_model()

        if not text or len(text.strip()) == 0:
            return []

        max_length = 10000
        if len(text) > max_length:
            logger.warning("Text truncated for NER", original_length=len(text))
            text = text[:max_length]

        try:
            doc = self._nlp(text)
            entities = []

            for ent in doc.ents:
                confidence = self._calculate_confidence(ent)
                entities.append({
                    'text': ent.text,
                    'label': ent.label_,
                    'start': ent.start_char,
                    'end': ent.end_char,
                    'confidence': confidence
                })

            logger.debug("Entities extracted", count=len(entities))
            return entities

        except Exception as e:
            logger.error("NER extraction failed", error=str(e))
            return []

    def extract_entities_enhanced(self, text: str, context: Optional[Dict] = None) -> List[Dict]:
        """
        Enhanced entity extraction combining spaCy NER + custom rules + regex.

        Adds:
        - TOOL entities from EntityRuler patterns
        - URL-based entity hints (GitHub repos, doc sites)
        - Context-aware confidence boosting

        Args:
            text: Input text to process
            context: Optional context dict with url, domain, app_name, etc.

        Returns:
            List of enriched entity dicts with source field
        """
        entities = self.extract_entities(text)

        # Mark source for standard NER entities
        for e in entities:
            e['source'] = 'spacy_ner'

        # Extract additional entities from context
        if context:
            url = context.get('url', '')
            domain = context.get('domain', '')
            app_name = context.get('app_name', '')

            # GitHub repo detection from URL
            if url and 'github.com' in url:
                match = re.search(r'github\.com/([\w.-]+)/([\w.-]+)', url)
                if match:
                    repo_name = f"{match.group(1)}/{match.group(2)}"
                    entities.append({
                        'text': repo_name,
                        'label': 'PROJECT',
                        'start': 0,
                        'end': 0,
                        'confidence': 0.90,
                        'source': 'url_pattern'
                    })

            # App name as entity
            if app_name and len(app_name) > 2:
                entities.append({
                    'text': app_name,
                    'label': 'TOOL',
                    'start': 0,
                    'end': 0,
                    'confidence': 0.85,
                    'source': 'context'
                })

        # Deduplicate by text+label
        seen = set()
        unique_entities = []
        for e in entities:
            key = (e['text'].lower(), e['label'])
            if key not in seen:
                seen.add(key)
                unique_entities.append(e)

        return unique_entities

    def extract_entities_batch(self, texts: List[str]) -> List[List[Dict]]:
        """
        Batch extract entities from multiple texts using spaCy pipe.

        More efficient than calling extract_entities() in a loop.

        Args:
            texts: List of texts to process

        Returns:
            List of entity lists (one per input text)
        """
        if self._nlp is None:
            self.load_model()

        if not texts:
            return []

        max_length = 10000
        cleaned = [t[:max_length] if t and len(t) > max_length else (t or "") for t in texts]

        results = []
        try:
            for doc in self._nlp.pipe(cleaned, batch_size=32):
                entities = []
                for ent in doc.ents:
                    entities.append({
                        'text': ent.text,
                        'label': ent.label_,
                        'start': ent.start_char,
                        'end': ent.end_char,
                        'confidence': self._calculate_confidence(ent),
                        'source': 'spacy_ner'
                    })
                results.append(entities)

            logger.info("Batch NER complete", text_count=len(texts), total_entities=sum(len(r) for r in results))
        except Exception as e:
            logger.error("Batch NER failed", error=str(e))
            results = [[] for _ in texts]

        return results

    def enrich_context(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich activity with contextual metadata.

        Adds: document_type, url_components, git_context, urgency estimate.

        Args:
            activity: Activity dict with keys like url, title, app_name, context

        Returns:
            Enrichment dict with derived metadata
        """
        enrichment: Dict[str, Any] = {}
        context = activity.get('context', {}) if isinstance(activity.get('context'), dict) else {}
        url = context.get('url', '') or activity.get('url', '') or ''
        title = activity.get('title', '') or context.get('title', '') or ''
        app_name = context.get('app_name', '') or activity.get('app_name', '') or ''

        # 1. Document type classification
        enrichment['document_type'] = self._classify_document_type(url, title, app_name)

        # 2. URL parsing
        if url:
            enrichment['url_components'] = self._parse_url(url)

        # 3. Git context extraction
        git_ctx = self._extract_git_context(url, title)
        if git_ctx:
            enrichment['git_context'] = git_ctx

        # 4. Urgency/sentiment estimation
        enrichment['urgency'] = self._estimate_urgency(title)

        return enrichment

    def _classify_document_type(self, url: str, title: str, app_name: str) -> str:
        """Classify activity into document type category."""
        combined = f"{url} {title} {app_name}".lower()

        for doc_type, patterns in DOCUMENT_TYPE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, combined, re.IGNORECASE):
                    return doc_type

        return "other"

    def _parse_url(self, url: str) -> Dict[str, str]:
        """Parse URL into components."""
        try:
            parsed = urlparse(url)
            return {
                'domain': parsed.netloc,
                'path': parsed.path,
                'scheme': parsed.scheme,
                'query': parsed.query or '',
                'path_segments': [s for s in parsed.path.split('/') if s],
            }
        except Exception:
            return {'domain': '', 'path': '', 'scheme': '', 'query': '', 'path_segments': []}

    def _extract_git_context(self, url: str, title: str) -> Optional[Dict[str, str]]:
        """Extract Git repository context from URL or title."""
        # GitHub/GitLab URL patterns
        for platform in ['github.com', 'gitlab.com', 'bitbucket.org']:
            if platform in url:
                match = re.search(
                    rf'{re.escape(platform)}/([\w.-]+)/([\w.-]+)(?:/(?:tree|blob|pull|issues|commit)/(.+))?',
                    url
                )
                if match:
                    ctx = {
                        'platform': platform.split('.')[0],
                        'owner': match.group(1),
                        'repo': match.group(2),
                    }
                    if match.group(3):
                        ctx['ref'] = match.group(3).split('/')[0]
                    return ctx

        # Title patterns like "repo - file.py - VSCode"
        vscode_match = re.search(r'(.+?)\s*[-–]\s*(.+?)\s*[-–]\s*Visual Studio Code', title)
        if vscode_match:
            return {
                'platform': 'local',
                'file': vscode_match.group(1).strip(),
                'project': vscode_match.group(2).strip(),
            }

        return None

    def _estimate_urgency(self, title: str) -> str:
        """Estimate urgency from activity title keywords."""
        title_lower = title.lower()
        high_urgency = ['urgent', 'critical', 'blocker', 'hotfix', 'p0', 'outage', 'incident', 'emergency']
        medium_urgency = ['important', 'deadline', 'asap', 'priority', 'p1', 'review needed', 'bug']

        if any(kw in title_lower for kw in high_urgency):
            return 'high'
        if any(kw in title_lower for kw in medium_urgency):
            return 'medium'
        return 'low'

    def _calculate_confidence(self, ent) -> float:
        """
        Calculate confidence score for extracted entity.

        Uses heuristics: label reliability + entity length.
        """
        label_confidence = {
            'PERSON': 0.85, 'ORG': 0.80, 'GPE': 0.85,
            'PRODUCT': 0.75, 'WORK_OF_ART': 0.70, 'LAW': 0.75,
            'EVENT': 0.70, 'FAC': 0.75, 'LOC': 0.80,
            'NORP': 0.70, 'LANGUAGE': 0.75,
            'DATE': 0.90, 'TIME': 0.90, 'MONEY': 0.90,
            'QUANTITY': 0.85, 'ORDINAL': 0.85, 'CARDINAL': 0.85,
            'PERCENT': 0.90,
            'TOOL': 0.88,     # Custom work-entity patterns
            'PROJECT': 0.85,  # Custom project patterns
        }.get(ent.label_, 0.70)

        length_bonus = min(0.10, len(ent.text) / 200)
        confidence = min(0.95, label_confidence + length_bonus)
        return round(confidence, 2)

    def get_model(self):
        """Get the loaded spaCy model."""
        if self._nlp is None:
            self.load_model()
        return self._nlp

    def get_model_name(self) -> Optional[str]:
        """Get the name of the currently loaded model."""
        return self._model_name


# Global singleton instance
nlp_service = NLPService()
