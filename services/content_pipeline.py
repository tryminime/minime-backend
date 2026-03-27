"""
Content Pipeline — unified NLP orchestrator for Phase 3.

Runs all NLP enrichment steps in sequence:
1. Entity extraction (spaCy NER via nlp_service)
2. Key phrase extraction (YAKE — pure Python, no GPU)
3. Topic classification (zero-shot via sentence-transformers)
4. Language detection (langdetect)
5. Reading time estimation

Used by the content ingestion API and document extractor endpoints.
"""

from dataclasses import dataclass, field
from typing import Optional
import re
import math
import structlog

# Lazy import — spaCy not installed on Render (desktop-only)
try:
    from services.nlp_service import nlp_service
except ImportError:
    nlp_service = None  # type: ignore

logger = structlog.get_logger()


# ============================================================================
# TOPIC TAXONOMY
# ============================================================================

TOPICS = [
    "software engineering",
    "machine learning and AI",
    "data science",
    "product management",
    "design and UX",
    "DevOps and infrastructure",
    "finance and business",
    "science and research",
    "news and current events",
    "entertainment and media",
    "health and wellness",
    "education and learning",
    "legal and compliance",
    "marketing and sales",
    "human resources",
    "cybersecurity",
    "cloud computing",
    "mobile development",
    "web development",
    "databases",
]


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class TopicResult:
    topic: str
    confidence: float
    secondary_topics: list = field(default_factory=list)


@dataclass
class ContentAnalysis:
    """Full NLP analysis of a piece of text content."""
    # Core text stats
    word_count: int = 0
    char_count: int = 0
    sentence_count: int = 0
    reading_time_seconds: int = 0   # at 200 WPM average

    # Language
    language: str = "en"
    language_confidence: float = 1.0

    # Key phrases
    keyphrases: list = field(default_factory=list)   # list[str]

    # Entities from spaCy
    entities: list = field(default_factory=list)     # list[dict]

    # Topic
    topic: Optional[TopicResult] = None

    # Complexity score (0-1, Flesch-Kincaid based)
    complexity: float = 0.5

    def to_dict(self) -> dict:
        return {
            "word_count": self.word_count,
            "char_count": self.char_count,
            "sentence_count": self.sentence_count,
            "reading_time_seconds": self.reading_time_seconds,
            "language": self.language,
            "keyphrases": self.keyphrases[:15],
            "entities": self.entities[:20],
            "topic": {
                "primary": self.topic.topic,
                "confidence": round(self.topic.confidence, 3),
                "secondary": self.topic.secondary_topics,
            } if self.topic else None,
            "complexity": round(self.complexity, 3),
        }


# ============================================================================
# CONTENT PIPELINE
# ============================================================================

class ContentPipeline:
    """
    Orchestrates all NLP enrichment steps for a text document.

    Designed to be called from:
    - POST /api/v1/content/ingest  (browser-extracted page content)
    - POST /api/v1/documents/extract  (uploaded PDF/DOCX/etc.)
    - Background tasks
    """

    # Reading speed at 200 WPM (average) — in words per minute
    READING_WPM = 200

    def process(self, text: str, context: Optional[dict] = None) -> ContentAnalysis:
        """
        Run full NLP pipeline on text.

        Args:
            text:    Input text content
            context: Optional context dict with url, domain, app_name, etc.

        Returns:
            ContentAnalysis with all enrichment results
        """
        if not text or not text.strip():
            return ContentAnalysis()

        # Cap text to avoid excessive processing time
        text = text[:100_000]
        analysis = ContentAnalysis()

        # 1. Basic stats
        words = text.split()
        analysis.word_count = len(words)
        analysis.char_count = len(text)
        analysis.sentence_count = max(1, len(re.split(r"[.!?]+", text)))
        analysis.reading_time_seconds = max(1, math.ceil(analysis.word_count / self.READING_WPM * 60))

        # 2. Language detection
        analysis.language, analysis.language_confidence = self._detect_language(text)

        # 3. Key phrases (YAKE)
        analysis.keyphrases = self.extract_keyphrases(text, n=12)

        # 4. Entity extraction (spaCy NER)
        try:
            analysis.entities = nlp_service.extract_entities_enhanced(text, context=context)
        except Exception as e:
            logger.warning("entity_extraction_failed", error=str(e))
            analysis.entities = []

        # 5. Topic classification
        analysis.topic = self.classify_topic(text)

        # 6. Reading complexity
        analysis.complexity = self._flesch_kincaid_grade(text, words)

        logger.info(
            "content_pipeline_complete",
            words=analysis.word_count,
            keyphrases=len(analysis.keyphrases),
            entities=len(analysis.entities),
            topic=analysis.topic.topic if analysis.topic else None,
            lang=analysis.language,
        )

        return analysis

    # -------------------------------------------------------------------------
    # Key Phrase Extraction (YAKE)
    # -------------------------------------------------------------------------

    def extract_keyphrases(self, text: str, n: int = 10) -> list:
        """
        Extract key phrases using YAKE (Yet Another Keyword Extractor).

        YAKE is pure Python, no GPU needed, works offline.
        Falls back to simple TF frequency if YAKE not installed.
        """
        try:
            import yake  # type: ignore
            kw_extractor = yake.KeywordExtractor(
                lan="en",
                n=3,           # max n-gram size
                dedupLim=0.9,
                top=n,
                features=None,
            )
            keywords = kw_extractor.extract_keywords(text)
            # YAKE returns (keyword, score) — lower score = more relevant
            return [kw for kw, _score in keywords]
        except ImportError:
            logger.debug("yake_not_installed_using_frequency_fallback")
            return self._frequency_keyphrases(text, n)
        except Exception as e:
            logger.warning("yake_extraction_failed", error=str(e))
            return self._frequency_keyphrases(text, n)

    def _frequency_keyphrases(self, text: str, n: int) -> list:
        """Simple term-frequency fallback when YAKE is not available."""
        # Remove stopwords and punctuation
        stopwords = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
            "for", "of", "with", "by", "from", "is", "are", "was", "were",
            "been", "be", "have", "has", "had", "do", "does", "did", "will",
            "would", "could", "should", "may", "might", "this", "that", "it",
            "its", "they", "them", "their", "we", "our", "you", "your",
        }
        words = re.findall(r"\b[a-zA-Z]{3,}\b", text.lower())
        freq: dict = {}
        for w in words:
            if w not in stopwords:
                freq[w] = freq.get(w, 0) + 1
        sorted_terms = sorted(freq.items(), key=lambda x: x[1], reverse=True)
        return [term for term, _count in sorted_terms[:n]]

    # -------------------------------------------------------------------------
    # Topic Classification
    # -------------------------------------------------------------------------

    def classify_topic(self, text: str) -> Optional[TopicResult]:
        """
        Classify text into one of ~20 work topics.

        Uses zero-shot classification via sentence-transformers similarity
        if the model is loaded; falls back to keyword-based classification.
        """
        try:
            return self._topic_via_keywords(text)
        except Exception as e:
            logger.warning("topic_classification_failed", error=str(e))
            return TopicResult(topic="other", confidence=0.5)

    def _topic_via_keywords(self, text: str) -> TopicResult:
        """Keyword-based topic classification (no ML model needed)."""
        text_lower = text.lower()

        # Keyword hints for topics
        hints = {
            "software engineering": [
                "function", "class", "variable", "algorithm", "api", "code",
                "programming", "software", "developer", "github", "debug",
                "refactor", "lint", "test", "deploy", "build",
            ],
            "machine learning and AI": [
                "model", "training", "neural", "embedding", "inference",
                "llm", "gpt", "pytorch", "tensorflow", "dataset", "accuracy",
                "loss", "gradient", "fine-tune", "transformer",
            ],
            "DevOps and infrastructure": [
                "docker", "kubernetes", "ci/cd", "pipeline", "deployment",
                "server", "cloud", "aws", "gcp", "azure", "terraform",
                "monitoring", "logs", "container",
            ],
            "data science": [
                "dataframe", "pandas", "numpy", "visualization", "chart",
                "analysis", "statistics", "correlation", "regression",
                "jupyter", "notebook", "csv", "sql query",
            ],
            "cybersecurity": [
                "vulnerability", "exploit", "cve", "attack", "encryption",
                "firewall", "authentication", "token", "breach", "pentest",
            ],
            "design and UX": [
                "figma", "wireframe", "prototype", "ui", "ux", "user experience",
                "typography", "color", "mockup", "component", "design system",
            ],
            "finance and business": [
                "revenue", "profit", "investor", "funding", "valuation",
                "budget", "roi", "kpi", "market", "startup", "saas",
            ],
            "health and wellness": [
                "health", "fitness", "sleep", "nutrition", "exercise",
                "mental health", "stress", "meditation", "diet",
            ],
        }

        scores = {}
        for topic, keywords in hints.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score > 0:
                scores[topic] = score

        if not scores:
            return TopicResult(topic="general", confidence=0.5)

        # Normalize scores
        max_score = max(scores.values())
        sorted_topics = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        primary_topic = sorted_topics[0][0]
        confidence = min(0.95, sorted_topics[0][1] / 10)

        secondary = [t for t, _s in sorted_topics[1:4] if _s >= 2]

        return TopicResult(
            topic=primary_topic,
            confidence=confidence,
            secondary_topics=secondary,
        )

    # -------------------------------------------------------------------------
    # Language Detection
    # -------------------------------------------------------------------------

    def _detect_language(self, text: str) -> tuple:
        """Detect language with confidence. Returns (lang_code, confidence)."""
        try:
            from langdetect import detect_langs  # type: ignore
            langs = detect_langs(text[:5000])
            if langs:
                top = langs[0]
                return str(top.lang), round(float(top.prob), 3)
        except ImportError:
            logger.debug("langdetect_not_installed")
        except Exception:
            pass
        return "en", 1.0

    # -------------------------------------------------------------------------
    # Reading Complexity (Flesch-Kincaid Grade Level)
    # -------------------------------------------------------------------------

    def _flesch_kincaid_grade(self, text: str, words: list) -> float:
        """
        Compute a normalized complexity score (0-1) based on Flesch-Kincaid.
        Higher = harder to read.
        """
        if not words:
            return 0.5

        num_words = len(words)
        num_sentences = max(1, len(re.split(r"[.!?]+", text)))
        num_syllables = sum(self._count_syllables(w) for w in words[:1000])

        # Flesch-Kincaid Grade Level
        fkgl = (
            0.39 * (num_words / num_sentences)
            + 11.8 * (num_syllables / num_words)
            - 15.59
        )
        # Grade 1-16 → normalize to 0-1
        return max(0.0, min(1.0, fkgl / 16))

    def _count_syllables(self, word: str) -> int:
        """Rough syllable count via vowel group heuristic."""
        word = word.lower().rstrip("e")
        count = len(re.findall(r"[aeiou]+", word))
        return max(1, count)


# Global singleton
content_pipeline = ContentPipeline()
