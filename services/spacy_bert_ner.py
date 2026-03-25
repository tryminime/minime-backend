"""
spaCy + BERT NER Pipeline — Dedicated entity extraction service.

Provides multi-language Named Entity Recognition using:
1. spaCy transformer pipeline (BERT-backed) for high-accuracy English NER
2. Multi-language support via HuggingFace transformers (bert-base-multilingual-cased)
3. Language detection to auto-route text to the correct pipeline
4. Fallback chain: BERT pipeline → spaCy sm model → regex NER

Entity types extracted:
- PERSON, ORG, GPE, LOC, FAC, EVENT, PRODUCT, WORK_OF_ART
- TOOL (custom work/tech patterns via EntityRuler)
- PROJECT (custom patterns for code repositories)
"""

import re
from typing import Optional, List, Dict, Any, Tuple
from functools import lru_cache
import structlog

logger = structlog.get_logger()

# Language detection
try:
    from langdetect import detect as _detect_lang, DetectorFactory
    DetectorFactory.seed = 42  # Deterministic detection
    HAS_LANGDETECT = True
except ImportError:
    HAS_LANGDETECT = False

# ── Supported Languages ──────────────────────────────────────────────────────

# spaCy model names by language code
SPACY_MODELS: Dict[str, str] = {
    "en": "en_core_web_sm",
    "de": "de_core_news_sm",
    "fr": "fr_core_news_sm",
    "es": "es_core_news_sm",
    "pt": "pt_core_news_sm",
    "it": "it_core_news_sm",
    "nl": "nl_core_news_sm",
    "zh": "zh_core_web_sm",
    "ja": "ja_core_news_sm",
    "ko": "ko_core_news_sm",
    "ru": "ru_core_news_sm",
    "pl": "pl_core_news_sm",
    "ro": "ro_core_news_sm",
    "da": "da_core_news_sm",
    "nb": "nb_core_news_sm",  # Norwegian Bokmål
    "sv": "sv_core_news_sm",
    "fi": "fi_core_news_sm",
    "el": "el_core_news_sm",
    "ca": "ca_core_news_sm",
    "hr": "hr_core_news_sm",
    "uk": "uk_core_news_sm",
    "lt": "lt_core_news_sm",
    "mk": "mk_core_news_sm",
    "xx": "xx_ent_wiki_sm",   # Multilingual fallback
}

# HuggingFace NER models by language / group
HF_NER_MODELS: Dict[str, str] = {
    "en": "dslim/bert-base-NER",
    "multilingual": "Davlan/bert-base-multilingual-cased-ner-hrl",
}

# Map HF NER labels → canonical labels
HF_LABEL_MAP = {
    "B-PER": "PERSON", "I-PER": "PERSON",
    "B-ORG": "ORG", "I-ORG": "ORG",
    "B-LOC": "GPE", "I-LOC": "GPE",
    "B-MISC": "MISC", "I-MISC": "MISC",
    "PER": "PERSON", "ORG": "ORG", "LOC": "GPE", "MISC": "MISC",
}

# ── Custom Work-Entity Patterns (shared with nlp_service) ────────────────────

TECH_TOOL_TERMS = {
    "python", "javascript", "typescript", "rust", "golang", "java", "kotlin",
    "swift", "ruby", "php", "scala", "haskell", "elixir", "clojure", "dart",
    "react", "vue", "angular", "svelte", "nextjs", "nuxt", "gatsby", "remix",
    "django", "flask", "fastapi", "express", "nestjs", "spring", "rails", "laravel",
    "aws", "gcp", "azure", "kubernetes", "docker", "terraform", "ansible",
    "postgresql", "postgres", "mysql", "mongodb", "redis", "elasticsearch",
    "neo4j", "qdrant", "sqlite", "dynamodb", "cassandra",
    "pytorch", "tensorflow", "keras", "scikit-learn", "pandas", "numpy",
    "huggingface", "langchain", "openai", "spacy", "transformers",
    "git", "github", "gitlab", "npm", "yarn", "webpack", "vite",
    "slack", "discord", "figma", "notion", "jira", "linear", "asana",
    "jenkins", "circleci", "grafana", "prometheus", "datadog", "sentry",
}


def detect_language(text: str) -> str:
    """Detect the language of the given text. Returns ISO 639-1 code."""
    if not text or len(text.strip()) < 10:
        return "en"  # Default to English for very short text

    if not HAS_LANGDETECT:
        return "en"

    try:
        lang = _detect_lang(text)
        return lang
    except Exception:
        return "en"


class SpacyBertNER:
    """
    Hybrid NER pipeline combining spaCy + BERT transformers.

    Architecture:
        1. Detect text language
        2. Try BERT NER pipeline for high-accuracy extraction
        3. Try language-specific spaCy model for NER
        4. Apply custom EntityRuler patterns for TOOL/PROJECT entities
        5. Merge and deduplicate results

    Multi-language support:
        - English: dslim/bert-base-NER + en_core_web_sm
        - Other languages: bert-base-multilingual-cased-ner-hrl + lang-specific spaCy
        - Unsupported: xx_ent_wiki_sm (multilingual) fallback

    Thread safety: Models loaded lazily and cached. Entity extraction is stateless.
    """

    def __init__(self):
        self._spacy_models: Dict[str, Any] = {}  # lang → loaded spaCy model
        self._hf_pipelines: Dict[str, Any] = {}  # key → loaded HF pipeline
        self._preferred_pipeline: str = "auto"    # auto | bert | spacy | regex

    # ── Model Loading ─────────────────────────────────────────────────────

    def _load_spacy_model(self, lang: str) -> Optional[Any]:
        """Load a spaCy model for the given language code."""
        if lang in self._spacy_models:
            return self._spacy_models[lang]

        model_name = SPACY_MODELS.get(lang)
        if not model_name:
            # Try multilingual fallback
            model_name = SPACY_MODELS.get("xx")

        try:
            import spacy
            nlp = spacy.load(model_name)

            # Add EntityRuler for TOOL patterns (English + multilingual)
            if lang in ("en", "xx") or model_name == SPACY_MODELS.get("xx"):
                try:
                    self._add_tool_patterns(nlp)
                except Exception as e:
                    logger.debug("EntityRuler not added", error=str(e))

            self._spacy_models[lang] = nlp
            logger.info("spaCy model loaded", lang=lang, model=model_name)
            return nlp
        except OSError:
            logger.warning("spaCy model not installed", model=model_name, lang=lang)
            # Try multilingual fallback
            if lang != "xx" and "xx" not in self._spacy_models:
                return self._load_spacy_model("xx")
            # Try English as last resort
            if lang != "en" and "en" not in self._spacy_models:
                return self._load_spacy_model("en")
            return None

    def _add_tool_patterns(self, nlp) -> None:
        """Add custom EntityRuler patterns for tech/work entities."""
        ruler = nlp.add_pipe("entity_ruler", before="ner")
        patterns = []
        for term in TECH_TOOL_TERMS:
            patterns.append({"label": "TOOL", "pattern": [{"LOWER": term}]})
        # Multi-token patterns
        patterns.extend([
            {"label": "TOOL", "pattern": [{"LOWER": "next"}, {"TEXT": "."}, {"LOWER": "js"}]},
            {"label": "TOOL", "pattern": [{"LOWER": "vue"}, {"TEXT": "."}, {"LOWER": "js"}]},
            {"label": "TOOL", "pattern": [{"LOWER": "node"}, {"TEXT": "."}, {"LOWER": "js"}]},
            {"label": "TOOL", "pattern": [{"LOWER": "visual"}, {"LOWER": "studio"}, {"LOWER": "code"}]},
            {"label": "TOOL", "pattern": [{"LOWER": "vs"}, {"LOWER": "code"}]},
            {"label": "TOOL", "pattern": [{"LOWER": "google"}, {"LOWER": "cloud"}]},
            {"label": "TOOL", "pattern": [{"LOWER": "microsoft"}, {"LOWER": "teams"}]},
        ])
        ruler.add_patterns(patterns)

    def _load_hf_pipeline(self, key: str = "en") -> Optional[Any]:
        """Load a HuggingFace NER pipeline."""
        if key in self._hf_pipelines:
            return self._hf_pipelines[key]

        model_name = HF_NER_MODELS.get(key) or HF_NER_MODELS.get("multilingual")
        if not model_name:
            return None

        try:
            from transformers import pipeline as hf_pipeline
            pipe = hf_pipeline(
                "ner",
                model=model_name,
                aggregation_strategy="simple",
                device=-1,  # CPU — set to 0 for GPU
            )
            self._hf_pipelines[key] = pipe
            logger.info("HuggingFace NER pipeline loaded", key=key, model=model_name)
            return pipe
        except Exception as e:
            logger.warning("HuggingFace pipeline failed to load", key=key, error=str(e))
            return None

    # ── Core Extraction ───────────────────────────────────────────────────

    def extract_entities(
        self,
        text: str,
        lang: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        pipeline: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract named entities from text using the best available pipeline.

        Args:
            text: Input text to process
            lang: ISO 639-1 language code (auto-detected if None)
            context: Optional activity context for additional extraction
            pipeline: Force pipeline: "bert", "spacy", "regex", or "auto"

        Returns:
            List of entity dicts: {text, label, start, end, confidence, source, lang}
        """
        if not text or len(text.strip()) < 3:
            return []

        # Truncate very long text
        max_len = 10000
        if len(text) > max_len:
            text = text[:max_len]

        # Detect language
        detected_lang = lang or detect_language(text)
        use_pipeline = pipeline or self._preferred_pipeline

        entities: List[Dict[str, Any]] = []

        # Pipeline selection with fallback chain
        if use_pipeline in ("auto", "bert"):
            bert_entities = self._extract_bert(text, detected_lang)
            if bert_entities:
                entities.extend(bert_entities)
                # If BERT found entities, we still run spaCy for TOOL patterns
                spacy_entities = self._extract_spacy(text, detected_lang)
                # Only add TOOL entities from spaCy (BERT doesn't know those)
                for e in spacy_entities:
                    if e["label"] in ("TOOL", "PROJECT"):
                        entities.append(e)
            elif use_pipeline == "auto":
                # BERT failed, fall through to spaCy
                entities.extend(self._extract_spacy(text, detected_lang))

        elif use_pipeline == "spacy":
            entities.extend(self._extract_spacy(text, detected_lang))

        # Always add regex-based TOOL extraction
        entities.extend(self._extract_tools_regex(text))

        # Context-based extraction
        if context:
            entities.extend(self._extract_from_context(context))

        # Deduplicate
        entities = self._deduplicate(entities)

        # Add language tag
        for e in entities:
            e["lang"] = detected_lang

        return entities

    def _extract_bert(self, text: str, lang: str) -> List[Dict[str, Any]]:
        """Extract entities using HuggingFace BERT NER."""
        key = "en" if lang == "en" else "multilingual"
        pipe = self._load_hf_pipeline(key)
        if not pipe:
            return []

        try:
            # HF pipeline returns list of entity groups
            results = pipe(text[:512])  # BERT has 512 token limit
            entities = []

            for ent in results:
                label = HF_LABEL_MAP.get(ent.get("entity_group", ""), ent.get("entity_group", "MISC"))
                score = ent.get("score", 0.0)

                # Skip low-confidence or very short entities
                if score < 0.5 or len(ent.get("word", "").strip()) < 2:
                    continue

                entities.append({
                    "text": ent["word"].strip(),
                    "label": label,
                    "start": ent.get("start", 0),
                    "end": ent.get("end", 0),
                    "confidence": round(float(score), 3),
                    "source": f"bert_{key}",
                })

            logger.debug("BERT NER extracted", count=len(entities), lang=lang, key=key)
            return entities

        except Exception as e:
            logger.warning("BERT NER failed", error=str(e), lang=lang)
            return []

    def _extract_spacy(self, text: str, lang: str) -> List[Dict[str, Any]]:
        """Extract entities using spaCy model for the given language."""
        nlp = self._load_spacy_model(lang)
        if not nlp:
            # Ultimate fallback to English
            nlp = self._load_spacy_model("en")
            if not nlp:
                return []

        try:
            doc = nlp(text)
            entities = []

            for ent in doc.ents:
                # Calculate confidence based on label
                confidence = {
                    "PERSON": 0.85, "ORG": 0.80, "GPE": 0.85, "LOC": 0.80,
                    "FAC": 0.75, "PRODUCT": 0.75, "EVENT": 0.70,
                    "WORK_OF_ART": 0.70, "LAW": 0.75, "NORP": 0.70,
                    "LANGUAGE": 0.75, "TOOL": 0.88, "PROJECT": 0.85,
                    "DATE": 0.90, "TIME": 0.90, "MONEY": 0.90,
                    "QUANTITY": 0.85, "ORDINAL": 0.85, "CARDINAL": 0.85,
                }.get(ent.label_, 0.70)

                # Skip numeric types
                if ent.label_ in ("DATE", "TIME", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL", "PERCENT"):
                    continue

                entities.append({
                    "text": ent.text,
                    "label": ent.label_,
                    "start": ent.start_char,
                    "end": ent.end_char,
                    "confidence": confidence,
                    "source": f"spacy_{lang}",
                })

            logger.debug("spaCy NER extracted", count=len(entities), lang=lang)
            return entities

        except Exception as e:
            logger.warning("spaCy NER failed", error=str(e), lang=lang)
            return []

    def _extract_tools_regex(self, text: str) -> List[Dict[str, Any]]:
        """Extract TOOL entities via regex word matching."""
        entities = []
        words = set(re.findall(r"[a-z0-9#+.]+", text.lower()))

        for term in TECH_TOOL_TERMS:
            if term in words:
                display = term.capitalize() if len(term) > 3 else term.upper()
                entities.append({
                    "text": display,
                    "label": "TOOL",
                    "start": 0,
                    "end": 0,
                    "confidence": 0.60,
                    "source": "regex_tool",
                })

        return entities

    def _extract_from_context(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract entities from activity context (URL, domain, app)."""
        entities = []

        # GitHub repo from URL
        url = context.get("url", "")
        if url and "github.com" in url:
            match = re.search(r"github\.com/([\w.-]+)/([\w.-]+)", url)
            if match:
                entities.append({
                    "text": f"{match.group(1)}/{match.group(2)}",
                    "label": "PROJECT",
                    "start": 0, "end": 0,
                    "confidence": 0.90,
                    "source": "url_pattern",
                })

        # App name
        app = context.get("app_name", "")
        if app and len(app) > 2:
            entities.append({
                "text": app,
                "label": "TOOL",
                "start": 0, "end": 0,
                "confidence": 0.85,
                "source": "context",
            })

        return entities

    def _deduplicate(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicate entities by (text_lower, label), keeping highest confidence."""
        best: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for e in entities:
            key = (e["text"].lower().strip(), e["label"])
            if key not in best or e["confidence"] > best[key]["confidence"]:
                best[key] = e

        return list(best.values())

    # ── Batch Processing ──────────────────────────────────────────────────

    def extract_entities_batch(
        self,
        texts: List[str],
        lang: Optional[str] = None,
    ) -> List[List[Dict[str, Any]]]:
        """
        Batch extract entities from multiple texts.

        Args:
            texts: List of texts to process
            lang: Force language (auto-detect per text if None)

        Returns:
            List of entity lists (one per input text)
        """
        return [self.extract_entities(text, lang=lang) for text in texts]

    # ── Introspection ─────────────────────────────────────────────────────

    def get_supported_languages(self) -> List[Dict[str, str]]:
        """Return list of supported languages with model info."""
        langs = []
        for code, model in SPACY_MODELS.items():
            if code == "xx":
                continue
            langs.append({
                "code": code,
                "spacy_model": model,
                "bert_model": HF_NER_MODELS.get(code, HF_NER_MODELS.get("multilingual", "none")),
            })
        return langs

    def get_loaded_models(self) -> Dict[str, List[str]]:
        """Return currently loaded models."""
        return {
            "spacy": list(self._spacy_models.keys()),
            "bert": list(self._hf_pipelines.keys()),
        }

    def set_pipeline_preference(self, pipeline: str) -> None:
        """Set preferred pipeline: 'auto', 'bert', 'spacy', or 'regex'."""
        if pipeline in ("auto", "bert", "spacy", "regex"):
            self._preferred_pipeline = pipeline
            logger.info("Pipeline preference set", pipeline=pipeline)


# ── Global singleton ─────────────────────────────────────────────────────────
spacy_bert_ner = SpacyBertNER()
