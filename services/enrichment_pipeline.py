"""
Enrichment Pipeline — Orchestrator for all data enrichment stages.

Runs a multi-stage pipeline on activity data:
1. NER (Named Entity Recognition with custom patterns)
2. Normalize (canonical forms + aliases)
3. Spelling Correction (tech-aware)
4. Temporal Enrichment (time context)
5. Auto-Tagging (domain/app/title classification)
6. Context Enrichment (URL, Git, document type)
7. Cross-Activity Resolution (entity dedup across platforms)

Each stage is independent and fault-tolerant — if one stage fails,
the pipeline continues with available data.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import structlog
import time as _time

logger = structlog.get_logger()


class EnrichmentPipeline:
    """
    Orchestrator for the complete data enrichment pipeline.

    Runs all enrichment stages in sequence, collecting results
    and metrics from each stage.
    """

    def __init__(self):
        """Initialize pipeline with all enrichment services."""
        self._nlp = None
        self._auto_tagger = None
        self._spelling = None
        self._temporal = None
        self._resolver = None

    @property
    def nlp(self):
        if self._nlp is None:
            from services.spacy_bert_ner import spacy_bert_ner
            self._nlp = spacy_bert_ner
        return self._nlp

    @property
    def auto_tagger(self):
        if self._auto_tagger is None:
            from services.auto_tagger import auto_tagger
            self._auto_tagger = auto_tagger
        return self._auto_tagger

    @property
    def spelling(self):
        if self._spelling is None:
            from services.spelling_correction import spelling_corrector
            self._spelling = spelling_corrector
        return self._spelling

    @property
    def temporal(self):
        if self._temporal is None:
            from services.temporal_enrichment import temporal_enricher
            self._temporal = temporal_enricher
        return self._temporal

    @property
    def resolver(self):
        if self._resolver is None:
            from services.cross_activity_resolver import cross_activity_resolver
            self._resolver = cross_activity_resolver
        return self._resolver

    def enrich_activity(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run full enrichment pipeline on a single activity.

        Args:
            activity: Activity dict with keys: title, url, domain, app_name,
                      context, timestamp, duration_seconds, etc.

        Returns:
            Dict with:
            - entities: List[Dict] — extracted entities
            - tags: Dict — auto-tagging results
            - temporal: Dict — temporal enrichment
            - context: Dict — context enrichment
            - spelling_corrections: List[Dict] — corrections applied
            - pipeline_metadata: Dict — metrics and timing
        """
        start_time = _time.time()
        result: Dict[str, Any] = {
            'entities': [],
            'tags': {},
            'temporal': {},
            'context': {},
            'spelling_corrections': [],
            'pipeline_metadata': {
                'started_at': datetime.utcnow().isoformat(),
                'stages_completed': [],
                'stages_failed': [],
                'stage_timings': {},
            }
        }

        # Build text blob for NER
        text_blob = self._build_text_blob(activity)
        context = activity.get('context', {}) if isinstance(activity.get('context'), dict) else {}

        # Stage 1: NER (Named Entity Recognition)
        stage_start = _time.time()
        try:
            # The new SpacyBertNER service handles text+context internally and does language detection
            entities = self.nlp.extract_entities(text_blob, context=context)
            result['entities'] = entities
            result['pipeline_metadata']['stages_completed'].append('ner')
        except Exception as e:
            logger.error("Pipeline stage failed: NER", error=str(e))
            result['pipeline_metadata']['stages_failed'].append('ner')
        result['pipeline_metadata']['stage_timings']['ner'] = round(_time.time() - stage_start, 4)

        # Stage 2: Spelling Correction on entities
        stage_start = _time.time()
        try:
            corrections = []
            for entity in result.get('entities', []):
                correction = self.spelling.correct_entity_name(entity['text'])
                if correction['was_corrected']:
                    entity['text'] = correction['corrected']
                    entity['original_text'] = correction['original']
                    corrections.append(correction)
            result['spelling_corrections'] = corrections
            result['pipeline_metadata']['stages_completed'].append('spelling')
        except Exception as e:
            logger.error("Pipeline stage failed: spelling", error=str(e))
            result['pipeline_metadata']['stages_failed'].append('spelling')
        result['pipeline_metadata']['stage_timings']['spelling'] = round(_time.time() - stage_start, 4)

        # Stage 3: Temporal Enrichment
        stage_start = _time.time()
        try:
            temporal = self.temporal.enrich_temporal(activity)
            result['temporal'] = temporal
            result['pipeline_metadata']['stages_completed'].append('temporal')
        except Exception as e:
            logger.error("Pipeline stage failed: temporal", error=str(e))
            result['pipeline_metadata']['stages_failed'].append('temporal')
        result['pipeline_metadata']['stage_timings']['temporal'] = round(_time.time() - stage_start, 4)

        # Stage 4: Auto-Tagging
        stage_start = _time.time()
        try:
            # Pass entities to auto-tagger for entity-based tagging
            activity_with_entities = {**activity, 'entities': result.get('entities', [])}
            tags = self.auto_tagger.auto_tag_activity(activity_with_entities)
            result['tags'] = tags
            result['pipeline_metadata']['stages_completed'].append('auto_tag')
        except Exception as e:
            logger.error("Pipeline stage failed: auto_tag", error=str(e))
            result['pipeline_metadata']['stages_failed'].append('auto_tag')
        result['pipeline_metadata']['stage_timings']['auto_tag'] = round(_time.time() - stage_start, 4)

        # Stage 5: Context Enrichment
        stage_start = _time.time()
        try:
            # We still need enrich_context from the old nlp_service, so we load it just for this
            from services.nlp_service import nlp_service
            ctx_enrichment = nlp_service.enrich_context(activity)
            result['context'] = ctx_enrichment
            result['pipeline_metadata']['stages_completed'].append('context')
        except Exception as e:
            logger.error("Pipeline stage failed: context", error=str(e))
            result['pipeline_metadata']['stages_failed'].append('context')
        result['pipeline_metadata']['stage_timings']['context'] = round(_time.time() - stage_start, 4)

        # Final metadata
        total_time = round(_time.time() - start_time, 4)
        result['pipeline_metadata']['total_time_seconds'] = total_time
        result['pipeline_metadata']['entity_count'] = len(result.get('entities', []))
        result['pipeline_metadata']['tag_count'] = len(result.get('tags', {}).get('tags', []))
        result['pipeline_metadata']['completed_at'] = datetime.utcnow().isoformat()

        logger.info(
            "Enrichment pipeline complete",
            entity_count=result['pipeline_metadata']['entity_count'],
            tag_count=result['pipeline_metadata']['tag_count'],
            total_time=total_time,
            stages_completed=result['pipeline_metadata']['stages_completed'],
            stages_failed=result['pipeline_metadata']['stages_failed'],
        )

        return result

    def enrich_batch(self, activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Run enrichment pipeline on a batch of activities.

        Args:
            activities: List of activity dicts

        Returns:
            List of enrichment results (one per activity)
        """
        results = []
        for activity in activities:
            try:
                result = self.enrich_activity(activity)
                results.append(result)
            except Exception as e:
                logger.error("Batch enrichment failed for activity", error=str(e))
                results.append({
                    'entities': [],
                    'tags': {},
                    'temporal': {},
                    'context': {},
                    'spelling_corrections': [],
                    'pipeline_metadata': {'error': str(e)},
                })

        logger.info("Batch enrichment complete", count=len(results))
        return results

    def get_pipeline_stages(self) -> List[str]:
        """Return ordered list of pipeline stages."""
        return ['ner', 'spelling', 'temporal', 'auto_tag', 'context']

    def _build_text_blob(self, activity: Dict[str, Any]) -> str:
        """Build text blob from activity fields for NER processing."""
        parts = []
        context = activity.get('context', {}) if isinstance(activity.get('context'), dict) else {}

        # Title is most important
        title = activity.get('title', '') or context.get('title', '')
        if title:
            parts.append(title)

        # URL for domain entities
        url = context.get('url', '') or activity.get('url', '')
        if url:
            parts.append(url)

        # App name
        app_name = context.get('app_name', '') or activity.get('app_name', '')
        if app_name:
            parts.append(app_name)

        # Any additional text content
        body = context.get('body', '') or context.get('content', '') or context.get('description', '')
        if body:
            parts.append(body[:2000])  # Limit body text

        return '. '.join(parts)


# Global instance
enrichment_pipeline = EnrichmentPipeline()
