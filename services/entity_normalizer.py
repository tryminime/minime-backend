"""
Entity Normalizer Service.

Normalizes extracted entities to canonical forms for deduplication and linking.
"""

from typing import Dict, List, Optional
import re
from uuid import UUID
import structlog

logger = structlog.get_logger()


class EntityNormalizer:
    """
    Normalizes extracted entities to canonical forms.
    
    This service:
    - Maps spaCy NER labels to canonical types
    - Cleans entity text (capitalization, punctuation)
    - Extracts external IDs from URLs (ORCID, DOI, GitHub, etc.)
    - Generates aliases for matching
    """
    
    # Map spaCy NER labels to our canonical entity types
    ENTITY_TYPE_MAP = {
        # Core types
        "PERSON": "PERSON",
        "ORG": "ORG",
        
        # Places
        "GPE": "PLACE",  # Geopolitical entity (countries, cities)
        "LOC": "PLACE",  # Non-GPE locations
        "FAC": "PLACE",  # Buildings, airports, highways, bridges
        
        # Tools and products
        "PRODUCT": "TOOL",
        "LANGUAGE": "TOOL",  # Programming languages
        
        # Publications and media
        "WORK_OF_ART": "PAPER",
        "LAW": "PAPER",
        
        # Organizations (alternative forms)
        "NORP": "ORG",  # Nationalities, religious, political groups
        
        # Events
        "EVENT": "EVENT",
        
        # Skip these types (too generic or not useful)
        # "DATE", "TIME", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL", "PERCENT"
    }
    
    # Patterns for extracting external IDs from URLs and text
    EXTERNAL_ID_PATTERNS = {
        'orcid': r'(?:orcid\.org/)?(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])',
        'doi': r'(?:doi\.org/)?(10\.\d{4,}/[^\s]+)',
        'github_user': r'github\.com/([a-zA-Z0-9](?:[a-zA-Z0-9]|-(?=[a-zA-Z0-9])){0,38})(?:/|$)',
        'github_repo': r'github\.com/([a-zA-Z0-9-]+/[a-zA-Z0-9_.-]+)',
        'arxiv': r'arxiv\.org/abs/(\d{4}\.\d{4,5})',
        'linkedin': r'linkedin\.com/in/([a-zA-Z0-9-]+)',
        'twitter': r'(?:twitter|x)\.com/(@?[a-zA-Z0-9_]+)',
    }
    
    def normalize(
        self,
        text: str,
        label: str,
        user_id: UUID,
        context: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Normalize an extracted entity.
        
        Args:
            text: Raw entity text from NER
            label: spaCy NER label (PERSON, ORG, GPE, etc.)
            user_id: User who owns this entity
            context: Activity context (for extracting external IDs)
        
        Returns:
            Normalized entity dict or None if should be skipped:
            {
                'canonical_name': str,
                'type': str,
                'external_ids': dict,
                'aliases': list[str],
                'user_id': UUID
            }
        """
        # Skip labels we don't care about
        canonical_type = self.ENTITY_TYPE_MAP.get(label)
        if not canonical_type:
            logger.debug("Skipping entity with unmapped label", label=label, text=text)
            return None
        
        # Clean entity text
        cleaned = self._clean_text(text, canonical_type)
        
        # Skip if too short or invalid
        if not cleaned or len(cleaned) < 2:
            logger.debug("Skipping short/invalid entity", text=text, cleaned=cleaned)
            return None
        
        # Extract external IDs from context (URLs, etc.)
        external_ids = self._extract_external_ids(context or {})
        
        # Generate aliases for matching
        aliases = self._generate_aliases(cleaned, canonical_type)
        
        return {
            'canonical_name': cleaned,
            'type': canonical_type,
            'external_ids': external_ids,
            'aliases': list(set([cleaned] + aliases)),  # Deduplicate
            'user_id': user_id
        }
    
    def _clean_text(self, text: str, entity_type: str) -> str:
        """
        Clean and normalize entity text.
        
        Args:
            text: Raw entity text
            entity_type: Canonical entity type
        
        Returns:
            Cleaned text
        """
        # Remove extra whitespace
        cleaned = ' '.join(text.split())
        
        # Remove leading/trailing punctuation
        cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', cleaned)
        
        # Title case for PERSON and ORG (but not acronyms)
        if entity_type in ('PERSON', 'ORG'):
            # Check if it's an acronym (all caps)
            if not re.match(r'^[A-Z]{2,}$', cleaned):
                cleaned = cleaned.title()
        
        # Remove possessives
        cleaned = re.sub(r"'s$", '', cleaned)
        
        return cleaned
    
    def _extract_external_ids(self, context: Dict) -> Dict:
        """
        Extract external IDs from activity context.
        
        Checks URL, domain, and other context fields for known patterns.
        
        Args:
            context: Activity context dictionary
        
        Returns:
            Dict of external IDs: {'github': 'username', 'orcid': '0000-...'}
        """
        external_ids = {}
        
        # Get all text fields from context
        text_fields = []
        if 'url' in context:
            text_fields.append(context['url'])
        if 'domain' in context:
            text_fields.append(context['domain'])
        if 'title' in context:
            text_fields.append(context['title'])
        
        # Check each pattern against all text fields
        for id_type, pattern in self.EXTERNAL_ID_PATTERNS.items():
            for text_field in text_fields:
                if not text_field:
                    continue
                    
                match = re.search(pattern, text_field, re.IGNORECASE)
                if match:
                    external_ids[id_type] = match.group(1)
                    break  # Found this ID type, move to next
        
        return external_ids
    
    def _generate_aliases(self, canonical_name: str, entity_type: str) -> List[str]:
        """
        Generate common aliases/variations for entity matching.
        
        Args:
            canonical_name: Cleaned canonical name
            entity_type: Entity type
        
        Returns:
            List of alias strings
        """
        aliases = []
        
        # For PERSON: Generate "LastName, FirstName" variant
        if entity_type == "PERSON" and ',' not in canonical_name:
            parts = canonical_name.split()
            if len(parts) >= 2:
                # "John Doe" → "Doe, John"
                aliases.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
                
                # Also add just last name for partial matching
                aliases.append(parts[-1])
        
        # Lowercase variant (for case-insensitive matching)
        if canonical_name.lower() != canonical_name:
            aliases.append(canonical_name.lower())
        
        # For ORG: Add acronym if applicable
        if entity_type == "ORG":
            # "Open Source Initiative" → "OSI"
            words = canonical_name.split()
            if len(words) >= 2 and all(len(w) > 0 for w in words):
                acronym = ''.join(w[0].upper() for w in words if w[0].isalpha())
                if len(acronym) >= 2:
                    aliases.append(acronym)
        
        # Remove empty strings and duplicates
        aliases = [a for a in aliases if a and len(a) >= 2]
        
        return aliases
    
    def should_merge(self, entity1: Dict, entity2: Dict) -> bool:
        """
        Determine if two entities should be merged (are the same entity).
        
        This is a simple heuristic check. Week 8 will add embedding-based matching.
        
        Args:
            entity1, entity2: Normalized entity dicts
        
        Returns:
            True if entities should be merged
        """
        # Must be same type
        if entity1['type'] != entity2['type']:
            return False
        
        # Exact canonical name match
        if entity1['canonical_name'].lower() == entity2['canonical_name'].lower():
            return True
        
        # Check if either canonical name appears in other's aliases
        if entity1['canonical_name'].lower() in [a.lower() for a in entity2.get('aliases', [])]:
            return True
        if entity2['canonical_name'].lower() in [a.lower() for a in entity1.get('aliases', [])]:
            return True
        
        # Check for matching external IDs
        ext_ids1 = entity1.get('external_ids', {})
        ext_ids2 = entity2.get('external_ids', {})
        
        for key in ext_ids1:
            if key in ext_ids2 and ext_ids1[key] == ext_ids2[key]:
                return True
        
        return False


# Global instance
entity_normalizer = EntityNormalizer()
