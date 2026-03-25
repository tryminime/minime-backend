#!/usr/bin/env python3
"""
Test script for NER functionality.

Tests:
1. NLP service entity extraction
2. Entity normalization
3. End-to-end NER worker task
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from services.nlp_service import nlp_service
from services.entity_normalizer import entity_normalizer
from uuid import UUID


def test_nlp_extraction():
    """Test basic NER extraction."""
    print("=" * 60)
    print("TEST 1: NLP Service - Entity Extraction")
    print("=" * 60)
    
    test_texts = [
        "John Doe is working at Google on the TensorFlow project in Mountain View.",
        "The paper by Marie Curie about radium was published in Nature.",
        "Visit github.com/tryminime/backend to see the code repository.",
        "Microsoft released Visual Studio Code as an open source project.",
    ]
    
    for i, text in enumerate(test_texts, 1):
        print(f"\n{i}. Text: {text}")
        entities = nlp_service.extract_entities(text)
        
        if entities:
            for ent in entities:
                print(f"   [{ent['label']:10}] {ent['text']:25} (confidence: {ent['confidence']:.2f})")
        else:
            print("   No entities found")


def test_entity_normalization():
    """Test entity normalization."""
    print("\n" + "=" * 60)
    print("TEST 2: Entity Normalizer - Text Cleaning & ID Extraction")
    print("=" * 60)
    
    test_cases = [
        {
            'text': 'john doe',
            'label': 'PERSON',
            'context': {}
        },
        {
            'text': 'Google Inc.',
            'label': 'ORG',
            'context': {}
        },
        {
            'text': 'TensorFlow',
            'label': 'PRODUCT',
            'context': {'url': 'https://github.com/tensorflow/tensorflow'}
        },
        {
            'text': 'Marie Curie',
            'label': 'PERSON',
            'context': {'url': 'https://orcid.org/0000-0001-2345-6789'}
        },
    ]
    
    fake_user_id = UUID('00000000-0000-0000-0000-000000000000')
    
    for i, case in enumerate(test_cases, 1):
        print(f"\n{i}. Input: {case['text']} ({case['label']})")
        normalized = entity_normalizer.normalize(
            text=case['text'],
            label=case['label'],
            user_id=fake_user_id,
            context=case['context']
        )
        
        if normalized:
            print(f"   Canonical: {normalized['canonical_name']}")
            print(f"   Type: {normalized['type']}")
            if normalized['aliases']:
                print(f"   Aliases: {', '.join(normalized['aliases'][:3])}")
            if normalized['external_ids']:
                print(f"   External IDs: {normalized['external_ids']}")
        else:
            print("   → SKIPPED (unmapped label)")


def test_end_to_end():
    """Test combined extraction + normalization."""
    print("\n" + "=" * 60)
    print("TEST 3: End-to-End - Extract & Normalize")
    print("=" * 60)
    
    text = "Linus Torvalds created Linux while working at Helsinki University. He later joined the Linux Foundation."
    
    print(f"\nText: {text}\n")
    
    # Extract
    entities = nlp_service.extract_entities(text)
    print(f"Extracted {len(entities)} entities:\n")
    
    # Normalize
    fake_user_id = UUID('00000000-0000-0000-0000-000000000000')
    
    for ent in entities:
        normalized = entity_normalizer.normalize(
            text=ent['text'],
            label=ent['label'],
            user_id=fake_user_id,
            context={}
        )
        
        if normalized:
            print(f"✓ {ent['text']:20} → {normalized['canonical_name']:25} [{normalized['type']}]")
        else:
            print(f"  {ent['text']:20} (skipped)")


def main():
    """Run all tests."""
    print("\n" + "🧠" * 30)
    print("NER FUNCTIONALITY TEST SUITE")
    print("🧠" * 30 + "\n")
    
    try:
        # Load spaCy model
        print("Loading spaCy model...")
        nlp_service.load_model('en_core_web_sm')
        print("✓ Model loaded successfully\n")
        
        # Run tests
        test_nlp_extraction()
        test_entity_normalization()
        test_end_to_end()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS COMPLETED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
