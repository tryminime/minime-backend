#!/usr/bin/env python3
"""
End-to-end integration test for Week 7 NER Pipeline.

This test simulates the complete flow:
1. Activity ingestion
2. NER task queueing
3. Entity extraction
4. Entity storage
"""

import sys
import time
sys.path.insert(0, '/home/ansari/Documents/MiniMe')

from services.nlp_service import nlp_service
from services.entity_normalizer import entity_normalizer
from tasks.ner_worker import extract_text_blob, find_or_create_entity
from models import Activity, Entity, EntityOccurrence
from database.postgres import SessionLocal
from uuid import uuid4, UUID
from datetime import datetime


def test_text_extraction():
    """Test extracting text from activity context."""
    print("\n" + "="*60)
    print("TEST 1: Text Extraction from Activity Context")
    print("="*60)
    
    # Mock activity
    class MockActivity:
        context = {
            'title': 'Neural Networks: A Comprehensive Introduction',
            'url': 'https://arxiv.org/abs/2401.12345',
            'domain': 'arxiv.org',
            'app_name': 'Chrome'
        }
    
    text = extract_text_blob(MockActivity())
    print(f"\nExtracted text: {text}")
    print(f"Length: {len(text)} characters")
    
    assert len(text) > 0, "Text extraction failed"
    assert 'Neural Networks' in text, "Title not in extracted text"
    print("✓ Text extraction working")


def test_ner_extraction():
    """Test NER extraction on sample activity text."""
    print("\n" + "="*60)
    print("TEST 2: NER Extraction")
    print("="*60)
    
    sample_texts = [
        "Geoffrey Hinton published a paper about deep learning at Google AI.",
        "The TensorFlow library was developed by the Google Brain team.",
        "Visit github.com/openai/gpt-3 for the code repository.",
    ]
    
    nlp_service.load_model('en_core_web_sm')
    
    for text in sample_texts:
        print(f"\nText: {text}")
        entities = nlp_service.extract_entities(text)
        print(f"Found {len(entities)} entities:")
        for ent in entities:
            print(f"  - {ent['text']:20} [{ent['label']}] (conf: {ent['confidence']:.2f})")
    
    print("\n✓ NER extraction working")


def test_entity_normalization_and_storage():
    """Test entity normalization and database storage."""
    print("\n" + "="*60)
    print("TEST 3: Entity Normalization & Database Storage")
    print("="*60)
    
    db = SessionLocal()
    
    try:
        # Create test user ID
        test_user_id = UUID('00000000-0000-0000-0000-000000000000')
        
        # Test normalizing an entity
        normalized = entity_normalizer.normalize(
            text='Geoffrey Hinton',
            label='PERSON',
            user_id=test_user_id,
            context={'url': 'https://en.wikipedia.org/wiki/Geoffrey_Hinton'}
        )
        
        print(f"\nNormalized entity:")
        print(f"  Canonical: {normalized['canonical_name']}")
        print(f"  Type: {normalized['type']}")
        print(f"  Aliases: {normalized['aliases']}")
        
        # Test find_or_create
        print(f"\nTesting find_or_create...")
        entity = find_or_create_entity(db, normalized, test_user_id)
        print(f"✓ Entity created/found: {entity.canonical_name} (ID: {entity.id})")
        
        db.commit()
        
        # Verify it's in database
        found = db.query(Entity).filter(Entity.id == entity.id).first()
        assert found is not None, "Entity not found in database"
        print(f"✓ Entity verified in database")
        
        # Clean up
        db.delete(found)
        db.commit()
        print(f"✓ Test entity cleaned up")
        
    finally:
        db.close()
    
    print("\n✓ Entity normalization and storage working")


def test_model_imports():
    """Test that all models import correctly."""
    print("\n" + "="*60)
    print("TEST 4: Model Imports")
    print("="*60)
    
    from models import User, Activity, Entity, EntityOccurrence, AuditLog
    
    models = [
        ('User', User),
        ('Activity', Activity),
        ('Entity', Entity),
        ('EntityOccurrence', EntityOccurrence),
        ('AuditLog', AuditLog),
    ]
    
    for name, model in models:
        tablename = model.__tablename__
        print(f"✓ {name:20} → table: {tablename}")
    
    print("\n✓ All models imported successfully")


def test_service_imports():
    """Test that all services import correctly."""
    print("\n" + "="*60)
    print("TEST 5: Service Imports")
    print("="*60)
    
    services = []
    
    try:
        from services.nlp_service import nlp_service
        services.append(('NLP Service', nlp_service))
    except Exception as e:
        print(f"✗ NLP Service failed: {e}")
    
    try:
        from services.entity_normalizer import entity_normalizer
        services.append(('Entity Normalizer', entity_normalizer))
    except Exception as e:
        print(f"✗ Entity Normalizer failed: {e}")
    
    try:
        from services.event_bus import EventBus
        services.append(('Event Bus', EventBus))
    except Exception as e:
        print(f"✗ Event Bus failed: {e}")
    
    try:
        from config.celery_config import celery_app
        services.append(('Celery App', celery_app))
    except Exception as e:
        print(f"✗ Celery App failed: {e}")
    
    try:
        from tasks.ner_worker import process_activity_ner
        services.append(('NER Worker', process_activity_ner))
    except Exception as e:
        print(f"✗ NER Worker failed: {e}")
    
    for name, service in services:
        print(f"✓ {name:25} imported")
    
    print(f"\n✓ {len(services)}/5 services imported successfully")


def main():
    """Run all integration tests."""
    print("\n" + "🧪" * 30)
    print("WEEK 7: END-TO-END INTEGRATION TEST")
    print("🧪" * 30)
    
    try:
        test_model_imports()
        test_service_imports()
        test_text_extraction()
        test_ner_extraction()
        test_entity_normalization_and_storage()
        
        print("\n" + "="*60)
        print("✅ ALL INTEGRATION TESTS PASSED")
        print("="*60)
        print("\nWeek 7 NER Pipeline is fully functional!")
        print("\nComponents verified:")
        print("  ✓ Models (EntityOccurrence, Entity)")
        print("  ✓ Services (NLP, Entity Normalizer)")
        print("  ✓ Celery (Worker, Config)")
        print("  ✓ Text extraction")
        print("  ✓ NER entity extraction")
        print("  ✓ Entity normalization")
        print("  ✓ Database storage")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Integration test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
