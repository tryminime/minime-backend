#!/usr/bin/env python3
"""
Test script for embedding service.

Tests:
1. Embedding generation for single text
2. Batch embedding generation
3. Similarity computation
4. Performance benchmark
"""

import sys
import time
sys.path.insert(0, '/home/ansari/Documents/MiniMe')

from services.embedding_service import embedding_service


def test_single_embedding():
    """Test single embedding generation."""
    print("=" * 60)
    print("TEST 1: Single Embedding Generation")
    print("=" * 60)
    
    test_texts = [
        "John Doe",
        "Google Inc",
        "TensorFlow",
        "Machine Learning",
        ""  # Empty string test
    ]
    
    embedding_service.load_model()
    
    for text in test_texts:
        start = time.time()
        embedding = embedding_service.generate_embedding(text)
        elapsed = (time.time() - start) * 1000
        
        print(f"\nText: '{text}'")
        print(f"Embedding dim: {len(embedding)}")
        print(f"First 5 values: {embedding[:5]}")
        print(f"Time: {elapsed:.2f}ms")
        
        # Verify dimensionality
        assert len(embedding) == 384, f"Expected 384 dimensions, got {len(embedding)}"
    
    print("\n✓ Single embedding generation working")


def test_batch_embeddings():
    """Test batch embedding generation."""
    print("\n" + "=" * 60)
    print("TEST 2: Batch Embedding Generation")
    print("=" * 60)
    
    texts = [
        "Albert Einstein",
        "Microsoft Corporation",
        "Python Programming Language",
        "Stanford University",
        "Neural Networks",
    ]
    
    start = time.time()
    embeddings = embedding_service.generate_batch_embeddings(texts)
    elapsed = (time.time() - start) * 1000
    
    print(f"\nGenerated {len(embeddings)} embeddings")
    print(f"Total time: {elapsed:.2f}ms")
    print(f"Avg time per embedding: {elapsed/len(texts):.2f}ms")
    
    for i, (text, embedding) in enumerate(zip(texts, embeddings)):
        print(f"{i+1}. {text:40} → {len(embedding)} dims")
    
    print("\n✓ Batch embedding generation working")


def test_similarity():
    """Test similarity computation."""
    print("\n" + "=" * 60)
    print("TEST 3: Similarity Computation")
    print("=" * 60)
    
    test_pairs = [
        ("John Doe", "john doe"),  # Same person, different case
        ("Google", "Google Inc"),  # Same entity, variation
        ("Python", "Java"),  # Different but related
        ("Apple", "Orange"),  # Different and unrelated
        ("TensorFlow", "PyTorch"),  # Similar tools
    ]
    
    for text1, text2 in test_pairs:
        emb1 = embedding_service.generate_embedding(text1)
        emb2 = embedding_service.generate_embedding(text2)
        
        similarity = embedding_service.compute_similarity(emb1, emb2)
        
        print(f"\n'{text1}' vs '{text2}'")
        print(f"Similarity: {similarity:.4f} ({similarity * 100:.1f}%)")
        
        # Interpretation
        if similarity > 0.9:
            status = "Very similar (likely duplicates)"
        elif similarity > 0.8:
            status = "Similar (potential duplicates)"
        elif similarity > 0.6:
            status = "Somewhat similar"
        else:
            status = "Different"
        
        print(f"Status: {status}")
    
    print("\n✓ Similarity computation working")


def test_performance():
    """Benchmark performance."""
    print("\n" + "=" * 60)
    print("TEST 4: Performance Benchmark")
    print("=" * 60)
    
    # Generate 100 random entity names
    test_texts = [
        f"Entity Name {i}" for i in range(100)
    ]
    
    # Warm-up
    _ = embedding_service.generate_embedding("warm up")
    
    # Benchmark single
    print("\nSingle embedding generation:")
    start = time.time()
    for text in test_texts[:10]:
        _ = embedding_service.generate_embedding(text)
    elapsed = (time.time() - start)
    rate = 10 / elapsed
    print(f"  Rate: {rate:.1f} embeddings/sec")
    
    # Benchmark batch
    print("\nBatch embedding generation:")
    start = time.time()
    _ = embedding_service.generate_batch_embeddings(test_texts)
    elapsed = (time.time() - start)
    rate = len(test_texts) / elapsed
    print(f"  Rate: {rate:.1f} embeddings/sec")
    print(f"  Total time for 100 entities: {elapsed:.2f}s")
    
    print("\n✓ Performance benchmark complete")


def main():
    """Run all tests."""
    print("\n" + "🧮" * 30)
    print("EMBEDDING SERVICE TEST SUITE")
    print("🧮" * 30 + "\n")
    
    try:
        test_single_embedding()
        test_batch_embeddings()
        test_similarity()
        test_performance()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
        print("\nEmbedding service is fully functional!")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
