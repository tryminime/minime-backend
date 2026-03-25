# Week 8 Entity Intelligence - Documentation

**Date**: 2026-02-03  
**Version**: 1.0  
**Status**: ✅ Complete

---

## 📋 Overview

Week 8 implements Entity Intelligence & Deduplication, the final component of the Weeks 5-8 Core Implementation Plan. This system uses semantic embeddings and multi-factor matching to automatically identify and merge duplicate entities, building a unified knowledge graph.

---

## 🧠 Embedding Model

### Model Choice: all-MiniLM-L6-v2

**Why this model?**
- **Fast**: ~1000 embeddings/sec on CPU
- **Compact**: 384 dimensions (vs 768+ for larger models)
- **Quality**: Good semantic understanding for entity matching
- **Production-ready**: Widely used, stable, well-tested

**Alternatives Considered**:
- `all-mpnet-base-v2`: Better quality but slower (50% more compute)
- `multilingual-e5-small`: For multi-language support
- `bge-small-en-v1.5`: Latest SOTA but requires testing

**Recommendation**: Start with all-MiniLM-L6-v2, upgrade to all-mpnet-base-v2 if precision requires improvement.

---

## 🔍 Entity Deduplication Guide

### How It Works

The deduplication service uses **multi-factor matching**:

1. **Embedding Similarity** (Semantic)
   - Generates 384-dim vector for each entity name
   - Uses cosine similarity to find similar entities
   - Example: "John Doe" ≈ "john doe" (0.95 similarity)

2. **External ID Matching** (Deterministic)
   - Matches on GitHub, ORCID, DOI, arXiv IDs
   - Confidence: 0.99 (very high)
   - Example: github:tensorflow/tensorflow

3. **Alias Matching** (Fuzzy)
   - Checks entity.aliases array
   - Normalized text comparison
   - Example: ["TensorFlow", "TF", "tensorflow"]

### Confidence Thresholds

| Threshold | Action | Use Case |
|-----------|--------|----------|
| ≥ 0.95 | **Auto-merge** | Identical with minor variations |
| 0.80-0.94 | **Suggest to user** | Likely duplicates, human review |
| 0.75-0.79 | **Show in results** | Possible duplicates |
| < 0.75 | **Ignore** | Different entities |

### Tuning Guidelines

**Too many false positives?**
- Increase `AUTO_MERGE_THRESHOLD` from 0.95 to 0.97
- Increase `SUGGEST_THRESHOLD` from 0.80 to 0.85

**Missing duplicates?**
- Decrease thresholds slightly
- Check entity normalization (ensure consistent casing)
- Verify embeddings are being generated

**Best practices**:
- Start conservative (high thresholds)
- Monitor user feedback on suggested merges
- Adjust based on precision/recall metrics

---

## 🔗 Entity Merge Process

### Step-by-Step

When merging entity A → entity B:

1. **Update Occurrences**
   ```sql
   UPDATE entity_occurrences 
   SET entity_id = B.id 
   WHERE entity_id = A.id
   ```

2. **Merge Aliases**
   ```python
   B.aliases = list(set(B.aliases) | set(A.aliases))
   ```

3. **Merge External IDs**
   ```python
   B.external_ids.update(A.external_ids)
   ```

4. **Update Frequency**
   ```python
   B.frequency += A.frequency
   ```

5. **Mark Source as Merged**
   ```python
   A.merged_into_id = B.id
   ```

6. **Delete from Qdrant**
   ```python
   qdrant.delete(A.id)
   ```

### Rollback

To undo a merge:
```sql
-- 1. Restore occurrences
UPDATE entity_occurrences 
SET entity_id = A.id 
WHERE entity_id = B.id 
  AND created_at >= merge_timestamp;

-- 2. Clear merged_into_id
UPDATE entities 
SET merged_into_id = NULL 
WHERE id = A.id;

-- 3. Re-upload to Qdrant
-- (requires application code)
```

---

## 🚀 API Reference

### List Entities

```http
GET /v1/entities?type=PERSON&limit=100&offset=0
Authorization: Bearer <token>
```

**Response**:
```json
{
  "entities": [
    {
      "id": "uuid",
      "canonical_name": "John Doe",
      "type": "PERSON",
      "frequency": 45,
      "aliases": ["John", "Doe"],
      "entity_metadata": {}
    }
  ],
  "total": 234,
  "limit": 100,
  "offset": 0
}
```

### Find Duplicates

```http
GET /v1/entities/{id}/duplicates?threshold=0.80&limit=20
Authorization: Bearer <token>
```

**Response**:
```json
{
  "entity_id": "uuid",
  "entity_name": "John Doe",
  "duplicates": [
    {
      "entity_id": "uuid2",
      "canonical_name": "john doe",
      "confidence": 0.95,
      "methods": ["embedding", "alias"],
      "recommendation": "auto_merge"
    }
  ],
  "count": 1,
  "thresholds": {
    "auto_merge": 0.95,
    "suggest": 0.80
  }
}
```

### Merge Entities

```http
POST /v1/entities/merge
Authorization: Bearer <token>
Content-Type: application/json

{
  "source_id": "uuid-to-merge",
  "target_id": "uuid-to-keep"
}
```

**Response**:
```json
{
  "status": "success",
  "message": "Entities merged successfully",
  "merged_entity": {
    "id": "target-uuid",
    "canonical_name": "John Doe",
    "frequency": 90,
    "aliases": ["John", "Doe", "Johnny"]
  }
}
```

### Get Neighbors

```http
GET /v1/entities/{id}/neighbors?depth=1&relationship_type=CO_OCCURS_WITH
Authorization: Bearer <token>
```

**Response**:
```json
{
  "entity_id": "uuid",
  "entity_name": "John Doe",
  "neighbors": [
    {
      "entity_id": "uuid-org",
      "canonical_name": "Google Inc",
      "type": "ORG",
      "co_occurrence_count": 15,
      "relationship_type": "CO_OCCURS_WITH"
    }
  ],
  "count": 1
}
```

---

## 🔧 Troubleshooting

### Problem: Embeddings not generating

**Check**:
1. Is sentence-transformers installed?
   ```bash
   pip show sentence-transformers
   ```

2. Is Celery worker running?
   ```bash
   celery -A backend.config.celery_config:celery_app worker --loglevel=info
   ```

3. Check entity.embedding field:
   ```sql
   SELECT id, canonical_name, 
          CASE WHEN embedding IS NULL THEN 'NULL' 
               WHEN embedding = '[]' THEN 'EMPTY' 
               ELSE 'OK' END as emb_status
   FROM entities LIMIT 10;
   ```

**Fix**:
```python
# Manual embedding generation
from backend.tasks.entity_tasks import batch_generate_embeddings
batch_generate_embeddings.delay(limit=100)
```

### Problem: Duplicates not being found

**Check**:
1. Are embeddings generated for both entities?
2. Is Qdrant running and accessible?
3. Check similarity manually:
   ```python
   from backend.services.embedding_service import embedding_service
   emb1 = embedding_service.generate_embedding("John Doe")
   emb2 = embedding_service.generate_embedding("john doe")
   sim = embedding_service.compute_similarity(emb1, emb2)
   print(f"Similarity: {sim:.4f}")
   ```

**Fix**: Lower threshold temporarily to see if any candidates appear:
```python
duplicates = deduplication_service.find_duplicates(entity, threshold=0.70)
```

### Problem: Too many false positive duplicates

**Check**: Distribution of confidence scores:
```python
duplicates = deduplication_service.find_duplicates(entity, limit=50)
scores = [d['confidence'] for d in duplicates]
print(f"Min: {min(scores):.2f}, Max: {max(scores):.2f}, Avg: {sum(scores)/len(scores):.2f}")
```

**Fix**: Increase thresholds in `backend/services/entity_deduplication.py`:
```python
AUTO_MERGE_THRESHOLD = 0.97  # Was 0.95
SUGGEST_THRESHOLD = 0.85  # Was 0.80
```

### Problem: Merge operation fails

**Check logs**:
```bash
tail -f /var/log/minime/backend.log | grep "merge_entities"
```

**Common issues**:
1. Entities belong to different users
2. Entity already merged (`merged_into_id` not NULL)
3. Database constraint violation

**Fix**:
```python
# Check entity status
entity = db.query(Entity).filter(Entity.id == entity_id).first()
print(f"User: {entity.user_id}, Merged: {entity.merged_into_id}")
```

---

## 📈 Performance Optimization

### Embedding Generation

**Batch processing** is 10x faster than individual:
```python
# Slow (individual)
for entity in entities:
    emb = embedding_service.generate_embedding(entity.canonical_name)

# Fast (batch)
texts = [e.canonical_name for e in entities]
embeddings = embedding_service.generate_batch_embeddings(texts)
```

**Benchmarks**:
- Individual: ~10 embeddings/sec
- Batch (32): ~100 embeddings/sec
- GPU: ~1000 embeddings/sec

### Qdrant Search

**Indexing**: Qdrant automatically indexes vectors (HNSW)

**Optimization**:
- Keep `limit` reasonable (<100)
- Use `score_threshold` to reduce results
- Batch uploads when possible

---

## 🎯 Best Practices

### 1. Embedding Strategy

✅ **Do**:
- Generate embeddings asynchronously (Celery)
- Cache embeddings in database
- Use batch generation for bulk operations

❌ **Don't**:
- Generate embeddings synchronously in API requests
- Regenerate embeddings unnecessarily
- Use embeddings without normalization

### 2. Deduplication Workflow

✅ **Do**:
- Start with high thresholds (conservative)
- Show confidence scores to users
- Log all merge operations for audit
- Provide undo functionality

❌ **Don't**:
- Auto-merge below 0.95 confidence
- Merge across different entity types
- Delete source entities immediately

### 3. Production Deployment

✅ **Do**:
- Monitor embedding generation queue depth
- Set up alerts for failed merges
- Regular backup of entity data
- Performance testing with 10k+ entities

❌ **Don't**:
- Run without Celery worker
- Skip database migrations
- Ignore Qdrant connection failures

---

## 📚 Additional Resources

- [Sentence Transformers Docs](https://www.sbert.net/)
- [Qdrant Documentation](https://qdrant.tech/documentation/)
- [Cosine Similarity Explained](https://en.wikipedia.org/wiki/Cosine_similarity)

---

**Last Updated**: 2026-02-03  
**Maintained By**: MiniMe Core Team
