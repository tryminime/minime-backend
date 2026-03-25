"""
Embedding Service for generating semantic embeddings for entities.

Uses sentence-transformers all-MiniLM-L6-v2:
- 384-dimensional embeddings
- Fast inference (~1000 sentences/sec on CPU)
- Good quality for entity similarity matching
"""

from sentence_transformers import SentenceTransformer
from typing import List, Optional
import structlog
import numpy as np

logger = structlog.get_logger()


class EmbeddingService:
    """
    Service for generating semantic embeddings for entities.
    
    This service uses sentence-transformers to generate dense vector
    representations of entity names, enabling similarity-based matching
    and duplicate detection.
    """
    
    _instance: Optional['EmbeddingService'] = None
    _model = None
    _model_name = 'sentence-transformers/all-MiniLM-L6-v2'
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize with lazy model loading."""
        pass
    
    def load_model(self):
        """Load sentence-transformers model."""
        if self._model is None:
            logger.info("Loading embedding model", model=self._model_name)
            try:
                self._model = SentenceTransformer(self._model_name)
                logger.info("Embedding model loaded successfully")
            except Exception as e:
                logger.error("Failed to load embedding model", error=str(e))
                raise
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text.
        
        Args:
            text: Input text (entity canonical name or description)
        
        Returns:
            384-dimensional embedding vector
        """
        if self._model is None:
            self.load_model()
        
        if not text or len(text.strip()) == 0:
            # Return zero vector for empty text
            return [0.0] * 384
        
        try:
            # Generate embedding
            embedding = self._model.encode(text, convert_to_numpy=True)
            
            # Convert to list and ensure proper precision
            return embedding.astype(np.float32).tolist()
            
        except Exception as e:
            logger.error("Failed to generate embedding", text=text[:50], error=str(e))
            return [0.0] * 384  # Return zero vector on error
    
    def generate_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts (batched for efficiency).
        
        Args:
            texts: List of input texts
        
        Returns:
            List of 384-dimensional embedding vectors
        """
        if self._model is None:
            self.load_model()
        
        if not texts:
            return []
        
        # Replace empty strings with placeholder
        texts_cleaned = [t if t and len(t.strip()) > 0 else "unknown" for t in texts]
        
        try:
            # Generate embeddings in batch
            embeddings = self._model.encode(
                texts_cleaned,
                convert_to_numpy=True,
                batch_size=32,
                show_progress_bar=False
            )
            
            # Convert to list of lists
            return [emb.astype(np.float32).tolist() for emb in embeddings]
            
        except Exception as e:
            logger.error("Failed to generate batch embeddings", count=len(texts), error=str(e))
            # Return zero vectors on error
            return [[0.0] * 384 for _ in texts]
    
    def compute_similarity(self, embedding1: List[float], embedding2: List[float]) -> float:
        """
        Compute cosine similarity between two embeddings.
        
        Args:
            embedding1, embedding2: 384-dimensional vectors
        
        Returns:
            Similarity score (0-1, higher = more similar)
        """
        try:
            # Convert to numpy arrays
            vec1 = np.array(embedding1, dtype=np.float32)
            vec2 = np.array(embedding2, dtype=np.float32)
            
            # Compute cosine similarity
            dot_product = np.dot(vec1, vec2)
            norm1 = np.linalg.norm(vec1)
            norm2 = np.linalg.norm(vec2)
            
            if norm1 == 0 or norm2 == 0:
                return 0.0
            
            similarity = dot_product / (norm1 * norm2)
            return float(similarity)
            
        except Exception as e:
            logger.error("Failed to compute similarity", error=str(e))
            return 0.0
    
    def get_embedding_dimension(self) -> int:
        """Get the dimensionality of embeddings."""
        return 384


# Global singleton instance
embedding_service = EmbeddingService()
