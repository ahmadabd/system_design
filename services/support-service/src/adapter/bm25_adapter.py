import re
import logging
from typing import List, Dict, Any, Optional
from rank_bm25 import BM25Okapi
from src.domain.models import DocumentChunk

logger = logging.getLogger("BM25Adapter")

def tokenize_text(text: str) -> List[str]:
    """Basic lowercased alphanumeric tokenizer for BM25 keyword matching"""
    return re.findall(r'\w+', text.lower())

class BM25SearchAdapter:
    """In-memory BM25 sparse keyword search index for fast exact-token retrieval"""
    def __init__(self):
        self._bm25: Optional[BM25Okapi] = None
        self._documents: List[DocumentChunk] = []

    @property
    def is_indexed(self) -> bool:
        return self._bm25 is not None and len(self._documents) > 0

    def index_documents(self, documents: List[DocumentChunk]) -> None:
        """Builds a BM25Okapi index over a collection of document chunks"""
        logger.info(f"Building in-memory BM25 index over {len(documents)} document chunks...")
        self._documents = documents
        tokenized_corpus = [tokenize_text(doc.content) for doc in documents]
        self._bm25 = BM25Okapi(tokenized_corpus)
        logger.info("BM25 index built successfully.")

    def search(self, query: str, k: int = 10) -> List[DocumentChunk]:
        """Performs sparse keyword search returning top-K documents ranked by BM25 score"""
        if not self.is_indexed:
            logger.warning("BM25 index is empty. Returning 0 results.")
            return []

        tokenized_query = tokenize_text(query)
        if not tokenized_query:
            return []

        # Get scores for all documents in corpus
        scores = self._bm25.get_scores(tokenized_query)
        
        # Rank document indices by descending BM25 score
        ranked_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:k]
        
        results: List[DocumentChunk] = []
        for idx in ranked_indices:
            score = float(scores[idx])
            # Only include documents that have at least some keyword overlap (score > 0)
            if score > 0:
                doc = self._documents[idx]
                results.append(DocumentChunk(
                    chunk_id=doc.chunk_id,
                    content=doc.content,
                    metadata=doc.metadata,
                    score=score
                ))

        logger.info(f"BM25 search for '{query}' returned {len(results)} matches.")
        return results

# Singleton BM25 Adapter instance
bm25_adapter = BM25SearchAdapter()
