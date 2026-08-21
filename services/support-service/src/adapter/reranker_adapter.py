import logging
from typing import List, Optional
from flashrank import Ranker, RerankRequest
from shared.common.resilience import AsyncCircuitBreaker
from src.domain.models import DocumentChunk
from src.infrastructure.config import settings

logger = logging.getLogger("RerankerAdapter")

class CrossEncoderRerankerAdapter:
    """Adapter for Cross-Encoder re-ranking using FlashRank (ONNX-accelerated CPU model)"""
    def __init__(self):
        self._ranker: Optional[Ranker] = None
        self.breaker = AsyncCircuitBreaker(
            name="reranker-breaker",
            failure_threshold=3,
            recovery_timeout=10.0
        )

    @property
    def ranker(self) -> Ranker:
        if self._ranker is None:
            logger.info(f"Initializing FlashRank Cross-Encoder model: {settings.RERANKER_MODEL_NAME}")
            self._ranker = Ranker(model_name=settings.RERANKER_MODEL_NAME, cache_dir="/tmp/flashrank_cache")
        return self._ranker

    async def rerank(self, query: str, chunks: List[DocumentChunk], top_k: int = 3) -> List[DocumentChunk]:
        """
        Re-scores candidate chunks using cross-attention between the query and each chunk content.
        Returns Top-K chunks sorted by descending cross-encoder relevance score (0.0 to 1.0).
        """
        if not chunks:
            return []

        # If only 1 chunk, no re-ranking needed
        if len(chunks) == 1:
            return chunks

        async def _run_rerank():
            # Format passages for FlashRank
            passages = [
                {
                    "id": c.chunk_id,
                    "text": c.content,
                    "meta": c.metadata
                }
                for c in chunks
            ]

            rerank_request = RerankRequest(query=query, passages=passages)
            # Execute cross-encoder scoring
            results = self.ranker.rerank(rerank_request)

            reranked_chunks: List[DocumentChunk] = []
            for item in results[:top_k]:
                reranked_chunks.append(DocumentChunk(
                    chunk_id=item.get("id", "unknown"),
                    content=item.get("text", ""),
                    metadata=item.get("meta", {}),
                    score=float(item.get("score", 0.0))
                ))
            return reranked_chunks

        try:
            return await self.breaker.call(_run_rerank)
        except Exception as e:
            logger.warning(f"Cross-encoder reranking failed ({e}). Falling back to initial candidate order.")
            return chunks[:top_k]

# Singleton Reranker Adapter instance
reranker_adapter = CrossEncoderRerankerAdapter()
