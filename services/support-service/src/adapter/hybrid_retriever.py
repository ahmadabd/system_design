import asyncio
import logging
from typing import List, Dict, Any
from src.domain.models import DocumentChunk
from src.adapter.vector_adapter import QdrantVectorAdapter
from src.adapter.bm25_adapter import bm25_adapter, BM25SearchAdapter
from src.adapter.reranker_adapter import reranker_adapter, CrossEncoderRerankerAdapter
from src.infrastructure.qdrant_setup import qdrant_manager
from src.infrastructure.config import settings

logger = logging.getLogger("HybridRetriever")

def compute_reciprocal_rank_fusion(
    dense_results: List[DocumentChunk],
    sparse_results: List[DocumentChunk],
    k_constant: int = 60
) -> List[DocumentChunk]:
    """
    Combines dense and sparse search rankings using Reciprocal Rank Fusion (RRF).
    Formula: RRF_score(d) = sum(1 / (k + rank(d)))
    """
    rrf_scores: Dict[str, float] = {}
    chunk_map: Dict[str, DocumentChunk] = {}

    # Process Dense Vector Rankings
    for rank, chunk in enumerate(dense_results, start=1):
        chunk_id = chunk.chunk_id or chunk.content[:50]
        chunk_map[chunk_id] = chunk
        rrf_scores[chunk_id] = rrf_scores.get(chunk_id, 0.0) + (1.0 / (k_constant + rank))

    # Process Sparse BM25 Rankings
    for rank, chunk in enumerate(sparse_results, start=1):
        chunk_id = chunk.chunk_id or chunk.content[:50]
        chunk_map[chunk_id] = chunk
        rrf_scores[chunk_id] = rrf_scores.get(chunk_id, 0.0) + (1.0 / (k_constant + rank))

    # Sort chunks by descending RRF score
    sorted_chunk_ids = sorted(rrf_scores.keys(), key=lambda cid: rrf_scores[cid], reverse=True)

    fused_chunks: List[DocumentChunk] = []
    for cid in sorted_chunk_ids:
        original_chunk = chunk_map[cid]
        # Attach composite RRF score to chunk
        fused_chunks.append(DocumentChunk(
            chunk_id=original_chunk.chunk_id,
            content=original_chunk.content,
            metadata=original_chunk.metadata,
            score=round(rrf_scores[cid], 6)
        ))

    return fused_chunks

class TwoStageHybridRetriever:
    """
    Production-Grade Two-Stage Retrieval Pipeline:
    Stage 1: Dense Vector Search (Qdrant) + Sparse Keyword Search (BM25) -> Reciprocal Rank Fusion (RRF)
    Stage 2: FlashRank Cross-Encoder Re-Ranking -> Final Top-K Chunks
    """
    def __init__(
        self,
        vector_adapter: QdrantVectorAdapter,
        bm25: BM25SearchAdapter,
        reranker: CrossEncoderRerankerAdapter
    ):
        self.vector_adapter = vector_adapter
        self.bm25 = bm25
        self.reranker = reranker

    async def retrieve_and_rerank(
        self,
        query: str,
        top_k: int = 3,
        candidate_k: int = 15
    ) -> List[DocumentChunk]:
        """Executes Stage 1 Hybrid Retrieval and Stage 2 Cross-Encoder Re-ranking"""
        logger.info(f"Executing Two-Stage Hybrid Retrieval for query: '{query}'")

        # --- STAGE 1: Parallel Hybrid Search (Dense + Sparse) ---
        dense_task = self.vector_adapter.similarity_search_with_score(query, k=settings.HYBRID_DENSE_K)
        
        # Run BM25 search in thread pool to avoid blocking async event loop
        sparse_results = self.bm25.search(query, k=settings.HYBRID_SPARSE_K)
        dense_results = await dense_task

        logger.info(f"Stage 1 Results: {len(dense_results)} dense chunks, {len(sparse_results)} BM25 sparse chunks.")

        # Merge rankings using Reciprocal Rank Fusion (RRF)
        fused_candidates = compute_reciprocal_rank_fusion(
            dense_results=dense_results,
            sparse_results=sparse_results,
            k_constant=settings.RRF_K_CONSTANT
        )[:candidate_k]

        logger.info(f"Stage 1 Fusion: Merged into {len(fused_candidates)} unique candidate chunks.")

        if not fused_candidates:
            return []

        # --- STAGE 2: Cross-Encoder Re-ranking ---
        reranked_chunks = await self.reranker.rerank(
            query=query,
            chunks=fused_candidates,
            top_k=top_k
        )

        logger.info(f"Stage 2 Re-ranking: Selected Top-{len(reranked_chunks)} precision chunks.")
        return reranked_chunks

# Singleton hybrid retriever
_vector_adapter = QdrantVectorAdapter(qdrant_manager)
hybrid_retriever = TwoStageHybridRetriever(_vector_adapter, bm25_adapter, reranker_adapter)
