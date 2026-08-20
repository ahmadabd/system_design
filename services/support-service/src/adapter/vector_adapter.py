import logging
from typing import List, Dict, Any, Tuple
from langchain_core.documents import Document
from shared.common.resilience import AsyncCircuitBreaker
from src.infrastructure.qdrant_setup import QdrantManager
from src.domain.models import DocumentChunk

logger = logging.getLogger("VectorAdapter")

class QdrantVectorAdapter:
    """Adapter for interacting with Qdrant vector store with circuit breaker resilience"""
    def __init__(self, manager: QdrantManager):
        self.manager = manager
        self.breaker = AsyncCircuitBreaker(
            name="qdrant-breaker",
            failure_threshold=4,
            recovery_timeout=10.0
        )

    async def add_documents(self, documents: List[Document]) -> List[str]:
        """Indexes documents into Qdrant collection under circuit breaker protection"""
        async def _add():
            vector_store = self.manager.get_vector_store()
            # LangChain QdrantVectorStore add_documents
            return await vector_store.aadd_documents(documents)
            
        return await self.breaker.call(_add)

    async def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        filter_dict: Dict[str, Any] | None = None
    ) -> List[DocumentChunk]:
        """Performs vector similarity search against Qdrant collection"""
        async def _search():
            vector_store = self.manager.get_vector_store()
            results: List[Tuple[Document, float]] = await vector_store.asimilarity_search_with_relevance_scores(
                query=query,
                k=k,
                filter=filter_dict
            )
            chunks = []
            for doc, score in results:
                chunks.append(DocumentChunk(
                    chunk_id=doc.metadata.get("id", doc.metadata.get("source", "unknown")),
                    content=doc.page_content,
                    metadata=doc.metadata,
                    score=float(score)
                ))
            return chunks

        return await self.breaker.call(_search)

    async def get_collection_info(self) -> Dict[str, Any]:
        """Retrieves collection statistics (points count, indexed status)"""
        async def _info():
            from src.infrastructure.config import settings
            info = self.manager.client.get_collection(collection_name=settings.QDRANT_COLLECTION_NAME)
            return {
                "collection_name": settings.QDRANT_COLLECTION_NAME,
                "status": str(info.status),
                "points_count": info.points_count,
                "vectors_count": getattr(info, "vectors_count", info.points_count)
            }
        return await self.breaker.call(_info)
