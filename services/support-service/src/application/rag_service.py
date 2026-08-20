import time
import logging
from typing import List, Dict, Any, AsyncIterator
from langchain_core.messages import SystemMessage, HumanMessage
from src.adapter.vector_adapter import QdrantVectorAdapter
from src.adapter.llm_adapter import OpenRouterLLMAdapter
from src.domain.models import SupportQuery, SupportResponse
from src.infrastructure.config import settings

logger = logging.getLogger("RAGService")

SYSTEM_RAG_PROMPT = """You are an expert, friendly, and empathetic AI Customer Support Specialist for our modern multi-tenant e-commerce platform.

Your mission is to help customers resolve their questions regarding orders, deliveries, returns, cancellations, and payments quickly and accurately.

### GUIDELINES:
1. **Factual Grounding**: Base your answers strictly and accurately on the provided Policy Context below. Do NOT invent policies or make promises outside the provided documents.
2. **Clear & Structured**: Use bullet points, bold text, and numbered steps for readability.
3. **Specific Timelines**: Always mention exact numbers (e.g., "30 calendar days", "2:00 PM EST", "48 hours", "$4.99 deduction") when relevant.
4. **Empathetic & Professional Tone**: Maintain a warm, reassuring, and customer-first attitude.
5. **Handling Unknowns**: If the provided context does not contain the exact answer, politely explain what policy is available and advise the user on how our team can help.

---
### OFFICIAL POLICY CONTEXT:
{context}
"""

class RAGApplicationService:
    """Application use case orchestrating semantic retrieval from Qdrant and generation via OpenRouter LLM"""
    def __init__(self, vector_adapter: QdrantVectorAdapter, llm_adapter: OpenRouterLLMAdapter):
        self.vector_adapter = vector_adapter
        self.llm_adapter = llm_adapter

    async def answer_query(self, query: SupportQuery, top_k: int = 4) -> SupportResponse:
        """Executes full RAG workflow: Retrieve context chunks -> Build prompt -> Generate LLM answer"""
        start_time = time.perf_counter()
        logger.info(f"Processing support query: session_id={query.session_id}, query='{query.message}'")

        # 1. Semantic Retrieval from Qdrant
        retrieved_chunks = await self.vector_adapter.similarity_search_with_score(
            query=query.message,
            k=top_k
        )

        logger.info(f"Retrieved {len(retrieved_chunks)} relevant chunks from Qdrant.")

        # 2. Format Context & Sources
        context_blocks = []
        sources: List[Dict[str, Any]] = []
        for chunk in retrieved_chunks:
            source_info = {
                "source": chunk.metadata.get("source", "unknown"),
                "title": chunk.metadata.get("title", ""),
                "score": chunk.score
            }
            sources.append(source_info)
            context_blocks.append(f"--- Document: {source_info['title']} ---\n{chunk.content}\n")

        formatted_context = "\n".join(context_blocks) if context_blocks else "No specific policy documents matched this query."

        # 3. Construct System and User Messages
        system_content = SYSTEM_RAG_PROMPT.format(context=formatted_context)
        messages = [
            SystemMessage(content=system_content),
            HumanMessage(content=query.message)
        ]

        # 4. Invoke LLM via OpenRouter Adapter (protected by Circuit Breaker)
        answer_text = await self.llm_adapter.invoke(messages)

        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        logger.info(f"Generated RAG response in {elapsed_ms:.2f}ms using model {settings.OPENROUTER_MODEL}")

        return SupportResponse(
            answer=str(answer_text),
            sources=sources,
            model_name=settings.OPENROUTER_MODEL,
            processing_time_ms=round(elapsed_ms, 2)
        )

    async def stream_query(self, query: SupportQuery, top_k: int = 4) -> AsyncIterator[str]:
        """Streams response tokens for real-time SSE streaming"""
        retrieved_chunks = await self.vector_adapter.similarity_search_with_score(
            query=query.message,
            k=top_k
        )

        context_blocks = [
            f"--- Document: {c.metadata.get('title', '')} ---\n{c.content}\n"
            for c in retrieved_chunks
        ]
        formatted_context = "\n".join(context_blocks) if context_blocks else "No specific policy documents matched."

        messages = [
            SystemMessage(content=SYSTEM_RAG_PROMPT.format(context=formatted_context)),
            HumanMessage(content=query.message)
        ]

        async for token in self.llm_adapter.stream(messages):
            yield token
