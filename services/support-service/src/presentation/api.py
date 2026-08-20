import time
import logging
import asyncio
from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException, status, Request
from fastapi.responses import StreamingResponse
from shared.common.idempotency import IdempotencyManager, idempotent_api
from shared.common.resilience import CircuitBreakerOpenException
from src.infrastructure.config import settings
from src.infrastructure.qdrant_setup import qdrant_manager
from src.infrastructure.llm_setup import llm_manager
from src.adapter.vector_adapter import QdrantVectorAdapter
from src.adapter.llm_adapter import OpenRouterLLMAdapter
from src.application.ingestion_service import IngestionApplicationService
from src.application.rag_service import RAGApplicationService
from src.application.graph_builder import support_workflow
from src.presentation.schemas import ChatRequest, ChatResponse, IngestResponse, HealthResponse

logger = logging.getLogger("SupportPresentation")

router = APIRouter(prefix="", tags=["Customer Support"])

# Establish Redis Idempotency Manager
idempotency_manager = IdempotencyManager(settings.REDIS_URL)

# Adapters
vector_adapter = QdrantVectorAdapter(qdrant_manager)
llm_adapter = OpenRouterLLMAdapter(llm_manager)

# Application Services
ingestion_service = IngestionApplicationService(vector_adapter)
rag_service = RAGApplicationService(vector_adapter, llm_adapter)

@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """System health check endpoint verifying Qdrant and OpenRouter connectivity"""
    try:
        qdrant_info = await vector_adapter.get_collection_info()
    except Exception as e:
        qdrant_info = {"status": "error", "detail": str(e)}

    return HealthResponse(
        status="healthy",
        service=settings.SERVICE_NAME,
        model=settings.OPENROUTER_MODEL,
        qdrant=qdrant_info
    )

@router.post("/ingest", response_model=IngestResponse)
async def ingest_knowledge_base():
    """Triggers knowledge base parsing, chunking, and Qdrant vector indexing"""
    try:
        result = await ingestion_service.ingest_directory()
        return IngestResponse(
            status=result.get("status", "success"),
            files_processed=result.get("files_processed", 0),
            chunks_indexed=result.get("chunks_indexed", 0),
            collection_name=result.get("collection_name"),
            message=result.get("message")
        )
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Qdrant Circuit Breaker Active: {str(cb_err)}. Vector operations degraded."
        )
    except Exception as e:
        logger.error(f"Failed to ingest knowledge base: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ingestion failed: {str(e)}"
        )

@router.post("/chat", response_model=ChatResponse)
async def chat_support(
    request_data: ChatRequest,
    request: Request
):
    """
    Executes multi-turn conversation via compiled LangGraph state machine.
    Dynamically routes between policy RAG, live order tracking, and product lookup.
    Supports optional X-Idempotency-Key header for response caching/deduplication.
    """
    start_time = time.perf_counter()
    session_id = request_data.session_id or "default-session"
    idempotency_key = request.headers.get("X-Idempotency-Key")
    redis_key = f"idem:support:{idempotency_key}" if idempotency_key else None

    # Check idempotency if header was provided
    if redis_key:
        try:
            is_new, cached = await idempotency_manager.check_and_lock(redis_key)
            if not is_new and cached:
                logger.info(f"Idempotency cache hit for {redis_key}")
                return ChatResponse(**cached["body"])
        except Exception as e:
            logger.warning(f"Idempotency check bypassed due to Redis error: {e}")

    from shared.common.tenant import set_tenant, TenantContext
    tenant_slug = request.headers.get("X-Tenant-ID") or "store_tech"
    set_tenant(TenantContext(slug=tenant_slug))

    effective_user_id = request_data.user_id or request.headers.get("X-User-ID")

    try:
        logger.info(f"Executing LangGraph agent for session_id={session_id}, user_id={effective_user_id}, tenant={tenant_slug}, query='{request_data.message}'")
        
        # Invoke LangGraph State Machine
        graph_result = await support_workflow.invoke(
            message=request_data.message,
            session_id=session_id,
            user_id=effective_user_id
        )

        answer = graph_result.get("final_answer") or "I apologize, but I could not process your request at this moment."
        sources = graph_result.get("sources", [])


        
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0

        response_payload = ChatResponse(
            answer=answer,
            sources=sources,
            model=settings.OPENROUTER_MODEL,
            processing_time_ms=round(elapsed_ms, 2)
        )

        # Save to idempotency store if key was provided
        if redis_key:
            try:
                await idempotency_manager.save_response(redis_key, 200, response_payload.model_dump())
            except Exception as e:
                logger.warning(f"Failed to save idempotency cache: {e}")

        return response_payload
    except CircuitBreakerOpenException as cb_err:
        if redis_key:
            await idempotency_manager.unlock(redis_key)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Circuit Breaker Active: {str(cb_err)}. Support service temporarily degraded."
        )
    except Exception as e:
        if redis_key:
            await idempotency_manager.unlock(redis_key)
        logger.error(f"Error during LangGraph chat execution: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Chat generation error: {str(e)}"
        )

@router.get("/conversations/{session_id}")
async def get_conversation_history(session_id: str):
    """Retrieves checkpointed message history for an ongoing multi-turn customer session"""
    try:
        messages = await support_workflow.get_history(session_id)
        history = []
        for m in messages:
            msg_type = getattr(m, "type", "unknown")
            history.append({
                "type": msg_type,
                "content": getattr(m, "content", "")
            })
        return {
            "session_id": session_id,
            "total_messages": len(history),
            "messages": history
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve history: {e}")

@router.post("/chat/stream")
async def chat_support_stream(request_data: ChatRequest):
    """Server-Sent Events (SSE) streaming endpoint for real-time token generation"""
    from src.domain.models import SupportQuery
    query = SupportQuery(
        user_id=request_data.user_id,
        session_id=request_data.session_id or "default-session",
        message=request_data.message
    )

    async def token_generator():
        try:
            async for token in rag_service.stream_query(query, top_k=request_data.top_k or 4):
                import json
                yield f"data: {json.dumps({'token': token})}\n\n"
                await asyncio.sleep(0.01)
            yield "data: [DONE]\n\n"
        except CircuitBreakerOpenException as cb_err:
            yield f"data: {{\"error\": \"Circuit breaker active: {str(cb_err)}\"}}\n\n"
        except Exception as e:
            yield f"data: {{\"error\": \"{str(e)}\"}}\n\n"

    return StreamingResponse(token_generator(), media_type="text/event-stream")
