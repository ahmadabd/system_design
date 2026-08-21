import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from shared.common.observability import setup_observability
from shared.common.resilience import CircuitBreakerOpenException
from src.infrastructure.config import settings
from src.infrastructure.qdrant_setup import qdrant_manager
from src.presentation.api import router, idempotency_manager, ingestion_service

logger = logging.getLogger("SupportApplication")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle coordinator establishing Qdrant collection, initial indexing, and resource management"""
    logger.info("Initializing Support Service resources in lifespan context...")
    
    # 1. Ensure Qdrant collection is ready and BM25 index is loaded
    try:
        qdrant_manager.ensure_collection()
        logger.info("Synchronizing Knowledge Base across Qdrant and BM25...")
        await ingestion_service.ingest_directory()
        
        # Pre-warm FlashRank Cross-Encoder model
        from src.adapter.reranker_adapter import reranker_adapter
        _ = reranker_adapter.ranker
        logger.info("FlashRank Cross-Encoder pre-warmed successfully.")
    except Exception as e:
        logger.warning(f"Could not connect to Qdrant or initialize knowledge base on startup: {e}. Will retry on demand.")


    yield

    logger.info("Tearing down Support Service resources in lifespan context...")
    await idempotency_manager.close()
    logger.info("Support Service lifespan teardown complete.")

app = FastAPI(
    title="Support AI Bounded Context Service",
    description="Agentic Customer Support Assistant with LangChain, LangGraph, Qdrant RAG, and OpenRouter",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/support"  # Prefix-stripped path routing for Traefik API Gateway
)

# OpenTelemetry and Prometheus observability setup
setup_observability(app, service_name=settings.SERVICE_NAME)

@app.exception_handler(CircuitBreakerOpenException)
async def circuit_breaker_exception_handler(request: Request, exc: CircuitBreakerOpenException):
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": f"Service temporarily unavailable: {str(exc)}"}
    )

app.include_router(router)
