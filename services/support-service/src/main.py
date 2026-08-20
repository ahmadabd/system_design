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
    
    # 1. Ensure Qdrant collection is ready
    try:
        qdrant_manager.ensure_collection()
        # Check if collection is empty, trigger initial ingestion
        info = qdrant_manager.client.get_collection(settings.QDRANT_COLLECTION_NAME)
        if getattr(info, "points_count", 0) == 0:
            logger.info("Qdrant collection is empty. Triggering initial knowledge base ingestion...")
            await ingestion_service.ingest_directory()
        else:
            logger.info(f"Qdrant collection already has {info.points_count} points indexed.")
    except Exception as e:
        logger.warning(f"Could not connect to Qdrant or initialize collection on startup: {e}. Will retry on demand.")

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
