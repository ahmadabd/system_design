import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from shared.common.observability import register_graceful_shutdown
from src.infrastructure.config import settings
from src.infrastructure.observability import setup_dispute_observability, instrument_app
from src.adapter.messaging_pub import dispute_messaging_pub
from src.presentation.api import router as dispute_router, drain_in_flight_claims

logger = logging.getLogger("DisputeMain")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle coordinator establishing Kafka producers and graceful shutdown"""
    logger.info(f"Starting {settings.SERVICE_NAME} on port {settings.PORT} (Environment: {settings.ENVIRONMENT})...")
    
    # 1. Start Kafka producer
    await dispute_messaging_pub.start()
    logger.info("Dispute resolution messaging publisher initialized.")

    yield

    # 2. Teardown
    logger.info("Tearing down Dispute Resolution Service resources...")
    await dispute_messaging_pub.stop()
    logger.info("Dispute Resolution Service shutdown complete.")


# Initialize Observability & Logging
setup_dispute_observability(service_name=settings.SERVICE_NAME)

app = FastAPI(
    title="Dispute Resolution & Claims Service",
    description="Multi-Agent Negotiation Arena & Judicial Arbitration Engine with Self-RAG & GraphRAG Evidence",
    version="1.0.0",
    lifespan=lifespan
)

# Register Graceful Shutdown & Traffic Draining
register_graceful_shutdown(app, cleanup_callbacks=[drain_in_flight_claims, dispute_messaging_pub.stop])

# Instrument FastAPI and Prometheus
instrument_app(app)

# Include API Router
app.include_router(dispute_router, prefix="/disputes")
app.include_router(dispute_router)  # Also expose at root for direct container calls
