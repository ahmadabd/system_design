import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from shared.common.database import Base
from shared.common.observability import setup_observability, register_graceful_shutdown
from src.infrastructure.config import settings
from src.infrastructure.db_setup import db
from src.presentation.api import router
from src.adapter.messaging_sub import WebhookMessagingSubscriber

logger = logging.getLogger("WebhookApplication")

# Initialize background messaging subscriber
subscriber = WebhookMessagingSubscriber(settings.KAFKA_BOOTSTRAP_SERVERS, db)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle coordinator establishing background subscription listeners and database schema initialization"""
    logger.info("Initializing Webhook Service database schema...")
    # Map SQLAlchemy tables to PostgreSQL DB with connection retries
    await db.initialize_schema(Base, logger)

    # Programmatically create the SQL-backed Inbox Pattern message deduplication table
    logger.info("Programmatically ensuring idempotent_consumers inbox table exists...")
    async with db._engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS idempotent_consumers (
                message_id VARCHAR(255) PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        # Ensure column is_famous exists in materialized_stores
        await conn.execute(text("""
            ALTER TABLE materialized_stores ADD COLUMN IF NOT EXISTS is_famous BOOLEAN DEFAULT FALSE
        """))
    logger.info("Idempotent consumers table and migrations initialized successfully.")

    # Start Kafka consumers
    await subscriber.start()

    yield

    logger.info("Tearing down Webhook Service resources in lifespan context...")
    await subscriber.stop()
    await db.close()
    logger.info("Webhook Service lifespan teardown complete.")

from fastapi import Request, status
from fastapi.responses import JSONResponse
from shared.common.resilience import CircuitBreakerOpenException

app = FastAPI(
    title="Webhook Bounded Context Service",
    description="Resilient Outbound Store Webhook Delivery Context",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/webhooks"  # Prefix-stripped path routing for Traefik API Gateway
)

@app.exception_handler(CircuitBreakerOpenException)
async def circuit_breaker_exception_handler(request: Request, exc: CircuitBreakerOpenException):
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": f"Service temporarily unavailable: {str(exc)}"}
    )

# Unify OpenTelemetry tracing, structured JSON logging, and Prometheus metrics
setup_observability(app, settings.SERVICE_NAME)

# Register cooperative graceful SIGTERM/SIGINT shutdown with 3s traffic draining
register_graceful_shutdown(
    app, 
    [subscriber.stop, db.close]
)

@app.get("/health", tags=["System"])
async def health_check():
    """System health check endpoint"""
    return {"status": "healthy", "service": "webhook-service"}

app.include_router(router)
