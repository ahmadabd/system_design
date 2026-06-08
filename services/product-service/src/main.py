import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from shared.common.database import Base
from shared.common.messaging import KafkaManager
from shared.common.observability import setup_observability, register_graceful_shutdown
from src.infrastructure.config import settings
from src.infrastructure.db_setup import db
from src.presentation.api import router, mq_manager
from src.adapter.messaging_sub import ProductMessagingSubscriber
from shared.common.outbox import OutboxPublisher

logger = logging.getLogger("ProductApplication")

# Separate independent broker connection for background consumer threads
background_mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

# Initialize outbox publisher background worker
outbox_publisher = OutboxPublisher(db, mq_manager)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle coordinator establishing background subscription listeners, database pools, and idempotency tables"""
    logger.info("Initializing Product Service database schema...")
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
        # Seed default store (ID 1)
        await conn.execute(text("""
            INSERT INTO stores (id, name, webhook_url)
            VALUES (1, 'Default Store', 'http://localhost/webhooks/default')
            ON CONFLICT (id) DO NOTHING
        """))
        # Sync the sequence so next insert starts at 2
        await conn.execute(text("""
            SELECT setval(pg_get_serial_sequence('stores', 'id'), coalesce(max(id), 1), max(id) IS NOT null) FROM stores;
        """))
    logger.info("Idempotent consumers table and default store initialized successfully.")

    # Start Outbox Publisher background worker
    outbox_publisher.start()

    # Open persistent Kafka connection for background subscriber listener
    await background_mq_manager.connect()
    subscriber = ProductMessagingSubscriber(background_mq_manager)
    await subscriber.start_listening()

    yield

    logger.info("Tearing down Product Service resources in lifespan context...")
    await outbox_publisher.stop()
    await db.close()
    await mq_manager.close()
    await background_mq_manager.close()
    logger.info("Product Service teardown complete.")

from fastapi import Request, status
from fastapi.responses import JSONResponse
from shared.common.resilience import CircuitBreakerOpenException

app = FastAPI(
    title="Product Bounded Context Service",
    description="Vaughn Vernon 5-Layer DDD E-Commerce Platform",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/products"  # Prefix-stripped path routing for Traefik API Gateway Swagger docs
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
    [outbox_publisher.stop, db.close, mq_manager.close, background_mq_manager.close]
)

@app.get("/health", tags=["System"])
async def health_check():
    """System health check endpoint"""
    return {"status": "healthy", "service": "product-service"}

app.include_router(router)
