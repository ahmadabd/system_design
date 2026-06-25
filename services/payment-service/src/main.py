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
from src.adapter.messaging_sub import PaymentMessagingSubscriber
from shared.common.outbox import OutboxPublisher

logger = logging.getLogger("PaymentApplication")

# Separate independent broker connection for background consumer threads
background_mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

# Initialize outbox publisher background worker
outbox_publisher = OutboxPublisher(db, mq_manager)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle coordinator establishing background subscription listeners, database pools, and idempotency tables"""
    logger.info("Applying database schema migrations...")
    import asyncio
    await asyncio.to_thread(db.run_migrations)

    # Programmatically create the SQL-backed Inbox Pattern message deduplication table
    logger.info("Programmatically ensuring idempotent_consumers inbox table exists...")
    async with db._engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS idempotent_consumers (
                message_id VARCHAR(255) PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
    logger.info("Idempotent consumers table initialized successfully.")

    # Start Outbox Publisher background worker
    outbox_publisher.start()

    # Open persistent Kafka connection for background subscriber listener
    await background_mq_manager.connect()
    subscriber = PaymentMessagingSubscriber(background_mq_manager)
    await subscriber.start_listening()

    yield

    logger.info("Tearing down Payment Service resources in lifespan context...")
    await outbox_publisher.stop()
    await db.close()
    await mq_manager.close()
    await background_mq_manager.close()
    logger.info("Payment Service lifespan teardown complete.")

from fastapi import Request, status
from fastapi.responses import JSONResponse
from shared.common.resilience import CircuitBreakerOpenException

app = FastAPI(
    title="Payment Bounded Context Service",
    description="Saga Choreography Payment Verification Context",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/payments"  # Prefix-stripped path routing for Traefik API Gateway Swagger docs
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
    return {"status": "healthy", "service": "payment-service"}

app.include_router(router)
