import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from shared.common.database import Base
from shared.common.observability import setup_observability, register_graceful_shutdown
from src.infrastructure.config import settings
from src.infrastructure.db_setup import db
from src.presentation.api import router, mq_manager
from shared.common.outbox import OutboxPublisher

logger = logging.getLogger("UserApplication")

# Initialize outbox publisher background worker
outbox_publisher = OutboxPublisher(db, mq_manager)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle startup and shutdown task coordinator"""
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
    
    yield
    
    logger.info("Tearing down User Service resources in lifespan context...")
    await outbox_publisher.stop()
    await db.close()
    await mq_manager.close()
    logger.info("User Service teardown complete.")

from fastapi import Request, status
from fastapi.responses import JSONResponse
from shared.common.resilience import CircuitBreakerOpenException

app = FastAPI(
    title="User Bounded Context Service",
    description="Vaughn Vernon 5-Layer DDD E-Commerce Platform",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/users"  # Prefix-stripped path routing for Traefik API Gateway Swagger docs
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
    [outbox_publisher.stop, db.close, mq_manager.close]
)

@app.get("/health", tags=["System"])
async def health_check():
    """System health validation check"""
    return {"status": "healthy", "service": "user-service"}

# Include inbound REST router
app.include_router(router)
