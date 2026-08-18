import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from shared.common.messaging import KafkaManager
from shared.common.observability import setup_observability, register_graceful_shutdown
from src.infrastructure.config import settings
from src.infrastructure.db_setup import db
from src.presentation.api import router
from src.adapter.messaging_sub import ReportingMessagingSubscriber
from shared.common.tenant_registry import TenantRegistry
from shared.common.tenant_middleware import TenantMiddleware

logger = logging.getLogger("ReportingApplication")

# Separate independent broker connection for background consumer threads
background_mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

tenant_registry = TenantRegistry(db._engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan coordinator establishing background subscription listeners and database schemas"""
    await tenant_registry.bootstrap()
    logger.info("Applying database schema migrations...")
    import asyncio
    await asyncio.to_thread(db.run_migrations)

    # Programmatically create the SQL-backed Inbox Pattern message deduplication table
    logger.info("Programmatically ensuring idempotent_consumers inbox table exists...")
    async with db._engine.begin() as conn:
        pass
    logger.info("Idempotent consumers table initialized successfully.")

    # Open persistent Kafka connection for background subscriber listener
    await background_mq_manager.connect()
    from src.presentation.api import idempotency_manager
    subscriber = ReportingMessagingSubscriber(background_mq_manager, redis_client=idempotency_manager.redis)
    await subscriber.start_listening()

    yield

    logger.info("Tearing down Reporting Service resources in lifespan context...")
    await db.close()
    await background_mq_manager.close()
    logger.info("Reporting Service lifespan teardown complete.")

from fastapi import Request, status
from fastapi.responses import JSONResponse
from shared.common.resilience import CircuitBreakerOpenException

app = FastAPI(
    title="Customer Reporting & Analytics Bounded Context Service",
    description="Vaughn Vernon 5-Layer DDD E-Commerce CQRS Reporting Bounded Context",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/reporting"  # Prefix-stripped path routing for Traefik API Gateway
)

@app.exception_handler(CircuitBreakerOpenException)
async def circuit_breaker_exception_handler(request: Request, exc: CircuitBreakerOpenException):
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": f"Service temporarily unavailable: {str(exc)}"}
    )

@app.get("/health", tags=["System"])
async def health_check():
    """System health check endpoint"""
    return {"status": "healthy", "service": "reporting-service"}

app.add_middleware(TenantMiddleware, registry=tenant_registry)
app.include_router(router)

# Unify OpenTelemetry tracing, structured JSON logging, and Prometheus metrics
setup_observability(app, settings.SERVICE_NAME)

# Register cooperative graceful SIGTERM/SIGINT shutdown with 3s traffic draining
register_graceful_shutdown(
    app, 
    [db.close, background_mq_manager.close]
)
