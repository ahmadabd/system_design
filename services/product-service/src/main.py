import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from shared.common.messaging import KafkaManager
from shared.common.observability import setup_observability, register_graceful_shutdown
from src.infrastructure.config import settings
from src.infrastructure.db_setup import db
from src.presentation.api import router, mq_manager
from src.adapter.messaging_sub import ProductMessagingSubscriber
from shared.common.outbox import OutboxPublisher
from shared.common.tenant_registry import TenantRegistry
from shared.common.tenant_middleware import TenantMiddleware
from shared.common.tenant_provisioner import TenantProvisioner
from fastapi import APIRouter
from pydantic import BaseModel

logger = logging.getLogger("ProductApplication")

# Separate independent broker connection for background consumer threads
background_mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

tenant_registry = TenantRegistry(db._engine)
tenant_provisioner = TenantProvisioner(db._engine, tenant_registry)

# Initialize outbox publisher background worker
outbox_publisher = OutboxPublisher(db, mq_manager, tenant_registry=tenant_registry)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle coordinator establishing background subscription listeners, database pools, and idempotency tables"""
    await tenant_registry.bootstrap()
    logger.info("Applying database schema migrations...")
    import asyncio
    await asyncio.to_thread(db.run_migrations)

    # Programmatically create the SQL-backed Inbox Pattern message deduplication table
    logger.info("Programmatically ensuring idempotent_consumers inbox table exists...")
    async with db._engine.begin() as conn:
        pass
    logger.info("Idempotent consumers table, default store, and non-negative stock constraint initialized successfully.")

    # Start Outbox Publisher background worker
    outbox_publisher.start()

    # Warm up Product Bloom Filter with active product IDs
    try:
        from src.presentation.api import product_bloom_filter
        from src.adapter.db_models import ProductDB
        from sqlalchemy.future import select
        async with db.get_session() as session:
            stmt = select(ProductDB.id)
            res = await session.execute(stmt)
            for (pid,) in res.all():
                product_bloom_filter.add(str(pid))
        logger.info(f"Product Bloom Filter warmed up with {product_bloom_filter.count} items.")
    except Exception as bf_err:
        logger.warning(f"Failed to warm up Product Bloom Filter: {bf_err}")

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

@app.get("/health", tags=["System"])
async def health_check():
    """System health check endpoint"""
    return {"status": "healthy", "service": "product-service"}

app.add_middleware(TenantMiddleware, registry=tenant_registry)

admin_router = APIRouter(prefix="/admin", tags=["Admin"])

class ProvisionTenantRequest(BaseModel):
    slug: str

@admin_router.post("/tenants", status_code=201)
async def provision_tenant(body: ProvisionTenantRequest):
    await tenant_provisioner.provision(body.slug)
    return {"status": "provisioned", "slug": body.slug}

@admin_router.get("/tenants")
async def list_tenants():
    return {"tenants": tenant_registry.list_all()}

app.include_router(admin_router)
app.include_router(router)

# Unify OpenTelemetry tracing, structured JSON logging, and Prometheus metrics
setup_observability(app, settings.SERVICE_NAME)

# Register cooperative graceful SIGTERM/SIGINT shutdown with 3s traffic draining
register_graceful_shutdown(
    app, 
    [outbox_publisher.stop, db.close, mq_manager.close, background_mq_manager.close]
)
