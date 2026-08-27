import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
import httpx

from src.infrastructure.config import settings
from src.infrastructure.observability import setup_discovery_observability
from src.presentation.api import router as discovery_router
from src.adapter.qdrant_adapter import QdrantDiscoveryAdapter
from src.adapter.messaging_sub import _enrich_product_specs, DiscoveryEventConsumer

logger = logging.getLogger("DiscoveryMain")


async def _cold_start_backfill(adapter: QdrantDiscoveryAdapter) -> None:
    """
    On cold startup, queries product-service (PostgreSQL product_db) to backfill
    and index all pre-existing historical products into Qdrant vector store.
    """
    url = f"{settings.PRODUCT_SERVICE_URL}/"
    tenants = ["store_tech", "store_gaming", "public"]
    total_backfilled = 0

    try:
        async with httpx.AsyncClient(timeout=5.0, trust_env=False) as client:
            for tenant in tenants:
                try:
                    resp = await client.get(url, headers={"X-Tenant-ID": tenant})
                    if resp.status_code == 200:
                        raw_products = resp.json()
                        if isinstance(raw_products, list) and raw_products:
                            enriched = []
                            for p in raw_products:
                                name = p.get("name", "")
                                cat, specs = _enrich_product_specs(name)
                                enriched.append({
                                    "id": p.get("id"),
                                    "name": name,
                                    "price": float(p.get("price", 0.0)),
                                    "stock": int(p.get("stock", 0)),
                                    "store_id": int(p.get("store_id", 1)),
                                    "category": cat,
                                    "specs": specs
                                })
                            count = adapter.index_products(enriched, tenant_id=tenant)
                            total_backfilled += count
                except Exception as e:
                    logger.debug(f"Backfill skip for tenant '{tenant}': {e}")

        if total_backfilled > 0:
            logger.info(f"Cold-start hydration completed: backfilled {total_backfilled} historical products from product-service.")
        else:
            logger.info("Cold-start hydration: no additional historical products found or product-service offline. Baseline catalog active.")
    except Exception as e:
        logger.warning(f"Could not perform cold-start backfill ({e}). Baseline catalog active.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize OpenTelemetry and logging
    setup_discovery_observability(settings.SERVICE_NAME)
    logger.info(f"Starting {settings.SERVICE_NAME} on port {settings.PORT} (Environment: {settings.ENVIRONMENT})...")
    
    adapter = QdrantDiscoveryAdapter()
    adapter.init_collection()
    
    # 1. Pre-index default catalog for instant availability
    default_catalog = [
        {"id": 1, "name": "Gaming Laptop (16GB RAM, RTX 4070)", "price": 1299.99, "stock": 12, "store_id": 1, "category": "Laptops", "specs": "15.6 inch 144Hz screen, 1TB SSD, 16GB DDR5"},
        {"id": 2, "name": "Gaming Laptop Pro (32GB RAM, RTX 4080)", "price": 1899.99, "stock": 5, "store_id": 1, "category": "Laptops", "specs": "16 inch 240Hz OLED, 2TB SSD, 32GB DDR5"},
        {"id": 3, "name": "Wireless Mechanical Keyboard", "price": 50.0, "stock": 99, "store_id": 1, "category": "Keyboards", "specs": "Hot-swappable RGB red switches, Bluetooth 5.2"},
        {"id": 4, "name": "Ultra HD 4K Monitor (27 inch)", "price": 399.0, "stock": 23, "store_id": 1, "category": "Monitors", "specs": "3840x216 concur IPS, HDR400, USB-C 65W charging"},
        {"id": 5, "name": "Ergonomic Optical Gaming Mouse", "price": 35.0, "stock": 50, "store_id": 1, "category": "Accessories", "specs": "26000 DPI sensor, 65g lightweight, wireless 2.4G"}
    ]
    adapter.index_products(default_catalog, tenant_id="store_tech")
    adapter.index_products(default_catalog, tenant_id="store_gaming")
    adapter.index_products(default_catalog, tenant_id="public")
    
    # 2. Asynchronous Cold-Start backfill from product-service
    asyncio.create_task(_cold_start_backfill(adapter))

    # 3. Start Kafka background event consumer for automatic real-time Qdrant sync
    consumer = DiscoveryEventConsumer(qdrant_adapter=adapter)
    consumer.start_background(bootstrap_servers=getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    
    yield
    consumer.stop()
    logger.info(f"Shutting down {settings.SERVICE_NAME}...")


app = FastAPI(
    title="Product Discovery & Bundle Builder Service",
    description="Advanced RAG with HyDE, Multi-Query Decomposition, and LangGraph State Machine for E-Commerce",
    version="1.0.0",
    lifespan=lifespan
)

# Prometheus metrics instrumentation
Instrumentator().instrument(app).expose(app)

# Include routes
app.include_router(discovery_router)
