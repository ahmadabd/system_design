import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import httpx

from src.infrastructure.config import settings
from src.infrastructure.observability import setup_copilot_observability, instrument_app
from src.infrastructure.clickhouse_client import clickhouse_client
from src.adapter.qdrant_policy_adapter import policy_adapter
from src.adapter.micro_batcher import micro_batcher
from src.adapter.messaging_sub import CopilotEventConsumer
from src.presentation.api import router as copilot_router

logger = logging.getLogger("CopilotMain")
consumer = CopilotEventConsumer()

async def _cold_start_backfill() -> None:
    """
    On cold startup, hydrates historical products from product-service
    into ClickHouse ReplacingMergeTree tables via vectorized batch insert.
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
                            records = []
                            for p in raw_products:
                                records.append({
                                    "id": int(p.get("id", 0)),
                                    "tenant_id": str(tenant),
                                    "name": str(p.get("name", "")),
                                    "category": "Electronics",
                                    "price": float(p.get("price", 0.0)),
                                    "stock": int(p.get("stock", 0)),
                                    "store_id": int(p.get("store_id", 1))
                                })
                            if records:
                                inserted = await asyncio.to_thread(
                                    clickhouse_client.insert_batch,
                                    "products_analytics",
                                    records
                                )
                                total_backfilled += inserted
                except Exception as e:
                    logger.debug(f"Cold-start backfill skip for tenant '{tenant}': {e}")

        if total_backfilled > 0:
            logger.info(f"Cold-start hydration completed: backfilled {total_backfilled} historical products into ClickHouse OLAP.")
        else:
            # Seed baseline demo products if product-service was empty or offline
            baseline_products = [
                {"id": 1, "tenant_id": "store_tech", "name": "Gaming Laptop (16GB RAM, RTX 4070)", "category": "Laptops", "price": 1299.99, "stock": 12, "store_id": 1},
                {"id": 2, "tenant_id": "store_tech", "name": "Gaming Laptop Pro (32GB RAM, RTX 4080)", "category": "Laptops", "price": 1899.99, "stock": 5, "store_id": 1},
                {"id": 3, "tenant_id": "store_tech", "name": "Wireless Mechanical Keyboard", "category": "Keyboards", "price": 50.0, "stock": 99, "store_id": 1},
                {"id": 4, "tenant_id": "store_tech", "name": "Ultra HD 4K Monitor (27 inch)", "category": "Monitors", "price": 399.0, "stock": 23, "store_id": 1},
                {"id": 5, "tenant_id": "store_tech", "name": "Shure SM7B Dynamic Cardioid Vocal Microphone", "category": "Microphones", "price": 399.0, "stock": 15, "store_id": 1},
                {"id": 6, "tenant_id": "store_tech", "name": "Rode PodMic Dynamic Podcasting Microphone", "category": "Microphones", "price": 99.0, "stock": 25, "store_id": 1}
            ]
            baseline_orders = [
                {"id": 101, "tenant_id": "store_tech", "user_id": 1, "total_amount": 1349.99, "status": "CONFIRMED"},
                {"id": 102, "tenant_id": "store_tech", "user_id": 2, "total_amount": 498.00, "status": "CONFIRMED"},
                {"id": 103, "tenant_id": "store_tech", "user_id": 3, "total_amount": 1899.99, "status": "PENDING"}
            ]
            baseline_items = [
                {"id": 1, "order_id": 101, "tenant_id": "store_tech", "product_id": 1, "product_name": "Gaming Laptop (16GB RAM, RTX 4070)", "category": "Laptops", "unit_price": 1299.99, "quantity": 1},
                {"id": 2, "order_id": 101, "tenant_id": "store_tech", "product_id": 3, "product_name": "Wireless Mechanical Keyboard", "category": "Keyboards", "unit_price": 50.00, "quantity": 1},
                {"id": 3, "order_id": 102, "tenant_id": "store_tech", "product_id": 5, "product_name": "Shure SM7B Dynamic Cardioid Vocal Microphone", "category": "Microphones", "unit_price": 399.00, "quantity": 1},
                {"id": 4, "order_id": 102, "tenant_id": "store_tech", "product_id": 6, "product_name": "Rode PodMic Dynamic Podcasting Microphone", "category": "Microphones", "unit_price": 99.00, "quantity": 1}
            ]
            baseline_payments = [
                {"id": 201, "order_id": 101, "tenant_id": "store_tech", "amount": 1349.99, "status": "SUCCEEDED", "payment_method": "STRIPE", "transaction_id": "txn_strp_001"},
                {"id": 202, "order_id": 102, "tenant_id": "store_tech", "amount": 498.00, "status": "SUCCEEDED", "payment_method": "CARD", "transaction_id": "txn_crd_002"}
            ]
            await asyncio.to_thread(clickhouse_client.insert_batch, "products_analytics", baseline_products)
            await asyncio.to_thread(clickhouse_client.insert_batch, "orders_analytics", baseline_orders)
            await asyncio.to_thread(clickhouse_client.insert_batch, "order_items_analytics", baseline_items)
            await asyncio.to_thread(clickhouse_client.insert_batch, "payments_analytics", baseline_payments)
            logger.info("Cold-start hydration initialized baseline analytical records in ClickHouse.")
    except Exception as e:
        logger.warning(f"Could not perform cold-start backfill ({e}).")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Setup Observability
    setup_copilot_observability(settings.SERVICE_NAME)
    logger.info(f"Starting {settings.SERVICE_NAME} on port {settings.PORT} (Environment: {settings.ENVIRONMENT})...")
    
    # 2. Initialize ClickHouse schemas & Qdrant collections
    clickhouse_client.init_database_and_tables()
    policy_adapter.init_collections()
    
    # 3. Start In-Memory Micro-Batcher
    await micro_batcher.start()
    
    # 4. Run Cold-Start Hydration
    await _cold_start_backfill()
    
    # 5. Start Kafka Consumer Event Ingestion
    await consumer.start()
    
    yield
    
    # Graceful Teardown
    logger.info(f"Shutting down {settings.SERVICE_NAME}...")
    await consumer.stop()
    await micro_batcher.stop()

app = FastAPI(
    title="Merchant Copilot Service",
    description="Hybrid Text-to-SQL + Policy RAG via ClickHouse OLAP and LangGraph",
    version="1.0.0",
    lifespan=lifespan
)

import signal

def register_graceful_shutdown(app: FastAPI, cleanup_callbacks: list, drain_seconds: float = 3.0):
    """Registers signal handlers to cooperatively drain traffic and flush buffers cleanly"""
    shut_logger = logging.getLogger("ShutdownHandler")

    async def shutdown_handler(sig_num):
        shut_logger.warning(f"Received shutdown signal {signal.Signals(sig_num).name} (SIGTERM/SIGINT). Draining in-flight traffic...")
        shut_logger.info(f"Traffic draining in progress: sleeping for {drain_seconds} seconds...")
        await asyncio.sleep(drain_seconds)

        shut_logger.info("Executing merchant-copilot buffer drains and cleanup callbacks...")
        for callback in cleanup_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                shut_logger.error(f"Error during cleanup callback: {e}", exc_info=True)

        shut_logger.warning("Resource cleanup, buffer flush, and traffic draining completed. Terminating process.")

    try:
        loop = asyncio.get_event_loop()
        for sig in [signal.SIGTERM, signal.SIGINT]:
            try:
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))
            except (ValueError, NotImplementedError):
                pass
    except Exception:
        pass

instrument_app(app)
app.include_router(copilot_router)

# Register cooperative graceful SIGTERM/SIGINT shutdown with buffer drain & traffic draining
register_graceful_shutdown(
    app,
    [consumer.stop, micro_batcher.stop]
)
