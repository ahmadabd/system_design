"""
Kafka Event Consumer for Discovery Service.
Listens to product lifecycle events (product.created, product.updated, product.deleted)
and automatically maintains real-time vector projections in Qdrant.
"""
import asyncio
import json
import logging
from typing import Optional
from src.adapter.qdrant_adapter import QdrantDiscoveryAdapter
from src.infrastructure.config import settings

logger = logging.getLogger("DiscoveryEventConsumer")


def _enrich_product_specs(name: str) -> tuple[str, str]:
    """Generates descriptive semantic category and specs from product name."""
    name_lower = name.lower()
    category = "Electronics"
    specs = "High quality hardware equipment with warranty and verified stock."

    if "laptop" in name_lower or "notebook" in name_lower or "macbook" in name_lower:
        category = "Laptops"
        specs = "High performance computing device with fast NVMe storage, DDR5 RAM, and high refresh display."
    elif "keyboard" in name_lower:
        category = "Keyboards"
        specs = "Mechanical tactile keyboard with low latency, hot-swappable switches, and RGB lighting."
    elif "monitor" in name_lower or "screen" in name_lower or "display" in name_lower:
        category = "Monitors"
        specs = "Ultra HD high resolution IPS display with HDR, wide color gamut, and low response time."
    elif "mouse" in name_lower or "trackpad" in name_lower:
        category = "Accessories"
        specs = "Ergonomic precision optical mouse with programmable buttons and high DPI sensor."
    elif "mic" in name_lower or "microphone" in name_lower:
        category = "Microphones"
        specs = "Studio grade dynamic cardioid microphone for clean broadcast voice and streaming."
    elif "headphone" in name_lower or "earphone" in name_lower or "headset" in name_lower:
        category = "Headphones"
        specs = "Professional closed-back studio monitor headphones with wide frequency range and clear soundstage."

    return category, specs


class DiscoveryEventConsumer:
    """Consumes Kafka events to automatically synchronize Qdrant vector store."""

    def __init__(self, qdrant_adapter: Optional[QdrantDiscoveryAdapter] = None):
        self.qdrant = qdrant_adapter or QdrantDiscoveryAdapter()
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

    async def handle_product_event(self, routing_key: str, payload: dict) -> None:
        """Processes a single product domain event."""
        event_type = payload.get("event_type", routing_key)
        tenant_id = payload.get("metadata", {}).get("tenant_slug") or "store_tech"

        logger.info(f"Received Kafka event '{event_type}' for tenant '{tenant_id}'.")

        if event_type in ["ProductCreated", "ProductUpdated", "product.created", "product.updated"]:
            product_id = payload.get("product_id")
            name = payload.get("name", "")
            price = float(payload.get("price", 0.0))
            stock = int(payload.get("stock", 0))
            store_id = int(payload.get("store_id", 1))

            category, specs = _enrich_product_specs(name)

            product_dict = {
                "id": product_id,
                "name": name,
                "price": price,
                "stock": stock,
                "store_id": store_id,
                "category": category,
                "specs": specs
            }

            self.qdrant.index_products([product_dict], tenant_id=tenant_id)
            logger.info(f"Auto-synced product #{product_id} ('{name}') into Qdrant for tenant '{tenant_id}'.")

        elif event_type in ["ProductDeleted", "product.deleted"]:
            product_id = payload.get("product_id")
            if product_id:
                self.qdrant.delete_product(product_id=product_id, tenant_id=tenant_id)
                logger.info(f"Deleted product #{product_id} from Qdrant vector index for tenant '{tenant_id}'.")

    async def start(self, bootstrap_servers: str = "kafka:9092") -> None:
        """Starts background Kafka consumer loop."""
        self.is_running = True
        logger.info(f"Starting DiscoveryEventConsumer on Kafka {bootstrap_servers}...")

        try:
            from aiokafka import AIOKafkaConsumer
            consumer = AIOKafkaConsumer(
                "product.created",
                "product.updated",
                "product.deleted",
                bootstrap_servers=bootstrap_servers,
                group_id="discovery-service-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            await consumer.start()
            logger.info("AIOKafkaConsumer connected successfully. Listening for product events...")

            try:
                while self.is_running:
                    msg = await consumer.getone()
                    topic = msg.topic
                    payload = msg.value
                    await self.handle_product_event(topic, payload)
            finally:
                await consumer.stop()
        except Exception as e:
            logger.warning(f"AIOKafkaConsumer encountered error or offline broker: {e}. Event listener stopped.")

    def start_background(self, bootstrap_servers: str = "kafka:9092") -> None:
        """Spawns non-blocking asyncio task for Kafka consumption."""
        self._task = asyncio.create_task(self.start(bootstrap_servers=bootstrap_servers))

    def stop(self) -> None:
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
