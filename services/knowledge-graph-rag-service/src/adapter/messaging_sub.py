import asyncio
import json
import logging
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaConsumer

from src.domain.graph_entities import GraphNode, GraphEdge, EntityType, RelationType
from src.infrastructure.graph_store import KnowledgeGraphStore
from src.adapter.qdrant_entity_adapter import QdrantEntityAdapter

logger = logging.getLogger("GraphRAGMessagingSub")


class GraphRAGEventConsumer:
    """
    Kafka consumer that dynamically hydrates and expands the Knowledge Graph
    and Qdrant vector index in real-time as business events arrive.
    """
    def __init__(self, graph_store: KnowledgeGraphStore, qdrant_adapter: QdrantEntityAdapter):
        self.graph_store = graph_store
        self.qdrant_adapter = qdrant_adapter
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self, bootstrap_servers: str = "kafka:9092"):
        """Starts Kafka consumer in background task"""
        topics = [
            "product.created",
            "product.updated",
            "inventory.failed",
            "order.created"
        ]
        try:
            self.consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=bootstrap_servers,
                group_id="graphrag-consumer-group-v2",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            await self.consumer.start()
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            logger.info(f"GraphRAG Kafka event consumer started on topics: {topics}")
        except Exception as e:
            logger.warning(f"Could not start GraphRAG Kafka consumer ({e}). Operating in memory graph mode.")

    async def stop(self):
        """Stops Kafka consumer safely"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            try:
                await self.consumer.stop()
            except Exception as e:
                logger.warning(f"Error closing GraphRAG consumer: {e}")
        logger.info("GraphRAG Kafka consumer stopped cleanly.")

    async def _consume_loop(self):
        """Processes incoming Kafka integration events and updates graph topology"""
        while self._running:
            try:
                msg = await self.consumer.getone()
                topic = msg.topic
                payload = msg.value or {}
                await self._handle_event(topic, payload)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing GraphRAG integration event: {e}", exc_info=True)
                await asyncio.sleep(1.0)

    async def _handle_event(self, topic: str, payload: Dict[str, Any]):
        """Maps event payloads to Knowledge Graph nodes and edges"""
        try:
            if topic in ["product.created", "product.updated"]:
                raw_id = payload.get("product_id") or payload.get("id") or "unknown"
                prod_id = f"prod_{raw_id}"
                name = payload.get("name", "Product")
                meta = payload.get("metadata", {})
                tenant = meta.get("tenant_slug") if isinstance(meta, dict) else None
                tenant = tenant or payload.get("tenant_id") or "store_tech"
                price = payload.get("price", 0.0)
                node = GraphNode(
                    id=prod_id,
                    name=name,
                    type=EntityType.PRODUCT,
                    description=f"Catalog item: {name} (Price: ${price})",
                    properties=payload,
                    tenant_id=tenant
                )
                self.graph_store.add_node(node)
                self.graph_store.add_edge(GraphEdge(
                    source=prod_id,
                    target="store_tech",
                    relation=RelationType.SOLD_BY,
                    description=f"Retails on {tenant} catalog store."
                ))
                self.qdrant_adapter.index_entities([node])
                logger.info(f"GraphRAG dynamically ingested product node: {prod_id} ('{name}').")

            elif topic == "inventory.failed":
                raw_id = payload.get("product_id") or payload.get("id") or "unknown"
                prod_id = f"prod_{raw_id}"
                defect_id = f"defect_stockout_{raw_id}"
                
                defect_node = GraphNode(
                    id=defect_id,
                    name=f"Stock Depletion Outage on Product #{raw_id}",
                    type=EntityType.DEFECT,
                    description="Inventory reservation failed due to stock exhaustion.",
                    properties=payload,
                    tenant_id=payload.get("tenant_id", "store_tech")
                )
                self.graph_store.add_node(defect_node)
                self.graph_store.add_edge(GraphEdge(
                    source=prod_id,
                    target=defect_id,
                    relation=RelationType.REPORTED_DEFECT,
                    description="Product encountered live stockout outage during checkout."
                ))
                logger.info(f"GraphRAG dynamically linked stockout defect to product: {prod_id}.")

        except Exception as e:
            logger.warning(f"Failed to ingest event into knowledge graph: {e}")
