import asyncio
import json
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from src.infrastructure.config import settings
from src.adapter.micro_batcher import micro_batcher

logger = logging.getLogger("CopilotEventConsumer")

class CopilotEventConsumer:
    """Subscribes to platform Kafka events and pipes them into the ClickHouse Micro-Batcher"""
    def __init__(self, bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.topics = [
            "product.created",
            "product.updated",
            "product.deleted",
            "order.created",
            "order.confirmed",
            "payment.succeeded",
            "payment.failed"
        ]
        self.consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self):
        """Initializes the Kafka Consumer with group 'merchant-copilot-group'"""
        try:
            logger.info(f"Connecting Copilot Kafka Consumer to {self.bootstrap_servers}...")
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id="merchant-copilot-group",
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # Manual commit ONLY after ClickHouse persists batch
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            await self.consumer.start()
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            logger.info(f"Copilot Kafka Consumer actively listening to topics: {self.topics}")
        except Exception as e:
            logger.warning(f"Could not connect Copilot Consumer to Kafka ({e}). Running in offline/standalone mode.")

    async def stop(self):
        """Safely stops consumer task"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
            logger.info("Copilot Kafka Consumer stopped safely.")

    async def _consume_loop(self):
        """Main event ingestion loop feeding the in-memory batch buffer"""
        logger.info("Starting Copilot event ingestion loop...")
        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                try:
                    topic = msg.topic
                    payload = msg.value
                    logger.debug(f"Received Kafka event on topic '{topic}': {payload}")

                    tenant_id = payload.get("metadata", {}).get("tenant_slug") or payload.get("tenant_id") or "store_tech"
                    
                    # Transform event into ClickHouse table records
                    if topic in ["product.created", "product.updated"]:
                        record = {
                            "id": int(payload.get("product_id") or payload.get("id", 0)),
                            "tenant_id": str(tenant_id),
                            "name": str(payload.get("name", "")),
                            "category": str(payload.get("category", "Electronics")),
                            "price": float(payload.get("price", 0.0)),
                            "stock": int(payload.get("stock", 0)),
                            "store_id": int(payload.get("store_id", 1))
                        }
                        await micro_batcher.enqueue("products_analytics", record, on_commit_callback=self._make_commit_callback(msg))

                    elif topic in ["order.created", "order.confirmed"]:
                        order_id = str(payload.get("order_id") or payload.get("id", "0"))
                        order_record = {
                            "id": order_id,
                            "tenant_id": str(tenant_id),
                            "user_id": int(payload.get("user_id", 1)),
                            "total_amount": float(payload.get("total_amount", 0.0)),
                            "status": "CONFIRMED" if topic == "order.confirmed" else "PENDING"
                        }
                        await micro_batcher.enqueue("orders_analytics", order_record, on_commit_callback=self._make_commit_callback(msg))

                        # Ingest order items if present
                        items = payload.get("items", [])
                        for item in items:
                            item_record = {
                                "id": str(item.get("id", order_id)),
                                "order_id": order_id,
                                "tenant_id": str(tenant_id),
                                "product_id": int(item.get("product_id", 0)),
                                "product_name": str(item.get("product_name", item.get("name", "Product"))),
                                "category": str(item.get("category", "Electronics")),
                                "unit_price": float(item.get("unit_price", item.get("price", 0.0))),
                                "quantity": int(item.get("quantity", 1))
                            }
                            await micro_batcher.enqueue("order_items_analytics", item_record)

                    elif topic in ["payment.succeeded", "payment.failed"]:
                        payment_record = {
                            "id": str(payload.get("payment_id") or payload.get("id", "0")),
                            "order_id": str(payload.get("order_id", "0")),
                            "tenant_id": str(tenant_id),
                            "amount": float(payload.get("amount", 0.0)),
                            "status": "SUCCEEDED" if topic == "payment.succeeded" else "FAILED",
                            "payment_method": str(payload.get("payment_method", "CARD")),
                            "transaction_id": str(payload.get("transaction_id", ""))
                        }
                        await micro_batcher.enqueue("payments_analytics", payment_record, on_commit_callback=self._make_commit_callback(msg))

                except Exception as msg_err:
                    logger.error(f"Error processing Kafka message from '{topic}': {msg_err}", exc_info=True)

        except asyncio.CancelledError:
            pass
        except Exception as loop_err:
            logger.error(f"Copilot consume loop encountered error: {loop_err}", exc_info=True)

    def _make_commit_callback(self, msg):
        """Creates a closure that commits the message offset to Kafka once ClickHouse persists the batch"""
        async def _commit():
            if self.consumer:
                await self.consumer.commit()
        return _commit
