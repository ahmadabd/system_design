import json
import logging
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from src.infrastructure.config import settings

logger = logging.getLogger("DisputeMessagingPub")


class DisputeMessagingPublisher:
    """Outbound Kafka integration event publisher for dispute events"""
    def __init__(self, bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[AIOKafkaProducer] = None
        self._connected = False

    async def start(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await self.producer.start()
            self._connected = True
            logger.info(f"Dispute Kafka publisher connected to {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"Could not connect Dispute Kafka publisher ({e}). Operating in memory mode.")

    async def stop(self):
        if self.producer and self._connected:
            try:
                await self.producer.stop()
                self._connected = False
                logger.info("Dispute Kafka publisher stopped.")
            except Exception as e:
                logger.warning(f"Error stopping Dispute Kafka publisher: {e}")

    async def publish_dispute_resolved(self, event_payload: Dict[str, Any]):
        """Emits dispute.resolved event to Kafka topic"""
        if not self.producer or not self._connected:
            logger.debug(f"Kafka unavailable. Skipping dispute.resolved event dispatch: {event_payload.get('claim_id')}")
            return
        try:
            topic = "dispute.resolved"
            await self.producer.send_and_wait(topic, event_payload)
            logger.info(f"Dispatched '{topic}' integration event for claim #{event_payload.get('claim_id')}")
        except Exception as e:
            logger.error(f"Failed to publish dispute.resolved event: {e}")


dispute_messaging_pub = DisputeMessagingPublisher()
