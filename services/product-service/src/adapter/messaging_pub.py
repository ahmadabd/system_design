import logging
from shared.common.messaging import KafkaManager
from shared.contracts.events import InventoryReservedEvent, InventoryFailedEvent

logger = logging.getLogger("ProductMessagingPublisher")

class ProductMessagingPublisher:
    """Outbound Messaging Adapter for Product Service"""
    def __init__(self, mq_manager: KafkaManager):
        self.mq_manager = mq_manager
        self.exchange_name = "ecommerce.events"

    async def publish_inventory_reserved(self, event: InventoryReservedEvent) -> None:
        """Publish an InventoryReservedEvent to the Kafka topic"""
        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "inventory.reserved"
        logger.info(f"Publishing inventory reserved event for order_id: {event.order_id}")
        await self.mq_manager.publish(
            exchange_name=self.exchange_name,
            routing_key=routing_key,
            event_data=event_dict
        )

    async def publish_inventory_failed(self, event: InventoryFailedEvent) -> None:
        """Publish an InventoryFailedEvent to the Kafka topic"""
        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "inventory.failed"
        logger.info(f"Publishing inventory failed event for order_id: {event.order_id}")
        await self.mq_manager.publish(
            exchange_name=self.exchange_name,
            routing_key=routing_key,
            event_data=event_dict
        )
