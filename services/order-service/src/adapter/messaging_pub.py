import logging
from shared.common.messaging import KafkaManager
from shared.contracts.events import OrderCreatedEvent

logger = logging.getLogger("OrderMessagingPublisher")

class OrderMessagingPublisher:
    """Outbound Messaging Adapter for Order Service"""
    def __init__(self, mq_manager: KafkaManager):
        self.mq_manager = mq_manager
        self.exchange_name = "ecommerce.events"

    async def publish_order_created(self, event: OrderCreatedEvent) -> None:
        """Publish an OrderCreatedEvent to the RabbitMQ Exchange"""
        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "order.created"
        logger.info(f"Publishing order created event for order_id: {event.order_id}")
        await self.mq_manager.publish(
            exchange_name=self.exchange_name,
            routing_key=routing_key,
            event_data=event_dict
        )
