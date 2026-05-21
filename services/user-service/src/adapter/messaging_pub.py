import logging
from shared.common.messaging import KafkaManager
from shared.contracts.events import UserRegisteredEvent

logger = logging.getLogger("UserMessagingPublisher")

class UserMessagingPublisher:
    """Outbound Messaging Adapter for User Service"""
    def __init__(self, mq_manager: KafkaManager):
        self.mq_manager = mq_manager
        self.exchange_name = "ecommerce.events"

    async def publish_user_registered(self, event: UserRegisteredEvent) -> None:
        """Publish a UserRegisteredEvent to the Kafka topic"""
        # Convert Pydantic event structure to dict, transforming datetime to string
        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "user.registered"
        logger.info(f"Publishing user registered event for user_id: {event.user_id}")
        await self.mq_manager.publish(
            exchange_name=self.exchange_name,
            routing_key=routing_key,
            event_data=event_dict
        )
