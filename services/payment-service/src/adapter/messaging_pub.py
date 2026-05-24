import logging
from shared.common.messaging import KafkaManager
from shared.contracts.events import PaymentSucceededEvent, PaymentFailedEvent

logger = logging.getLogger("PaymentMessagingPublisher")

class PaymentMessagingPublisher:
    """Outbound hexagonal messaging adapter to dispatch payment integration events"""
    def __init__(self, mq_manager: KafkaManager):
        self.mq_manager = mq_manager

    async def publish_payment_succeeded(self, event: PaymentSucceededEvent) -> None:
        """Publish a success payment event onto payment.succeeded Kafka routing key"""
        logger.info(f"Dispatching PaymentSucceeded event for Order {event.order_id} (Payment: {event.payment_id})")
        payload = event.model_dump()
        # Convert timestamp to ISO string for JSON serialization
        if "timestamp" in payload.get("metadata", {}):
            payload["metadata"]["timestamp"] = payload["metadata"]["timestamp"].isoformat()
        
        await self.mq_manager.publish(
            exchange_name="ecommerce.events",
            routing_key="payment.succeeded",
            event_data=payload
        )

    async def publish_payment_failed(self, event: PaymentFailedEvent) -> None:
        """Publish a failure payment event onto payment.failed Kafka routing key"""
        logger.info(f"Dispatching PaymentFailed event for Order {event.order_id} (Reason: {event.reason})")
        payload = event.model_dump()
        # Convert timestamp to ISO string for JSON serialization
        if "timestamp" in payload.get("metadata", {}):
            payload["metadata"]["timestamp"] = payload["metadata"]["timestamp"].isoformat()
        
        await self.mq_manager.publish(
            exchange_name="ecommerce.events",
            routing_key="payment.failed",
            event_data=payload
        )
