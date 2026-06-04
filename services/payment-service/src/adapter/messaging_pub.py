import logging
from sqlalchemy.ext.asyncio import AsyncSession
from shared.contracts.events import PaymentSucceededEvent, PaymentFailedEvent
from shared.common.outbox import save_to_outbox

logger = logging.getLogger("PaymentMessagingPublisher")

class PaymentMessagingPublisher:
    """Outbound hexagonal messaging adapter to dispatch payment integration events using Outbox Pattern"""
    def __init__(self, session: AsyncSession):
        self.session = session

    async def publish_payment_succeeded(self, event: PaymentSucceededEvent) -> None:
        """Queue a PaymentSucceededEvent into the database outbox"""
        logger.info(f"Writing PaymentSucceeded event to outbox for Order {event.order_id} (Payment: {event.payment_id})")
        payload = event.model_dump()
        # Convert timestamp to ISO string for JSON serialization
        if "timestamp" in payload.get("metadata", {}):
            payload["metadata"]["timestamp"] = payload["metadata"]["timestamp"].isoformat()
        
        await save_to_outbox(self.session, "payment.succeeded", payload)

    async def publish_payment_failed(self, event: PaymentFailedEvent) -> None:
        """Queue a PaymentFailedEvent into the database outbox"""
        logger.info(f"Writing PaymentFailed event to outbox for Order {event.order_id} (Reason: {event.reason})")
        payload = event.model_dump()
        # Convert timestamp to ISO string for JSON serialization
        if "timestamp" in payload.get("metadata", {}):
            payload["metadata"]["timestamp"] = payload["metadata"]["timestamp"].isoformat()
        
        await save_to_outbox(self.session, "payment.failed", payload)

