import logging
from sqlalchemy.ext.asyncio import AsyncSession
from shared.contracts.events import OrderCreatedEvent
from shared.common.outbox import save_to_outbox

logger = logging.getLogger("OrderMessagingPublisher")

class OrderMessagingPublisher:
    """Outbound Messaging Adapter for Order Service using Outbox Pattern"""
    def __init__(self, session: AsyncSession):
        self.session = session
        self.exchange_name = "ecommerce.events"

    async def publish_order_created(self, event: OrderCreatedEvent) -> None:
        """Queue an OrderCreatedEvent into the database outbox"""
        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "order.created"
        logger.info(f"Writing order created event to outbox for order_id: {event.order_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

