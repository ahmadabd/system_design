import logging
from sqlalchemy.ext.asyncio import AsyncSession
from shared.contracts.events import InventoryReservedEvent, InventoryFailedEvent
from shared.common.outbox import save_to_outbox

logger = logging.getLogger("ProductMessagingPublisher")

class ProductMessagingPublisher:
    """Outbound Messaging Adapter for Product Service using Outbox Pattern"""
    def __init__(self, session: AsyncSession):
        self.session = session
        self.exchange_name = "ecommerce.events"

    async def publish_inventory_reserved(self, event: InventoryReservedEvent) -> None:
        """Queue an InventoryReservedEvent into the database outbox"""
        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "inventory.reserved"
        logger.info(f"Writing inventory reserved event to outbox for order_id: {event.order_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

    async def publish_inventory_failed(self, event: InventoryFailedEvent) -> None:
        """Queue an InventoryFailedEvent into the database outbox"""
        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "inventory.failed"
        logger.info(f"Writing inventory failed event to outbox for order_id: {event.order_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

