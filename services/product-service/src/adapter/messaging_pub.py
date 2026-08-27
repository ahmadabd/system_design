import logging
from sqlalchemy.ext.asyncio import AsyncSession
from shared.contracts.events import (
    InventoryReservedEvent,
    InventoryFailedEvent,
    ProductCreatedEvent,
    ProductUpdatedEvent,
    ProductDeletedEvent
)
from shared.common.outbox import save_to_outbox

logger = logging.getLogger("ProductMessagingPublisher")

class ProductMessagingPublisher:
    """Outbound Messaging Adapter for Product Service using Outbox Pattern"""
    def __init__(self, session: AsyncSession):
        self.session = session
        self.exchange_name = "ecommerce.events"

    async def publish_product_created(self, event: ProductCreatedEvent) -> None:
        """Queue a ProductCreatedEvent into the database outbox"""
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        event.metadata.tenant_slug = tenant.slug if tenant else None

        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "product.created"
        logger.info(f"Writing product created event to outbox for product_id: {event.product_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

    async def publish_product_updated(self, event: ProductUpdatedEvent) -> None:
        """Queue a ProductUpdatedEvent into the database outbox"""
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        event.metadata.tenant_slug = tenant.slug if tenant else None

        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "product.updated"
        logger.info(f"Writing product updated event to outbox for product_id: {event.product_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

    async def publish_product_deleted(self, event: ProductDeletedEvent) -> None:
        """Queue a ProductDeletedEvent into the database outbox"""
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        event.metadata.tenant_slug = tenant.slug if tenant else None

        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "product.deleted"
        logger.info(f"Writing product deleted event to outbox for product_id: {event.product_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

    async def publish_inventory_reserved(self, event: InventoryReservedEvent) -> None:
        """Queue an InventoryReservedEvent into the database outbox"""
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        event.metadata.tenant_slug = tenant.slug if tenant else None

        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "inventory.reserved"
        logger.info(f"Writing inventory reserved event to outbox for order_id: {event.order_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

    async def publish_inventory_failed(self, event: InventoryFailedEvent) -> None:
        """Queue an InventoryFailedEvent into the database outbox"""
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        event.metadata.tenant_slug = tenant.slug if tenant else None

        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "inventory.failed"
        logger.info(f"Writing inventory failed event to outbox for order_id: {event.order_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

    async def publish_store_registered(self, event) -> None:
        """Queue a StoreRegisteredEvent into the database outbox"""
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        event.metadata.tenant_slug = tenant.slug if tenant else None

        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "store.registered"
        logger.info(f"Writing store registered event to outbox for store_id: {event.store_id}")
        await save_to_outbox(self.session, routing_key, event_dict)



