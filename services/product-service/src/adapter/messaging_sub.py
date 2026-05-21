import logging
from shared.common.messaging import KafkaManager
from shared.common.idempotency import check_and_register_event
from src.infrastructure.db_setup import db
from src.adapter.repository import SQLAlchemyProductRepository
from src.adapter.messaging_pub import ProductMessagingPublisher
from src.application.product_service import ProductApplicationService
from src.application.commands import ReserveInventoryCommand

logger = logging.getLogger("ProductMessagingSubscriber")

class ProductMessagingSubscriber:
    """Inbound Messaging Adapter for Product Service (Hexagonal Adapter)"""
    def __init__(self, mq_manager: KafkaManager):
        self.mq_manager = mq_manager

    async def start_listening(self) -> None:
        """Register the consumer queue and bind to order.created routing keys"""
        logger.info("Registering Kafka listener for 'order.created' integration events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="product_service_group",
            routing_key="order.created",
            callback=self._handle_order_created
        )

    async def _handle_order_created(self, event_data: dict) -> None:
        """Process OrderCreated event and trigger application use case"""
        order_id = event_data.get("order_id")
        product_id = event_data.get("product_id")
        quantity = event_data.get("quantity")
        event_id = event_data.get("metadata", {}).get("event_id", f"order-fallback-{order_id}")

        logger.info(f"Received OrderCreated event (ID: {event_id}) from broker: Order={order_id}, Product={product_id}, Qty={quantity}")

        if not order_id or not product_id or not quantity:
            logger.error("Invalid OrderCreated event structure. Skipping processing.")
            return

        # Background transactions require manual session extraction to guarantee execution safety
        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate 'order.created' event detected (ID: {event_id}). "
                        f"Discarding event to ensure idempotency."
                    )
                    return

                # 2. Proceed with domain command execution
                repo = SQLAlchemyProductRepository(session)
                publisher = ProductMessagingPublisher(self.mq_manager)
                service = ProductApplicationService(repo, publisher)

                command = ReserveInventoryCommand(
                    order_id=order_id,
                    product_id=product_id,
                    quantity=quantity
                )

                # Process inventory reservation
                await service.reserve_stock(command)
                
                # Commit any inventory decreases
                await session.commit()
                logger.info(f"Successfully processed inventory transaction for Order: {order_id} under event ID {event_id}")
            except Exception as e:
                logger.error(f"Error executing asynchronous stock reservation for order {order_id}: {e}", exc_info=True)
                await session.rollback()
