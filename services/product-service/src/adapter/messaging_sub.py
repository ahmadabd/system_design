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

        logger.info("Registering Kafka listener for 'payment.failed' integration events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="product_service_group",
            routing_key="payment.failed",
            callback=self._handle_payment_failed
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

    async def _handle_payment_failed(self, event_data: dict) -> None:
        """Process PaymentFailed event (Saga compensation) and release stock"""
        order_id = event_data.get("order_id")
        event_id = event_data.get("metadata", {}).get("event_id", f"payment-fail-fallback-{order_id}")
        reason = event_data.get("reason", "Payment failed")
        
        logger.info(f"SAGA COMPENSATION: Received PaymentFailed event (ID: {event_id}) for Order={order_id}. Reason: {reason}.")

        if not order_id:
            logger.error("Missing order_id in PaymentFailed payload. Skipping compensation.")
            return

        from shared.common.http_client import ResilientHTTPClient
        from src.infrastructure.config import settings

        # 1. Fetch order details from order-service via Resilient HTTP Client to determine product and quantity
        client = ResilientHTTPClient(timeout=5.0)
        try:
            url = f"{settings.ORDER_SERVICE_URL}/{order_id}"
            logger.info(f"Querying order-service for compensation: GET {url}")
            response = await client.get(url)
            response.raise_for_status()
            order_data = response.json()
        except Exception as http_err:
            logger.error(f"Failed to query order-service for Order {order_id} details: {http_err}. Compensation failed.")
            return
        finally:
            await client.close()

        product_id = order_data.get("product_id")
        quantity = order_data.get("quantity")

        if not product_id or not quantity:
            logger.error(f"Incomplete order data retrieved for Order {order_id}: {order_data}. Skipping compensation.")
            return

        async with db._session_maker() as session:
            try:
                # Deduplication Check (Inbox Pattern)
                comp_event_id = f"comp-{event_id}"
                is_duplicate = await check_and_register_event(session, comp_event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate payment.failed compensation event detected (ID: {event_id}). "
                        f"Skipping to ensure idempotency."
                    )
                    return

                repo = SQLAlchemyProductRepository(session)
                product = await repo.find_by_id(product_id)
                if not product:
                    logger.error(f"Product {product_id} not found during compensation for Order {order_id}.")
                    return

                # Release the reserved stock
                logger.info(f"Releasing stock for Product {product_id}: Qty={quantity} for Order={order_id}")
                product.release_stock(quantity)
                await repo.save(product)
                await session.commit()
                logger.info(f"Successfully compensated inventory stock for Order: {order_id} (released quantity {quantity})")
            except Exception as e:
                logger.error(f"Error executing stock release compensation for order {order_id}: {e}", exc_info=True)
                await session.rollback()
