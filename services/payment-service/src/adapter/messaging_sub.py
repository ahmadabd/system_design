import logging
from shared.common.messaging import KafkaManager
from shared.common.idempotency import check_and_register_event
from src.infrastructure.db_setup import db
from src.adapter.repository import SQLAlchemyPaymentRepository
from src.adapter.messaging_pub import PaymentMessagingPublisher
from src.application.payment_service import PaymentApplicationService
from src.application.commands import ProcessPaymentCommand

logger = logging.getLogger("PaymentMessagingSubscriber")

class PaymentMessagingSubscriber:
    """Inbound Hexagonal Messaging Adapter for Payment Service"""
    def __init__(self, mq_manager: KafkaManager):
        self.mq_manager = mq_manager

    async def start_listening(self) -> None:
        """Register consumer queue binding to integration topics"""
        logger.info("Registering Kafka listener for 'order.created' integration events for materialization...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="payment_service_group",
            routing_key="order.created",
            callback=self._handle_order_created
        )

        logger.info("Registering Kafka listener for 'inventory.reserved' integration events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="payment_service_group",
            routing_key="inventory.reserved",
            callback=self._handle_inventory_reserved
        )

    async def _handle_order_created(self, event_data: dict) -> None:
        """Process OrderCreated event and save materialized details locally (CQRS read model)"""
        order_id = event_data.get("order_id")
        total_price = event_data.get("total_price")
        quantity = event_data.get("quantity", 1)
        event_id = event_data.get("metadata", {}).get("event_id", f"order-created-fallback-{order_id}")

        logger.info(f"Received OrderCreated event for materialization (ID: {event_id}): Order={order_id}, Price={total_price}, Qty={quantity}")

        if not order_id or total_price is None:
            logger.error("Invalid OrderCreated event structure for materialization. Skipping processing.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                # For materialization, we prepend 'mat-' to the event_id
                mat_event_id = f"mat-{event_id}"
                is_duplicate = await check_and_register_event(session, mat_event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate 'order.created' materialization event detected (ID: {event_id}). "
                        f"Skipping to ensure idempotency."
                    )
                    return

                # 2. Persist details locally using SQLAlchemy Repository
                repo = SQLAlchemyPaymentRepository(session)
                logger.info(f"CQRS Materialized State: Persisting order locally in payment-service for Order={order_id}: Price={total_price}, Qty={quantity}")
                await repo.save_materialized_order(
                    order_id=order_id,
                    total_price=total_price,
                    quantity=quantity
                )
                
                await session.commit()
                logger.info(f"Successfully materialized order details for Order: {order_id} under event ID {event_id}")
            except Exception as e:
                logger.error(f"Error materializing order details for order {order_id}: {e}", exc_info=True)
                await session.rollback()

    async def _handle_inventory_reserved(self, event_data: dict) -> None:
        """Process stock reservation events and coordinate customer payments"""
        order_id = event_data.get("order_id")
        event_id = event_data.get("metadata", {}).get("event_id", f"inventory-reserved-fallback-{order_id}")

        logger.info(f"Received InventoryReserved event (ID: {event_id}): Order={order_id}. Initializing payment processing.")

        if not order_id:
            logger.error("Missing order_id in InventoryReserved payload. Skipping processing.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate 'inventory.reserved' event detected (ID: {event_id}). "
                        f"Discarding event to ensure idempotency."
                    )
                    return

                # 2. Proceed with domain command execution
                repo = SQLAlchemyPaymentRepository(session)
                publisher = PaymentMessagingPublisher(self.mq_manager)
                service = PaymentApplicationService(repo, publisher)

                command = ProcessPaymentCommand(order_id=order_id, event_id=event_id)
                
                # Execute payment processing
                await service.process_payment(command)
                
                # Commit database writes
                await session.commit()
                logger.info(f"Successfully processed payment transaction for Order: {order_id} under event ID {event_id}")
            except Exception as e:
                logger.error(f"Error executing asynchronous payment processing for order {order_id}: {e}", exc_info=True)
                await session.rollback()
