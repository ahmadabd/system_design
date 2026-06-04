import logging
from shared.common.messaging import KafkaManager
from shared.common.idempotency import check_and_register_event
from src.infrastructure.db_setup import db
from src.adapter.repository import SQLAlchemyOrderRepository
from src.adapter.messaging_pub import OrderMessagingPublisher
from src.application.order_service import OrderApplicationService
from src.application.commands import ConfirmOrderCommand, CancelOrderCommand

logger = logging.getLogger("OrderMessagingSubscriber")

class OrderMessagingSubscriber:
    """Inbound Messaging Adapter for Order Service (Hexagonal Adapter)"""
    def __init__(self, mq_manager: KafkaManager):
        self.mq_manager = mq_manager

    async def start_listening(self) -> None:
        """Register consumers for inventory reserved, inventory failed, and payment succeeded/failed triggers"""
        logger.info("Registering Kafka listener for 'inventory.reserved' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="order_service_group",
            routing_key="inventory.reserved",
            callback=self._handle_inventory_reserved
        )

        logger.info("Registering Kafka listener for 'inventory.failed' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="order_service_group",
            routing_key="inventory.failed",
            callback=self._handle_inventory_failed
        )

        logger.info("Registering Kafka listener for 'payment.succeeded' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="order_service_group",
            routing_key="payment.succeeded",
            callback=self._handle_payment_succeeded
        )

        logger.info("Registering Kafka listener for 'payment.failed' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="order_service_group",
            routing_key="payment.failed",
            callback=self._handle_payment_failed
        )

    async def _handle_inventory_reserved(self, event_data: dict) -> None:
        """Process InventoryReserved event (Logs reservation and waits for payment status)"""
        order_id = event_data.get("order_id")
        event_id = event_data.get("metadata", {}).get("event_id", f"reserved-fallback-{order_id}")
        logger.info(f"SAGA PROGRESS: Received InventoryReserved event (ID: {event_id}): Order={order_id}. Waiting for payment confirmation.")

    async def _handle_inventory_failed(self, event_data: dict) -> None:
        """Process InventoryFailed event and cancel the order"""
        order_id = event_data.get("order_id")
        event_id = event_data.get("metadata", {}).get("event_id", f"failed-fallback-{order_id}")
        reason = event_data.get("reason", "Out of stock / catalog validation failed")
        logger.info(f"Received InventoryFailed event (ID: {event_id}): Order={order_id}, Reason={reason}. Moving status to CANCELLED.")

        if not order_id:
            logger.error("Missing order_id in InventoryFailed payload. Skipping.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate 'inventory.failed' event detected (ID: {event_id}). "
                        f"Discarding event to ensure idempotency."
                    )
                    return

                # 2. Proceed with domain command execution
                repo = SQLAlchemyOrderRepository(session)
                publisher = OrderMessagingPublisher(session)
                service = OrderApplicationService(repo, publisher)

                command = CancelOrderCommand(order_id=order_id, reason=reason)
                await service.cancel_order(command)
                await session.commit()
                logger.info(f"Cancelled Order {order_id} successfully under event ID {event_id}")
            except Exception as e:
                logger.error(f"Error handling stock reserved failure callback for order {order_id}: {e}", exc_info=True)
                await session.rollback()

    async def _handle_payment_succeeded(self, event_data: dict) -> None:
        """Process PaymentSucceeded event and confirm the order"""
        order_id = event_data.get("order_id")
        payment_id = event_data.get("payment_id")
        event_id = event_data.get("metadata", {}).get("event_id", f"pay-success-fallback-{order_id}")
        logger.info(f"Received PaymentSucceeded event (ID: {event_id}): Order={order_id}, Payment={payment_id}. Confirming order.")

        if not order_id:
            logger.error("Missing order_id in PaymentSucceeded payload. Skipping.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate 'payment.succeeded' event detected (ID: {event_id}). "
                        f"Discarding event to ensure idempotency."
                    )
                    return

                # 2. Proceed with domain command execution
                repo = SQLAlchemyOrderRepository(session)
                publisher = OrderMessagingPublisher(session)
                service = OrderApplicationService(repo, publisher)

                command = ConfirmOrderCommand(order_id=order_id)
                await service.confirm_order(command)
                await session.commit()
                logger.info(f"Confirmed Order {order_id} successfully under event ID {event_id}")
            except Exception as e:
                logger.error(f"Error handling payment succeeded callback for order {order_id}: {e}", exc_info=True)
                await session.rollback()

    async def _handle_payment_failed(self, event_data: dict) -> None:
        """Process PaymentFailed event and cancel the order (compensating saga transaction)"""
        order_id = event_data.get("order_id")
        payment_id = event_data.get("payment_id")
        event_id = event_data.get("metadata", {}).get("event_id", f"pay-fail-fallback-{order_id}")
        reason = event_data.get("reason", "Payment processor rejected transaction")
        logger.info(f"Received PaymentFailed event (ID: {event_id}): Order={order_id}, Payment={payment_id}. Reason: {reason}. Cancelling order.")

        if not order_id:
            logger.error("Missing order_id in PaymentFailed payload. Skipping.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate 'payment.failed' event detected (ID: {event_id}). "
                        f"Discarding event to ensure idempotency."
                    )
                    return

                # 2. Proceed with domain command execution
                repo = SQLAlchemyOrderRepository(session)
                publisher = OrderMessagingPublisher(session)
                service = OrderApplicationService(repo, publisher)

                command = CancelOrderCommand(order_id=order_id, reason=reason)
                await service.cancel_order(command)
                await session.commit()
                logger.info(f"Cancelled Order {order_id} successfully under event ID {event_id}")
            except Exception as e:
                logger.error(f"Error handling payment failed callback for order {order_id}: {e}", exc_info=True)
                await session.rollback()
