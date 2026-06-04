import logging
from shared.common.messaging import KafkaManager
from shared.common.idempotency import check_and_register_event
from src.infrastructure.db_setup import db
from src.adapter.db_models import ReportingProfileDB, ReportingOrderDB, ReportingPaymentDB
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import update

logger = logging.getLogger("ReportingMessagingSubscriber")

class ReportingMessagingSubscriber:
    """Inbound hexagonal messaging adapter to consume and materialize platform events (CQRS)"""
    def __init__(self, mq_manager: KafkaManager):
        self.mq_manager = mq_manager

    async def start_listening(self) -> None:
        """Register consumer callbacks on Kafka event topics"""
        logger.info("Registering Kafka listener for 'user.registered' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="reporting_service_group",
            routing_key="user.registered",
            callback=self._handle_user_registered
        )

        logger.info("Registering Kafka listener for 'order.created' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="reporting_service_group",
            routing_key="order.created",
            callback=self._handle_order_created
        )

        logger.info("Registering Kafka listener for 'payment.succeeded' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="reporting_service_group",
            routing_key="payment.succeeded",
            callback=self._handle_payment_succeeded
        )

        logger.info("Registering Kafka listener for 'payment.failed' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="reporting_service_group",
            routing_key="payment.failed",
            callback=self._handle_payment_failed
        )

        logger.info("Registering Kafka listener for 'inventory.failed' events...")
        await self.mq_manager.subscribe(
            exchange_name="ecommerce.events",
            queue_name="reporting_service_group",
            routing_key="inventory.failed",
            callback=self._handle_inventory_failed
        )

    async def _handle_user_registered(self, event_data: dict) -> None:
        """Store basic user profile fields locally upon registration"""
        user_id = event_data.get("user_id")
        username = event_data.get("username")
        email = event_data.get("email")
        event_id = event_data.get("metadata", {}).get("event_id", f"user-registered-{user_id}")

        logger.info(f"CQRS Materializer: Processing UserRegistered event (ID: {event_id}) for User={user_id}")

        if not user_id:
            logger.error("Missing user_id in UserRegistered payload. Skipping.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(f"Inbox Pattern: Duplicate 'user.registered' event (ID: {event_id}). Discarding.")
                    return

                # 2. Materialize profile data (Idempotent Upsert)
                stmt = insert(ReportingProfileDB).values(
                    user_id=user_id,
                    username=username,
                    email=email
                ).on_conflict_do_nothing()
                await session.execute(stmt)
                await session.commit()
                logger.info(f"CQRS Materializer: Profile for User={user_id} successfully materialized.")
            except Exception as e:
                logger.error(f"Error materializing UserRegistered event: {e}", exc_info=True)
                await session.rollback()
                raise e

    async def _handle_order_created(self, event_data: dict) -> None:
        """Store new customer order details locally in a PENDING state, or matching the already materialized status if out-of-order"""
        order_id = event_data.get("order_id")
        user_id = event_data.get("user_id")
        product_id = event_data.get("product_id")
        quantity = event_data.get("quantity")
        total_price = event_data.get("total_price")
        event_id = event_data.get("metadata", {}).get("event_id", f"order-created-{order_id}")

        logger.info(f"CQRS Materializer: Processing OrderCreated event (ID: {event_id}) for Order={order_id}")

        if not order_id:
            logger.error("Missing order_id in OrderCreated payload. Skipping.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(f"Inbox Pattern: Duplicate 'order.created' event (ID: {event_id}). Discarding.")
                    return

                # 2. Check if a payment has already materialized for this order (handles out-of-order events)
                from sqlalchemy import select
                pay_stmt = select(ReportingPaymentDB).where(ReportingPaymentDB.order_id == order_id)
                pay_result = await session.execute(pay_stmt)
                payment = pay_result.scalars().first()

                initial_status = "PENDING"
                if payment:
                    if "SUCCEEDED" in payment.status:
                        initial_status = "CONFIRMED"
                    elif "FAILED" in payment.status:
                        initial_status = "CANCELLED"

                # 3. Materialize order details (Idempotent Upsert)
                stmt = insert(ReportingOrderDB).values(
                    order_id=order_id,
                    user_id=user_id,
                    product_id=product_id,
                    quantity=quantity,
                    total_price=total_price,
                    status=initial_status
                ).on_conflict_do_nothing()
                await session.execute(stmt)
                await session.commit()
                logger.info(f"CQRS Materializer: Order={order_id} materialized as {initial_status}.")
            except Exception as e:
                logger.error(f"Error materializing OrderCreated event: {e}", exc_info=True)
                await session.rollback()
                raise e

    async def _handle_payment_succeeded(self, event_data: dict) -> None:
        """Store payment transaction record and transition order status to CONFIRMED"""
        order_id = event_data.get("order_id")
        payment_id = event_data.get("payment_id")
        amount = event_data.get("amount")
        event_id = event_data.get("metadata", {}).get("event_id", f"pay-succeeded-{order_id}")

        logger.info(f"CQRS Materializer: Processing PaymentSucceeded event (ID: {event_id}) for Order={order_id}")

        if not order_id:
            logger.error("Missing order_id in PaymentSucceeded payload. Skipping.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(f"Inbox Pattern: Duplicate 'payment.succeeded' event (ID: {event_id}). Discarding.")
                    return

                # 2. Materialize Payment Record
                stmt_pay = insert(ReportingPaymentDB).values(
                    payment_id=payment_id,
                    order_id=order_id,
                    amount=amount,
                    status="SUCCEEDED"
                ).on_conflict_do_nothing()
                await session.execute(stmt_pay)

                # 3. Transition order to CONFIRMED
                stmt_order = (
                    update(ReportingOrderDB)
                    .where(ReportingOrderDB.order_id == order_id)
                    .values(status="CONFIRMED")
                )
                await session.execute(stmt_order)
                await session.commit()
                logger.info(f"CQRS Materializer: Payment={payment_id} recorded. Order={order_id} status updated to CONFIRMED.")
            except Exception as e:
                logger.error(f"Error materializing PaymentSucceeded event: {e}", exc_info=True)
                await session.rollback()
                raise e

    async def _handle_payment_failed(self, event_data: dict) -> None:
        """Store payment failure transaction record and transition order status to CANCELLED"""
        order_id = event_data.get("order_id")
        payment_id = event_data.get("payment_id")
        amount = event_data.get("amount")
        reason = event_data.get("reason", "Unknown payment rejection reason")
        event_id = event_data.get("metadata", {}).get("event_id", f"pay-failed-{order_id}")

        logger.info(f"CQRS Materializer: Processing PaymentFailed event (ID: {event_id}) for Order={order_id}")

        if not order_id:
            logger.error("Missing order_id in PaymentFailed payload. Skipping.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(f"Inbox Pattern: Duplicate 'payment.failed' event (ID: {event_id}). Discarding.")
                    return

                # 2. Materialize Payment Failure Record
                stmt_pay = insert(ReportingPaymentDB).values(
                    payment_id=payment_id or f"failed-{order_id}",
                    order_id=order_id,
                    amount=amount or 0.0,
                    status=f"FAILED: {reason}"
                ).on_conflict_do_nothing()
                await session.execute(stmt_pay)

                # 3. Transition order to CANCELLED
                stmt_order = (
                    update(ReportingOrderDB)
                    .where(ReportingOrderDB.order_id == order_id)
                    .values(status="CANCELLED")
                )
                await session.execute(stmt_order)
                await session.commit()
                logger.info(f"CQRS Materializer: Payment failure recorded. Order={order_id} status updated to CANCELLED.")
            except Exception as e:
                logger.error(f"Error materializing PaymentFailed event: {e}", exc_info=True)
                await session.rollback()
                raise e

    async def _handle_inventory_failed(self, event_data: dict) -> None:
        """Transition order status to CANCELLED due to out-of-stock validation failures"""
        order_id = event_data.get("order_id")
        reason = event_data.get("reason", "Out of stock")
        event_id = event_data.get("metadata", {}).get("event_id", f"inv-failed-{order_id}")

        logger.info(f"CQRS Materializer: Processing InventoryFailed event (ID: {event_id}) for Order={order_id}")

        if not order_id:
            logger.error("Missing order_id in InventoryFailed payload. Skipping.")
            return

        async with db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(f"Inbox Pattern: Duplicate 'inventory.failed' event (ID: {event_id}). Discarding.")
                    return

                # 2. Transition order to CANCELLED
                stmt_order = (
                    update(ReportingOrderDB)
                    .where(ReportingOrderDB.order_id == order_id)
                    .values(status="CANCELLED")
                )
                await session.execute(stmt_order)
                await session.commit()
                logger.info(f"CQRS Materializer: Inventory failure recorded. Order={order_id} status updated to CANCELLED.")
            except Exception as e:
                logger.error(f"Error materializing InventoryFailed event: {e}", exc_info=True)
                await session.rollback()
                raise e
