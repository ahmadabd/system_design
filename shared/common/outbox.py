import asyncio
import logging
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from shared.common.database import Database, OutboxMessage
from shared.common.messaging import KafkaManager

logger = logging.getLogger("OutboxPublisher")

async def save_to_outbox(session: AsyncSession, topic: str, payload: dict) -> None:
    """Helper to save a message payload to the outbox database table.
    Must be called within an active transaction session.
    """
    msg = OutboxMessage(
        topic=topic,
        payload=payload,
        processed=False
    )
    session.add(msg)
    logger.debug(f"Event queued to database outbox: topic='{topic}'")

class OutboxPublisher:
    """Background task runner to poll the local outbox_messages table and publish to Kafka"""
    def __init__(self, db: Database, mq_manager: KafkaManager, poll_interval: float = 0.2):
        self.db = db
        self.mq_manager = mq_manager
        self.poll_interval = poll_interval
        self._is_running = False
        self._task = None

    def start(self) -> None:
        if not self._is_running:
            self._is_running = True
            self._task = asyncio.create_task(self._publish_loop())
            logger.info("Outbox publisher background task started.")

    async def stop(self) -> None:
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            logger.info("Outbox publisher background task stopped.")

    async def _publish_loop(self) -> None:
        # Give services a small warming-up buffer before reading outbox
        await asyncio.sleep(2.0)
        while self._is_running:
            try:
                await self._publish_pending()
            except Exception as e:
                logger.error(f"Error in outbox publishing loop: {e}", exc_info=True)
            await asyncio.sleep(self.poll_interval)

    async def _publish_pending(self) -> None:
        # Ensure we are connected to Kafka
        if not self.mq_manager.producer:
            try:
                await self.mq_manager.connect()
            except Exception as conn_err:
                logger.warning(f"Outbox publisher cannot connect to Kafka: {conn_err}. Will retry...")
                return

        async with self.db._session_maker() as session:
            try:
                # Query oldest unprocessed messages
                stmt = (
                    select(OutboxMessage)
                    .where(OutboxMessage.processed == False)
                    .order_by(OutboxMessage.id.asc())
                    .limit(20)
                )
                result = await session.execute(stmt)
                messages = result.scalars().all()

                if not messages:
                    return

                logger.info(f"Outbox publisher: processing {len(messages)} pending outbox events.")

                for msg in messages:
                    try:
                        # Publish using KafkaManager with breaker protection
                        await self.mq_manager.publish(
                            exchange_name="ecommerce.events",
                            routing_key=msg.topic,
                            event_data=msg.payload
                        )
                        # Delete the message upon successful dispatch to keep outbox database lean
                        await session.delete(msg)
                    except Exception as pub_err:
                        logger.error(
                            f"Failed to publish outbox message ID {msg.id} (topic: '{msg.topic}'): {pub_err}"
                        )
                        # Stop processing this batch to preserve event order
                        break

                await session.commit()
            except Exception as loop_err:
                logger.error(f"Failed transaction step in outbox loop: {loop_err}", exc_info=True)
                await session.rollback()
