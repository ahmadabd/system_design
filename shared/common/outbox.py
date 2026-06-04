import asyncio
import logging
import os
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from shared.common.database import Database, OutboxMessage
from shared.common.messaging import KafkaManager

try:
    from opentelemetry import trace
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    otel_available = True
except ImportError:
    otel_available = False

try:
    from opentelemetry.instrumentation.utils import suppress_instrumentation
except ImportError:
    suppress_instrumentation = None

try:
    from prometheus_client import Gauge, Counter
    service_name = os.getenv("SERVICE_NAME", "unknown-service")
    outbox_backlog_size = Gauge(
        "outbox_backlog_size",
        "Number of pending messages in the database outbox",
        ["service_name"]
    )
    outbox_published_total = Counter(
        "outbox_published_total",
        "Total number of messages successfully published from the outbox",
        ["service_name", "topic"]
    )
    outbox_errors_total = Counter(
        "outbox_errors_total",
        "Total number of errors encountered during outbox publishing",
        ["service_name", "error_type"]
    )
except ImportError:
    outbox_backlog_size = None
    outbox_published_total = None
    outbox_errors_total = None

logger = logging.getLogger("OutboxPublisher")


async def save_to_outbox(session: AsyncSession, topic: str, payload: dict) -> None:
    """Helper to save a message payload to the outbox database table.
    Must be called within an active transaction session.
    Automatically propagates OpenTelemetry trace context.
    """
    if otel_available:
        try:
            trace_headers = {}
            TraceContextTextMapPropagator().inject(trace_headers)
            if trace_headers:
                if "metadata" not in payload:
                    payload["metadata"] = {}
                payload["metadata"]["trace_headers"] = trace_headers
        except Exception as trace_err:
            logger.warning(f"Failed to inject OTel context in save_to_outbox: {trace_err}")

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
                # Update backlog count metric inside suppression context
                if suppress_instrumentation:
                    with suppress_instrumentation():
                        count_stmt = select(func.count()).select_from(OutboxMessage).where(OutboxMessage.processed == False)
                        count_result = await session.execute(count_stmt)
                        total_pending = count_result.scalar() or 0
                else:
                    count_stmt = select(func.count()).select_from(OutboxMessage).where(OutboxMessage.processed == False)
                    count_result = await session.execute(count_stmt)
                    total_pending = count_result.scalar() or 0

                if outbox_backlog_size:
                    outbox_backlog_size.labels(service_name=service_name).set(total_pending)

                # Query oldest unprocessed messages inside suppress block if available
                if suppress_instrumentation:
                    with suppress_instrumentation():
                        stmt = (
                            select(OutboxMessage)
                            .where(OutboxMessage.processed == False)
                            .order_by(OutboxMessage.id.asc())
                            .limit(20)
                        )
                        result = await session.execute(stmt)
                        messages = result.scalars().all()
                else:
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
                    # Attempt to extract parents trace context
                    parent_context = None
                    if otel_available:
                        try:
                            trace_headers = msg.payload.get("metadata", {}).get("trace_headers")
                            if trace_headers:
                                parent_context = TraceContextTextMapPropagator().extract(carrier=trace_headers)
                        except Exception as trace_err:
                            logger.warning(f"Failed to extract OTel context for outbox msg {msg.id}: {trace_err}")

                    try:
                        # Publish Kafka event (outside of DB suppression context to allow tracing)
                        if otel_available and parent_context:
                            tracer = trace.get_tracer("outbox-publisher")
                            with tracer.start_as_current_span(
                                name=f"outbox.publish {msg.topic}",
                                context=parent_context,
                                kind=trace.SpanKind.PRODUCER
                            ):
                                await self.mq_manager.publish(
                                    exchange_name="ecommerce.events",
                                    routing_key=msg.topic,
                                    event_data=msg.payload
                                )
                        else:
                            await self.mq_manager.publish(
                                exchange_name="ecommerce.events",
                                routing_key=msg.topic,
                                event_data=msg.payload
                            )

                        # Increment successfully published counter
                        if outbox_published_total:
                            outbox_published_total.labels(service_name=service_name, topic=msg.topic).inc()

                        # Delete the message (inside suppress block to avoid tracing cleanup)
                        if suppress_instrumentation:
                            with suppress_instrumentation():
                                await session.delete(msg)
                        else:
                            await session.delete(msg)

                    except Exception as pub_err:
                        logger.error(
                            f"Failed to publish outbox message ID {msg.id} (topic: '{msg.topic}'): {pub_err}"
                        )
                        # Increment publish errors counter
                        if outbox_errors_total:
                            outbox_errors_total.labels(service_name=service_name, error_type=type(pub_err).__name__).inc()
                        # Stop processing this batch to preserve event order
                        break

                # Commit operations inside suppress block
                if suppress_instrumentation:
                    with suppress_instrumentation():
                        await session.commit()
                else:
                    await session.commit()

            except Exception as loop_err:
                logger.error(f"Failed transaction step in outbox loop: {loop_err}", exc_info=True)
                if suppress_instrumentation:
                    with suppress_instrumentation():
                        await session.rollback()
                else:
                    await session.rollback()

