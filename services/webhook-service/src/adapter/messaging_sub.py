import asyncio
import json
import logging
import datetime
from aiokafka import AIOKafkaConsumer, TopicPartition
from src.adapter.repository import SQLAlchemyWebhookRepository
from src.application.resilience import breaker_registry, RetriableWebhookError, NonRetriableWebhookError
from shared.common.idempotency import check_and_register_event

logger = logging.getLogger("WebhookMessagingSubscriber")

try:
    from prometheus_client import Counter, Histogram
    webhook_delivery_attempts_total = Counter(
        "webhook_delivery_attempts_total",
        "Total number of webhook dispatch attempts",
        ["store_id", "status_code", "success"]
    )
    webhook_delivery_duration_seconds = Histogram(
        "webhook_delivery_duration_seconds",
        "Time spent executing webhook dispatch attempts",
        ["store_id"]
    )
    webhook_partition_pauses_total = Counter(
        "webhook_partition_pauses_total",
        "Total number of partition pauses due to circuit breaker trip",
        ["store_id", "partition"]
    )
except ImportError:
    webhook_delivery_attempts_total = None
    webhook_delivery_duration_seconds = None
    webhook_partition_pauses_total = None

class WebhookMessagingSubscriber:
    """Inbound Messaging Adapter for Webhook Service handling backpressure, pauses, and retries"""
    def __init__(self, bootstrap_servers: str, db):
        self.bootstrap_servers = bootstrap_servers
        self.db = db
        self.consumer = None
        self.tasks = []
        self.active_probes = {}  # store_id -> Task
        self._is_running = False

    async def start(self) -> None:
        """Initialize the Kafka Consumer and start background message polling loop"""
        logger.info("Initializing Kafka Consumer for Webhook Service...")
        self.consumer = AIOKafkaConsumer(
            "store.registered", "order.confirmed",
            bootstrap_servers=self.bootstrap_servers,
            group_id="webhook_service_group",
            enable_auto_commit=False,  # Enforce manual offset commits for reliability
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest"
        )
        
        # Connection retry loop to handle Kafka startup delays
        retries = 15
        delay = 3.0
        for i in range(retries):
            try:
                logger.info(f"Connecting consumer to Kafka (Attempt {i+1}/{retries})...")
                await self.consumer.start()
                break
            except Exception as e:
                if i == retries - 1:
                    raise e
                logger.warning(f"Consumer failed to connect to Kafka: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)

        self._is_running = True
        self.tasks.append(asyncio.create_task(self._consume_loop()))
        logger.info("Webhook Service consumer started listening on store.registered and order.confirmed.")

    async def stop(self) -> None:
        """Gracefully stop consumer loops and clear active health probes"""
        self._is_running = False
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        for store_id, probe_task in list(self.active_probes.items()):
            probe_task.cancel()
            try:
                await probe_task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
        logger.info("Webhook Service consumer shut down successfully.")

    async def _consume_loop(self) -> None:
        """Continuous message polling loop with offset seek-back on processing failures"""
        while self._is_running:
            try:
                async for msg in self.consumer:
                    topic = msg.topic
                    payload = msg.value
                    partition = msg.partition
                    offset = msg.offset
                    tp = TopicPartition(topic, partition)

                    # Extract OTel context from Kafka message headers
                    headers_dict = {}
                    if msg.headers:
                        for k, v in msg.headers:
                            key_str = k.decode("utf-8") if isinstance(k, bytes) else k
                            val_str = v.decode("utf-8") if isinstance(v, bytes) else v
                            headers_dict[key_str] = val_str

                    from opentelemetry import trace
                    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
                    parent_context = TraceContextTextMapPropagator().extract(carrier=headers_dict)

                    tracer = trace.get_tracer("webhook-consumer")
                    with tracer.start_as_current_span(
                        name=f"kafka.consume {topic}",
                        context=parent_context,
                        kind=trace.SpanKind.CONSUMER
                    ) as span:
                        span.set_attribute("messaging.system", "kafka")
                        span.set_attribute("messaging.destination", topic)
                        span.set_attribute("messaging.kafka.consumer_group", "webhook_service_group")
                        span.set_attribute("messaging.kafka.partition", partition)

                        # Execute processing command
                        success = await self._process_message(topic, payload, tp)

                        if success:
                            # Commit offset indicating successful processing (offset + 1)
                            await self.consumer.commit({tp: msg.offset + 1})
                        else:
                            # Seek back to current offset on transient failure so it is re-read on partition resume
                            logger.warning(f"Transient error: seeking back partition {tp} to offset {offset}")
                            self.consumer.seek(tp, offset)
                            # Yield control to allow loop to process other partitions if any
                            await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Critical exception in Webhook consume loop: {e}. Reconnecting in 5s...", exc_info=True)
                await asyncio.sleep(5.0)

    async def _process_message(self, topic: str, payload: dict, tp: TopicPartition) -> bool:
        """Routes message payload to the correct domain handler"""
        try:
            if topic == "store.registered":
                return await self._handle_store_registered(payload)
            elif topic == "order.confirmed":
                return await self._handle_order_confirmed(payload, tp)
            return True
        except Exception as err:
            logger.error(f"Unexpected error routing message from {topic}: {err}", exc_info=True)
            return True # Discard corrupt messages to prevent infinite blocking

    async def _handle_store_registered(self, payload: dict) -> bool:
        """Materializes store metadata locally in PostgreSQL"""
        store_id = payload.get("store_id")
        name = payload.get("name")
        webhook_url = payload.get("webhook_url")
        is_famous = payload.get("is_famous", False)

        if not store_id or not name:
            logger.error(f"Discarding invalid store.registered payload: {payload}")
            return True

        event_id = payload.get("metadata", {}).get("event_id", f"store-registered-fallback-{store_id}")

        async with self.db._session_maker() as session:
            try:
                # 1. Deduplication Check (Inbox Pattern)
                # Prepend 'mat-' to event_id for CQRS read model materialization
                mat_event_id = f"mat-{event_id}"
                is_duplicate = await check_and_register_event(session, mat_event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate 'store.registered' event detected (ID: {event_id}). "
                        f"Skipping materialization to ensure idempotency."
                    )
                    await session.commit()
                    return True

                repo = SQLAlchemyWebhookRepository(session)
                await repo.save_materialized_store(store_id, name, webhook_url, is_famous=is_famous)
                await session.commit()
                logger.info(f"Materialized store registered event: ID {store_id}, name='{name}', is_famous={is_famous}")
                return True
            except Exception as db_err:
                logger.error(f"Database error materializing store {store_id}: {db_err}")
                await session.rollback()
                return False

    async def _handle_order_confirmed(self, payload: dict, tp: TopicPartition) -> bool:
        """Resolves webhook address from materialized local view and dispatches POST request"""
        order_id = payload.get("order_id")
        store_id = payload.get("store_id")
        total_price = payload.get("total_price")

        if not order_id or not store_id:
            logger.error(f"Discarding invalid order.confirmed event payload: {payload}")
            return True

        event_id = payload.get("metadata", {}).get("event_id", f"order-confirmed-fallback-{order_id}")

        async with self.db._session_maker() as session:
            try:
                repo = SQLAlchemyWebhookRepository(session)
                
                # 1. Deduplication Check (Inbox Pattern)
                is_duplicate = await check_and_register_event(session, event_id)
                if is_duplicate:
                    logger.warning(
                        f"Inbox Pattern: Duplicate 'order.confirmed' event detected (ID: {event_id}). "
                        f"Skipping to ensure idempotency."
                    )
                    await session.commit()
                    return True

                # 2. Resolve store metadata locally (Zero HTTP REST calls to product-service)
                store = await repo.find_materialized_store(store_id)
                if not store or not store.webhook_url:
                    logger.warning(f"Store ID {store_id} webhook details not materialized. Routing to DLQ.")
                    await self._route_to_dlq(payload, "StoreNotMaterializedException", f"No webhook configured for store {store_id}")
                    await session.commit()
                    return True

                webhook_url = store.webhook_url
                breaker = breaker_registry.get_breaker(store_id)

                # 3. Check if circuit breaker is currently open
                if breaker.state == "OPEN":
                    if store.is_famous:
                        logger.warning(f"Webhook dispatch for famous store {store_id} blocked: Circuit is OPEN. Pausing partition {tp}.")
                        self._pause_partition_and_start_probe(store_id, tp, webhook_url)
                        await session.rollback()
                        return False
                    else:
                        logger.warning(f"Webhook dispatch for small store {store_id} blocked: Circuit is OPEN. Fast-failing to DLQ without pausing partition.")
                        await self._route_to_dlq(payload, "CircuitBreakerOpenException", f"Circuit open for small store {store_id}")
                        await session.commit()
                        return True

                attempt = 1
                success = False
                response_status = None
                response_body = None

                async def _do_dispatch():
                    nonlocal response_status, response_body
                    import httpx
                    
                    headers = {"Content-Type": "application/json"}
                    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
                    TraceContextTextMapPropagator().inject(headers)

                    async with httpx.AsyncClient() as client:
                        try:
                            res = await client.post(
                                webhook_url,
                                json={
                                    "event_type": "OrderConfirmed",
                                    "order_id": order_id,
                                    "store_id": store_id,
                                    "total_price": total_price
                                },
                                headers=headers,
                                timeout=5.0
                            )
                            response_status = res.status_code
                            response_body = res.text
                            res.raise_for_status()
                        except Exception as err:
                            from shared.common.resilience import is_retriable_exception
                            if is_retriable_exception(err):
                                raise RetriableWebhookError(str(err)) from err
                            else:
                                raise NonRetriableWebhookError(str(err)) from err

                # 4. Call HTTP Dispatch inside the circuit breaker wrapper
                import time
                start_time = time.perf_counter()
                try:
                    from opentelemetry import trace
                    from opentelemetry.trace import StatusCode
                    tracer = trace.get_tracer("webhook-dispatcher")
                    with tracer.start_as_current_span(
                        name=f"webhook.dispatch {store_id}",
                        kind=trace.SpanKind.CLIENT
                    ) as dispatch_span:
                        dispatch_span.set_attribute("http.url", webhook_url)
                        dispatch_span.set_attribute("http.method", "POST")
                        dispatch_span.set_attribute("webhook.store_id", str(store_id))
                        dispatch_span.set_attribute("webhook.order_id", str(order_id))
                        dispatch_span.set_attribute("webhook.is_famous", bool(store.is_famous))
                        
                        try:
                            await breaker.call(_do_dispatch)
                            success = True
                            logger.info(f"Webhook successfully delivered to Store {store_id} for Order {order_id}!")
                            
                            # Record success metrics
                            if webhook_delivery_attempts_total:
                                webhook_delivery_attempts_total.labels(store_id=str(store_id), status_code=str(response_status or 200), success="true").inc()
                            if webhook_delivery_duration_seconds:
                                duration = time.perf_counter() - start_time
                                webhook_delivery_duration_seconds.labels(store_id=str(store_id)).observe(duration)
                            
                            dispatch_span.set_attribute("http.status_code", response_status or 200)
                            dispatch_span.set_status(StatusCode.OK)
                        except Exception as err:
                            dispatch_span.set_attribute("http.status_code", response_status or 0)
                            dispatch_span.set_status(StatusCode.ERROR, str(err))
                            raise err
                except Exception as err:
                    logger.warning(f"Webhook delivery failed for Store {store_id}: {err}")
                    
                    # Record failure metrics
                    if webhook_delivery_attempts_total:
                        webhook_delivery_attempts_total.labels(store_id=str(store_id), status_code=str(response_status or 0), success="false").inc()
                    if webhook_delivery_duration_seconds:
                        duration = time.perf_counter() - start_time
                        webhook_delivery_duration_seconds.labels(store_id=str(store_id)).observe(duration)

                    from shared.common.resilience import is_retriable_exception
                    if isinstance(err, NonRetriableWebhookError):
                        retriable = False
                    elif isinstance(err, RetriableWebhookError):
                        retriable = True
                    else:
                        retriable = is_retriable_exception(err)

                    # Audit failure in PostgreSQL log
                    await repo.log_delivery(
                        order_id=order_id,
                        store_id=store_id,
                        event_type="OrderConfirmed",
                        webhook_url=webhook_url,
                        request_payload=payload,
                        response_status=response_status,
                        response_body=response_body[:1000] if response_body else str(err),
                        attempt=attempt,
                        success=False
                    )

                    if not retriable:
                        # Non-retriable failure: route to DLQ, commit transaction to save inbox and audit log
                        logger.error(f"Non-retriable failure routing store webhook. Moving to DLQ.")
                        await self._route_to_dlq(payload, err.__class__.__name__, str(err))
                        await session.commit()
                        return True

                    # Retriable failure: check if circuit breaker tripped
                    if breaker.state == "OPEN":
                        if store.is_famous:
                            logger.error(f"Circuit breaker tripped to OPEN for famous Store {store_id}! Pausing partition {tp}.")
                            self._pause_partition_and_start_probe(store_id, tp, webhook_url)
                            # Rollback transaction so inbox check isn't persisted (can be retried)
                            await session.rollback()
                            return False
                        else:
                            logger.error(f"Circuit breaker tripped to OPEN for small Store {store_id}! Fast-failing to DLQ without pausing partition.")
                            await self._route_to_dlq(payload, err.__class__.__name__, f"Circuit breaker tripped to OPEN: {str(err)}")
                            await session.commit()
                            return True

                    # Circuit is still closed, rollback transaction (so inbox check is retried) and seek back
                    logger.warning(f"Store {store_id} webhook transient failure. Circuit still CLOSED. Seeking back.")
                    await session.rollback()
                    await asyncio.sleep(2.0)
                    return False

                # 5. Audit success in PostgreSQL log
                await repo.log_delivery(
                    order_id=order_id,
                    store_id=store_id,
                    event_type="OrderConfirmed",
                    webhook_url=webhook_url,
                    request_payload=payload,
                    response_status=response_status,
                    response_body=response_body[:1000] if response_body else "SUCCESS",
                    attempt=attempt,
                    success=True
                )
                await session.commit()
                return True
            except Exception as db_err:
                logger.error(f"Database or system failure processing order.confirmed: {db_err}", exc_info=True)
                await session.rollback()
                return False

    def _pause_partition_and_start_probe(self, store_id: int, tp: TopicPartition, webhook_url: str) -> None:
        """Pause message retrieval for the TopicPartition and initiate background recovery health checks"""
        self.consumer.pause(tp)
        logger.info(f"PAUSED Kafka consumption on partition {tp} for Store {store_id}.")
        
        # Record partition pause metric
        if webhook_partition_pauses_total:
            webhook_partition_pauses_total.labels(store_id=str(store_id), partition=str(tp.partition)).inc()

        if store_id not in self.active_probes:
            self.active_probes[store_id] = asyncio.create_task(
                self._probe_and_recover(store_id, tp, webhook_url)
            )

    async def _probe_and_recover(self, store_id: int, tp: TopicPartition, webhook_url: str) -> None:
        """Active probing task checking if the store webhook has recovered"""
        import httpx
        logger.info(f"Initiated background health probe for Store {store_id} webhook at: {webhook_url}")
        
        breaker = breaker_registry.get_breaker(store_id)
        probe_interval = 10.0

        while self._is_running:
            await asyncio.sleep(probe_interval)
            logger.info(f"Active health probe: pinging Store {store_id} webhook...")
            try:
                async with httpx.AsyncClient() as client:
                    res = await client.post(
                        webhook_url,
                        json={
                            "event_type": "HealthProbe",
                            "store_id": store_id
                        },
                        timeout=3.0
                    )
                    # Any response status < 500 implies host machine is available/processing requests
                    if res.status_code < 500:
                        logger.info(f"Store {store_id} responded with status {res.status_code}. Webhook recovered!")
                        break
            except Exception as probe_err:
                logger.warning(f"Health probe to Store {store_id} failed: {probe_err}")

        # Recovery Logic
        logger.info(f"Recovering Store {store_id}: resetting circuit breaker and resuming partition {tp}")
        async with breaker._lock:
            breaker.state = "CLOSED"
            breaker.failure_count = 0
            from shared.common.resilience import circuit_breaker_state_gauge
            if circuit_breaker_state_gauge:
                circuit_breaker_state_gauge.labels(name=breaker.name).set(0)

        self.consumer.resume(tp)
        logger.info(f"RESUMED partition {tp} successfully.")
        self.active_probes.pop(store_id, None)

    async def _route_to_dlq(self, payload: dict, error_class: str, error_message: str) -> None:
        """Route failed webhook messages to the dead letter queue (DLQ) topic"""
        dlq_topic = "webhook.deadletter"
        dlq_payload = {
            "metadata": {
                "original_topic": "order.confirmed",
                "failed_at": datetime.datetime.utcnow().isoformat() + "Z",
                "error_class": error_class,
                "error_message": error_message
            },
            "original_payload": payload
        }
        logger.warning(f"Routing failed event to DLQ: {dlq_topic}")
        try:
            from aiokafka import AIOKafkaProducer
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await producer.start()
            try:
                await producer.send_and_wait(dlq_topic, value=dlq_payload)
                logger.info(f"DLQ Pipeline: Successfully routed event to {dlq_topic}")
            finally:
                await producer.stop()
        except Exception as dlq_err:
            logger.error(f"DLQ Pipeline CRITICAL failure: {dlq_err}", exc_info=True)
