import asyncio
import json
import logging
import time
from typing import Callable, Any, Coroutine, List
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from shared.common.resilience import AsyncCircuitBreaker

try:
    from prometheus_client import Counter, Histogram
    messaging_kafka_messages_total = Counter(
        "messaging_kafka_messages_total",
        "Total number of Kafka messages sent or received",
        ["topic", "operation"]
    )
    messaging_process_duration_seconds = Histogram(
        "messaging_process_duration_seconds",
        "Time spent executing message callbacks",
        ["topic"]
    )
    messaging_dlq_routed_total = Counter(
        "messaging_dlq_routed_total",
        "Total number of messages routed to DLQ",
        ["original_topic", "consumer_group", "error_class"]
    )
    messaging_consumer_retries_total = Counter(
        "messaging_consumer_retries_total",
        "Total number of consumer callback retry attempts",
        ["topic", "consumer_group", "attempt"]
    )
except ImportError:
    messaging_kafka_messages_total = None
    messaging_process_duration_seconds = None
    messaging_dlq_routed_total = None
    messaging_consumer_retries_total = None

logger = logging.getLogger("KafkaManager")

class KafkaManager:
    """Asynchronous Apache Kafka producer and consumer manager with circuit breaker protection"""
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer: AIOKafkaProducer | None = None
        self.consumers: List[AIOKafkaConsumer] = []
        self.tasks: List[asyncio.Task] = []
        # Unique Kafka publisher circuit breaker
        self.kafka_breaker = AsyncCircuitBreaker(
            name="kafka-message-broker",
            failure_threshold=5,
            recovery_timeout=15.0
        )

    async def connect(self, retries: int = 10, delay: float = 3.0) -> None:
        """Initialize the Kafka Producer with connection retry logic"""
        for i in range(retries):
            try:
                logger.info(f"Connecting to Kafka at {self.bootstrap_servers} (Attempt {i+1}/{retries})...")
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                await self.producer.start()
                logger.info("Successfully connected to Kafka and started Producer!")
                try:
                    await self.ensure_topics_exist(["order.created", "order.confirmed", "store.registered"], num_partitions=8)
                except Exception as topics_err:
                    logger.warning(f"Failed to ensure topics exist during startup: {topics_err}")
                return
            except Exception as e:
                logger.warning(f"Failed to connect to Kafka: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
        raise ConnectionError("Could not establish connection to Kafka after multiple retries.")

    async def ensure_topics_exist(self, topics: List[str], num_partitions: int = 8) -> None:
        """Ensure that the given Kafka topics exist with the specified number of partitions"""
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        try:
            await admin.start()
            existing_topics = await admin.list_topics()
            new_topics = []
            for topic in topics:
                if topic not in existing_topics:
                    logger.info(f"Creating topic '{topic}' with {num_partitions} partitions...")
                    new_topics.append(NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1))
            if new_topics:
                await admin.create_topics(new_topics=new_topics)
                logger.info(f"Successfully created topics: {[t.name for t in new_topics]}")
        except Exception as e:
            logger.warning(f"Failed to ensure Kafka topics exist via Admin Client: {e}")
        finally:
            await admin.close()

    async def close(self) -> None:
        """Safely stop Kafka producer and background consumer tasks"""
        # Cancel all background consumer tasks
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Stop producer
        if self.producer:
            await self.producer.stop()
            logger.info("Closed Kafka Producer safely.")
        
        # Stop any active consumers
        for consumer in self.consumers:
            try:
                await consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer: {e}")
        logger.info("All Kafka connections torn down safely.")

    async def publish(self, exchange_name: str, routing_key: str, event_data: dict) -> None:
        """Publish a message to a Kafka topic with circuit breaker fail-fast protection"""
        if not self.producer:
            raise RuntimeError("Producer is not active. Call connect() first.")
        
        async def _do_publish():
            topic = routing_key
            
            # Extract store partitioning metadata
            store_id = event_data.get("store_id")
            is_famous = event_data.get("is_famous", False)
            partition = None
            
            if store_id is not None:
                try:
                    store_id_int = int(store_id)
                except (ValueError, TypeError):
                    store_id_int = hash(str(store_id))
                
                if is_famous:
                    # Partitions 0-3 are dedicated to famous stores
                    partition = store_id_int % 4
                else:
                    # Partitions 4-7 are shared for small/non-famous stores
                    partition = 4 + (store_id_int % 4)
            
            key = str(event_data.get("store_id") or event_data.get("order_id") or event_data.get("user_id") or "").encode("utf-8") or None
            
            # Start an active PRODUCER span for visual timeline tracking in Jaeger
            from opentelemetry import trace
            tracer = trace.get_tracer("kafka-producer")
            with tracer.start_as_current_span(
                name=f"kafka.send {topic}",
                kind=trace.SpanKind.PRODUCER
            ) as span:
                span.set_attribute("messaging.system", "kafka")
                span.set_attribute("messaging.destination", topic)
                if key:
                    span.set_attribute("messaging.kafka.partition_key", key.decode("utf-8"))
                if partition is not None:
                    span.set_attribute("messaging.kafka.partition", partition)
                
                # Inject the active PRODUCER span context into the headers
                headers_dict = {}
                from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
                TraceContextTextMapPropagator().inject(headers_dict)
                
                # Convert to list of tuples format: (str, bytes) for Kafka headers
                kafka_headers = [
                    (k, v.encode("utf-8")) 
                    for k, v in headers_dict.items()
                ]
                
                await self.producer.send_and_wait(topic, value=event_data, key=key, partition=partition, headers=kafka_headers)
                if messaging_kafka_messages_total:
                    messaging_kafka_messages_total.labels(topic=topic, operation="send").inc()
                logger.info(f"Published message to Kafka topic '{topic}' with key '{key.decode() if key else 'None'}' on partition {partition}")

        await self.kafka_breaker.call(_do_publish)


    async def subscribe(
        self,
        exchange_name: str,
        queue_name: str,
        routing_key: str,
        callback: Callable[[dict], Coroutine[Any, Any, None]]
    ) -> None:
        """Subscribe to a Kafka topic.
        
        routing_key  -> Kafka topic name
        queue_name   -> Kafka Consumer Group ID (ensures competing-consumer load balancing)
        """
        topic = routing_key
        group_id = queue_name

        async def consume_loop():
            """
            Reconnect-aware consumer loop.
            AIOKafkaConsumer is single-use: once stopped it cannot be restarted,
            so we create a brand-new consumer instance on each reconnect cycle.
            """
            while True:
                consumer = AIOKafkaConsumer(
                    topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=group_id,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="earliest"
                )
                # Track current consumer so close() can stop it if needed
                self.consumers.append(consumer)
                try:
                    logger.info(f"Starting Kafka Consumer for topic '{topic}' in group '{group_id}'...")
                    await consumer.start()
                    async for msg in consumer:
                        try:
                            logger.info(f"Received event from Kafka topic '{topic}' in group '{group_id}'")
                            
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
                            
                            # Start a consumer span with parent link
                            tracer = trace.get_tracer("kafka-consumer")
                            with tracer.start_as_current_span(
                                name=f"kafka.consume {topic}",
                                context=parent_context,
                                kind=trace.SpanKind.CONSUMER
                            ) as span:
                                span.set_attribute("messaging.system", "kafka")
                                span.set_attribute("messaging.destination", topic)
                                span.set_attribute("messaging.kafka.consumer_group", group_id)
                                
                                try:
                                    # Process the message callback under the active span context with local retry loop
                                    start_time = time.perf_counter()
                                    
                                    max_retries = 3
                                    retry_delay = 1.0
                                    for attempt in range(1, max_retries + 1):
                                        try:
                                            await callback(msg.value)
                                            break
                                        except Exception as cb_err:
                                            from shared.common.resilience import is_retriable_exception
                                            retriable = is_retriable_exception(cb_err)
                                            
                                            if not retriable or attempt == max_retries:
                                                raise cb_err
                                            
                                            if messaging_consumer_retries_total:
                                                messaging_consumer_retries_total.labels(
                                                    topic=topic,
                                                    consumer_group=group_id,
                                                    attempt=str(attempt)
                                                ).inc()

                                            logger.warning(
                                                f"Transient error in consumer callback (attempt {attempt}/{max_retries}) for topic '{topic}': {cb_err}. "
                                                f"Retrying in {retry_delay}s..."
                                            )
                                            await asyncio.sleep(retry_delay)
                                            retry_delay *= 2
                                            
                                    duration = time.perf_counter() - start_time
                                    
                                    if messaging_kafka_messages_total:
                                        messaging_kafka_messages_total.labels(topic=topic, operation="receive").inc()
                                    if messaging_process_duration_seconds:
                                        messaging_process_duration_seconds.labels(topic=topic).observe(duration)
                                except Exception as cb_err:
                                    logger.error(
                                        f"Error handling message in consumer callback for topic '{topic}': {cb_err}. Dead-lettering...",
                                        exc_info=True
                                    )
                                    span.record_exception(cb_err)
                                    span.set_status(trace.StatusCode.ERROR, str(cb_err))
                                    # Route failed event to Dead Letter Queue (DLQ)
                                    await self._route_to_dlq(topic, group_id, msg.value, cb_err)
                        except Exception as loop_err:
                            logger.error(
                                f"Critical consumer loop parsing exception for topic '{topic}': {loop_err}",
                                exc_info=True
                            )
                except asyncio.CancelledError:
                    # Graceful shutdown — stop the current consumer and exit loop
                    try:
                        await consumer.stop()
                    except Exception:
                        pass
                    break
                except Exception as e:
                    logger.error(
                        f"Kafka consumer runtime error on topic '{topic}': {e}. Reconnecting in 5s...",
                        exc_info=True
                    )
                    try:
                        await consumer.stop()
                    except Exception:
                        pass
                    # Remove the stale consumer reference before creating a new one
                    if consumer in self.consumers:
                        self.consumers.remove(consumer)
                    await asyncio.sleep(5)
        
        task = asyncio.create_task(consume_loop())
        self.tasks.append(task)
        logger.info(f"Started background subscription consumer on Kafka topic '{topic}' for group '{group_id}'")

    async def _route_to_dlq(self, original_topic: str, consumer_group: str, message_value: Any, exception: Exception) -> None:
        """Route failed message payload along with diagnostic metadata to the dead-letter queue (DLQ)"""
        dlq_topic = f"{original_topic}.deadletter"
        import traceback
        import datetime
        
        # Build enriched diagnostics payload
        dlq_payload = {
            "metadata": {
                "original_topic": original_topic,
                "consumer_group": consumer_group,
                "failed_at": datetime.datetime.utcnow().isoformat() + "Z",
                "error_class": exception.__class__.__name__,
                "error_message": str(exception),
                "stack_trace": "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
            },
            "original_payload": message_value
        }
        
        logger.warning(f"DLQ Pipeline: Dead-lettering failed event from topic '{original_topic}' to '{dlq_topic}' due to error: {exception.__class__.__name__}")
        try:
            if messaging_dlq_routed_total:
                messaging_dlq_routed_total.labels(
                    original_topic=original_topic,
                    consumer_group=consumer_group,
                    error_class=exception.__class__.__name__
                ).inc()
            # Publish using standard circuit-breaker-protected publish method
            await self.publish(
                exchange_name="ecommerce.events",
                routing_key=dlq_topic,
                event_data=dlq_payload
            )
            logger.info(f"DLQ Pipeline: Successfully routed failed event to DLQ topic '{dlq_topic}'")
        except Exception as dlq_err:
            logger.error(f"DLQ Pipeline Critical Failure: Unable to publish to DLQ topic '{dlq_topic}': {dlq_err}", exc_info=True)
