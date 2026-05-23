import asyncio
import json
import logging
from typing import Callable, Any, Coroutine, List
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from shared.common.resilience import AsyncCircuitBreaker

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
                return
            except Exception as e:
                logger.warning(f"Failed to connect to Kafka: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
        raise ConnectionError("Could not establish connection to Kafka after multiple retries.")

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
            key = str(event_data.get("order_id") or event_data.get("user_id") or "").encode("utf-8") or None
            
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
                
                # Inject the active PRODUCER span context into the headers
                headers_dict = {}
                from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
                TraceContextTextMapPropagator().inject(headers_dict)
                
                # Convert to list of tuples format: (str, bytes) for Kafka headers
                kafka_headers = [
                    (k, v.encode("utf-8")) 
                    for k, v in headers_dict.items()
                ]
                
                await self.producer.send_and_wait(topic, value=event_data, key=key, headers=kafka_headers)
                logger.info(f"Published message to Kafka topic '{topic}' with key '{key.decode() if key else 'None'}'")

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
                                
                                # Process the message callback under the active span context
                                await callback(msg.value)
                        except Exception as cb_err:
                            logger.error(
                                f"Error handling message in consumer callback for topic '{topic}': {cb_err}",
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
