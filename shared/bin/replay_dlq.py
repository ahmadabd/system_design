#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import sys
import argparse
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("DLQReplayUtility")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# All known DLQ topics corresponding to our integration events
DEFAULT_DLQ_TOPICS = [
    "order.created.deadletter",
    "inventory.reserved.deadletter",
    "inventory.failed.deadletter",
    "payment.succeeded.deadletter",
    "payment.failed.deadletter",
    "user.registered.deadletter"
]


async def replay_dlq_topic(dlq_topic: str, producer: AIOKafkaProducer) -> int:
    """Consumes all pending messages from a DLQ topic and replays them to their original topics.
    Uses high-watermark boundaries to prevent tail-chasing infinite loops.
    """
    logger.info(f"Scanning DLQ topic '{dlq_topic}' for failed events...")
    
    # Create consumer for the specific DLQ topic
    consumer = AIOKafkaConsumer(
        dlq_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="dlq_replay_admin_group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Manually commit after republishing to prevent loss
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    
    await consumer.start()
    
    # Query partitions and their end offsets (high watermarks) at start time to prevent tail-chasing loop
    partitions = consumer.partitions_for_topic(dlq_topic)
    if not partitions:
        logger.info(f"No partitions found for DLQ topic '{dlq_topic}'.")
        await consumer.stop()
        return 0

    from aiokafka import TopicPartition
    topic_partitions = [TopicPartition(dlq_topic, p) for p in partitions]
    end_offsets = await consumer.end_offsets(topic_partitions)
    
    active_partitions = {tp for tp, offset in end_offsets.items() if offset > 0}
    if not active_partitions:
        logger.info(f"No events found in '{dlq_topic}'.")
        await consumer.stop()
        return 0

    logger.info(f"Initial DLQ end offsets for '{dlq_topic}': {end_offsets}")
    replayed_count = 0
    drained_partitions = set()
    
    try:
        while True:
            # Check if we have processed up to the start-time high watermark for all active partitions
            if active_partitions.issubset(drained_partitions):
                logger.info(f"Drained all active partitions up to their initial offsets for '{dlq_topic}'.")
                break

            try:
                # Poll for a batch of messages
                records = await consumer.getmany(timeout_ms=2000, max_records=10)
                if not records:
                    break  # Timeout - no more messages
                
                for tp, messages in records.items():
                    if tp in drained_partitions:
                        continue
                        
                    limit_offset = end_offsets.get(tp, 0)
                    for msg in messages:
                        if msg.offset >= limit_offset:
                            drained_partitions.add(tp)
                            logger.info(f"Partition {tp.partition} reached start-time end offset {limit_offset}. Drained.")
                            break
                            
                        envelope = msg.value
                        if not isinstance(envelope, dict) or "metadata" not in envelope or "original_payload" not in envelope:
                            logger.warning(
                                f"Skipping invalid DLQ envelope on '{dlq_topic}' offset {msg.offset}: {envelope}"
                            )
                            # Manually commit this message offset on the DLQ topic to mark it as skipped
                            tp_offset = {tp: msg.offset + 1}
                            await consumer.commit(tp_offset)
                            continue
                        
                        metadata = envelope["metadata"]
                        original_topic = metadata.get("original_topic")
                        original_payload = envelope["original_payload"]
                        
                        if not original_topic:
                            logger.error(
                                f"Skipping event: original_topic not specified in metadata for offset {msg.offset}"
                            )
                            # Manually commit this message offset on the DLQ topic to mark it as skipped
                            tp_offset = {tp: msg.offset + 1}
                            await consumer.commit(tp_offset)
                            continue
                        
                        logger.info(
                            f"[REPLAYING] Found event in '{dlq_topic}' (failed due to: {metadata.get('error_class')}). "
                            f"Republishing to original topic '{original_topic}'..."
                        )
                        
                        # Reserialize and republish back to the original topic
                        key = str(original_payload.get("order_id") or original_payload.get("user_id") or "").encode("utf-8") or None
                        await producer.send_and_wait(
                            original_topic,
                            value=original_payload,
                            key=key
                        )
                        
                        # Manually commit this message offset on the DLQ topic to mark it as replayed
                        tp_offset = {tp: msg.offset + 1}
                        await consumer.commit(tp_offset)
                        
                        replayed_count += 1
                        logger.info(
                            f"[SUCCESS] Replayed event {replayed_count} back to '{original_topic}' and committed DLQ offset."
                        )
                        
            except Exception as loop_err:
                logger.error(f"Error in replay loop for '{dlq_topic}': {loop_err}", exc_info=True)
                break
                
    finally:
        await consumer.stop()
        
    if replayed_count > 0:
        logger.info(f"Finished replaying '{dlq_topic}': Replayed {replayed_count} events.")
    else:
        logger.info(f"No events found in '{dlq_topic}'.")
        
    return replayed_count


async def main():
    parser = argparse.ArgumentParser(description="DLQ Replay Resiliency Utility")
    parser.add_argument(
        "--topic",
        type=str,
        help="Specific DLQ topic to replay. If omitted, all known DLQ topics are scanned."
    )
    args = parser.parse_args()
    
    logger.info(f"Starting DLQ Replay Utility. Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Initialize Kafka Producer for republishing
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    
    total_replayed = 0
    try:
        if args.topic:
            topics_to_scan = [args.topic]
        else:
            topics_to_scan = DEFAULT_DLQ_TOPICS
            
        for topic in topics_to_scan:
            try:
                count = await replay_dlq_topic(topic, producer)
                total_replayed += count
            except Exception as scan_err:
                logger.error(f"Failed scanning topic '{topic}': {scan_err}")
                
        logger.info(f"DLQ Replay complete. Total replayed events across all topics: {total_replayed}")
    finally:
        await producer.stop()
        logger.info("DLQ Replay Producer shut down successfully.")


if __name__ == "__main__":
    asyncio.run(main())
