import asyncio
import inspect
import logging
from typing import List, Dict, Any, Optional
from opentelemetry import trace
from src.infrastructure.clickhouse_client import clickhouse_client
from src.infrastructure.config import settings

logger = logging.getLogger("ClickHouseMicroBatcher")

class ClickHouseMicroBatcher:
    """
    Resilient in-memory micro-batcher that collects Kafka events and flushes
    vectorized batch inserts into ClickHouse MergeTree tables, preventing
    'Too Many Parts' exceptions while guaranteeing at-least-once durability.
    """
    def __init__(self, batch_size: int = 500, flush_interval: float = 1.0):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        # Dedicated in-memory buffer per ClickHouse table
        self.buffers: Dict[str, List[Dict[str, Any]]] = {
            "products_analytics": [],
            "orders_analytics": [],
            "order_items_analytics": [],
            "payments_analytics": []
        }
        self.uncommitted_callbacks: List[Any] = []
        self._lock = asyncio.Lock()
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None

    async def start(self):
        """Starts the periodic background flush timer task"""
        if not self._running:
            self._running = True
            self._flush_task = asyncio.create_task(self._periodic_flush_loop())
            logger.info(f"ClickHouseMicroBatcher started with batch_size={self.batch_size}, flush_interval={self.flush_interval}s")

    async def stop(self):
        """Drains any remaining items and shuts down safely"""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # Final drain flush
        async with self._lock:
            await self._flush_all_tables_locked()
        logger.info("ClickHouseMicroBatcher stopped safely after final buffer drain.")

    async def enqueue(self, table_name: str, record: Dict[str, Any], on_commit_callback=None):
        """Non-blocking record enqueue into RAM buffer"""
        async with self._lock:
            if table_name not in self.buffers:
                self.buffers[table_name] = []
            self.buffers[table_name].append(record)
            if on_commit_callback:
                self.uncommitted_callbacks.append(on_commit_callback)

            # Flush immediately if buffer exceeds threshold
            if len(self.buffers[table_name]) >= self.batch_size:
                await self._flush_table_locked(table_name)

    async def _periodic_flush_loop(self):
        """Periodic timer flush ensuring low write latency (<= 1.0s)"""
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                async with self._lock:
                    await self._flush_all_tables_locked()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error during periodic batch flush: {e}", exc_info=True)

    async def _flush_all_tables_locked(self):
        """Flushes all table buffers under lock"""
        for table in list(self.buffers.keys()):
            if self.buffers[table]:
                await self._flush_table_locked(table)

    async def _flush_table_locked(self, table_name: str):
        """Flushes a single table buffer to ClickHouse and triggers offset commits upon success"""
        records = self.buffers[table_name]
        if not records:
            return

        to_insert = records.copy()
        self.buffers[table_name].clear()

        tracer = trace.get_tracer("merchant-copilot-service")
        with tracer.start_as_current_span("MicroBatcher: flush_table") as span:
            span.set_attribute("batcher.table_name", table_name)
            span.set_attribute("batcher.record_count", len(to_insert))

            try:
                inserted = await asyncio.to_thread(
                    clickhouse_client.insert_batch,
                    table_name,
                    to_insert
                )
                span.set_attribute("batcher.inserted_count", inserted)
                logger.info(f"Micro-batch flushed {inserted} records to ClickHouse '{table_name}'.")

                # Execute Kafka offset commit callbacks now that data is persisted to disk
                callbacks = self.uncommitted_callbacks.copy()
                self.uncommitted_callbacks.clear()
                for cb in callbacks:
                    try:
                        if inspect.iscoroutinefunction(cb):
                            await cb()
                        else:
                            cb()
                    except Exception as cb_err:
                        logger.warning(f"Error executing commit callback: {cb_err}")

            except Exception as e:
                span.record_exception(e)
                logger.error(f"Failed to flush batch to ClickHouse '{table_name}' ({e}). Re-queueing records to prevent data loss.")
                # Put records back in front of buffer for retry
                self.buffers[table_name] = to_insert + self.buffers[table_name]


micro_batcher = ClickHouseMicroBatcher(batch_size=500, flush_interval=1.0)
