import os
import time
import logging
from typing import Dict, Any, Optional, List
from algorithms.lsm_tree import LSMTree
from algorithms.bloom_filter import BloomFilter

logger = logging.getLogger("WebhookLSMStorage")


try:
    from prometheus_client import Counter, Gauge, REGISTRY

    def _get_or_create_counter(name, doc, labels):
        if name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[name]
        return Counter(name, doc, labels)

    def _get_or_create_gauge(name, doc, labels):
        if name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[name]
        return Gauge(name, doc, labels)

    lsm_appends_total = _get_or_create_counter(
        "lsm_storage_appends_total",
        "Total number of append writes into the LSM Tree engine",
        ["engine"]
    )
    lsm_reads_total = _get_or_create_counter(
        "lsm_storage_reads_total",
        "Total number of reads from the LSM Tree engine",
        ["engine", "result"]
    )
    lsm_memtable_gauge = _get_or_create_gauge(
        "lsm_storage_memtable_entries",
        "Current number of entries in active LSM MemTable",
        ["engine"]
    )
    lsm_sstables_gauge = _get_or_create_gauge(
        "lsm_storage_sstables_count",
        "Current number of immutable SSTable files on disk",
        ["engine"]
    )
except ImportError:
    lsm_appends_total = None
    lsm_reads_total = None
    lsm_memtable_gauge = None
    lsm_sstables_gauge = None


class WebhookLSMStorageEngine:
    """
    High-Throughput LSM-Tree Storage Engine for Webhook Delivery Audit Trails.
    
    Converts high-frequency random database writes into append-only sequential disk writes.
    Features:
    - Write-Ahead Log (WAL) for durability
    - MemTable write buffer in RAM
    - Immutable SSTables on disk with embedded Bloom Filters
    - Fast-path event deduplication via dedicated Bloom Filter
    - Full OpenTelemetry and Prometheus observability
    """
    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = data_dir or os.getenv("WEBHOOK_LSM_DATA_DIR", "/tmp/webhook_lsm_data")
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Initialize LSM Tree engine with MemTable threshold of 50 and compaction threshold of 4
        self.lsm = LSMTree(
            data_dir=self.data_dir,
            memtable_threshold=50,
            compaction_threshold=4
        )
        
        # Fast-path Bloom Filter for Event Deduplication & Idempotency
        self.dedup_bloom = BloomFilter(expected_elements=500000, false_positive_rate=0.01)
        logger.info(f"Webhook LSM Storage Engine initialized at: {self.data_dir}")

    def is_event_processed(self, event_id: str) -> bool:
        """
        Fast-path idempotency pre-check.
        Returns True if the event was PROBABLY already processed.
        Returns False if the event was DEFINITELY NOT processed (zero DB load).
        """
        return self.dedup_bloom.exists(str(event_id))

    def mark_event_processed(self, event_id: str) -> None:
        """Marks event ID as processed in the Bloom Filter."""
        self.dedup_bloom.add(str(event_id))

    def record_delivery_log(
        self,
        order_id: int,
        store_id: int,
        event_type: str,
        webhook_url: str,
        request_payload: dict,
        response_status: Optional[int],
        response_body: Optional[str],
        attempt: int,
        success: bool
    ) -> Dict[str, Any]:
        """
        Appends a delivery log record to the LSM Tree (MemTable + WAL).
        """
        now = time.time()
        log_key = f"log:{store_id}:{order_id}:{attempt}:{int(now * 1000)}"
        
        log_record = {
            "key": log_key,
            "order_id": order_id,
            "store_id": store_id,
            "event_type": event_type,
            "webhook_url": webhook_url,
            "request_payload": request_payload,
            "response_status": response_status,
            "response_body": response_body,
            "attempt": attempt,
            "success": success,
            "created_at": now
        }
        
        # Write to LSM Tree (RAM MemTable + Disk WAL)
        self.lsm.put(log_key, log_record)
        self.mark_event_processed(f"order_{order_id}_attempt_{attempt}")

        # Record Observability Metrics
        if lsm_appends_total:
            lsm_appends_total.labels(engine="webhook_lsm").inc()
        if lsm_memtable_gauge:
            lsm_memtable_gauge.labels(engine="webhook_lsm").set(len(self.lsm.memtable))
        if lsm_sstables_gauge:
            lsm_sstables_gauge.labels(engine="webhook_lsm").set(len(self.lsm.sstables))

        return log_record

    def get_delivery_log(self, log_key: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single delivery log by key from LSM storage with OpenTelemetry tracing."""
        from opentelemetry import trace
        tracer = trace.get_tracer("webhook-lsm-storage")
        with tracer.start_as_current_span("LSMTree: get_delivery_log") as span:
            span.set_attribute("lsm.engine", "webhook_lsm")
            span.set_attribute("lsm.key", log_key)

            result = self.lsm.get(log_key)
            outcome = "hit" if result else "miss"
            span.set_attribute("lsm.outcome", outcome)

            if lsm_reads_total:
                lsm_reads_total.labels(engine="webhook_lsm", result=outcome).inc()

            return result

    def flush(self) -> None:
        """Flushes active MemTable to an immutable SSTable on disk."""
        self.lsm.flush()

    def compact(self) -> None:
        """Triggers background merge-sort compaction."""
        self.lsm.compact()

    def close(self) -> None:
        """Safely flushes and closes LSM storage."""
        self.lsm.close()


# Global Singleton for the Webhook Service
webhook_lsm_engine = WebhookLSMStorageEngine()
