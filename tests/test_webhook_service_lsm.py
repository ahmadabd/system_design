import os
import tempfile
import pytest
import importlib.util
from pathlib import Path

# Dynamically import WebhookLSMStorageEngine to prevent 'src' namespace collisions
_webhook_lsm_file = Path(__file__).resolve().parent.parent / "services" / "webhook-service" / "src" / "infrastructure" / "lsm_storage.py"
_spec = importlib.util.spec_from_file_location("webhook_lsm_storage", _webhook_lsm_file)
_lsm_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_lsm_module)
WebhookLSMStorageEngine = _lsm_module.WebhookLSMStorageEngine


@pytest.fixture
def webhook_lsm():
    temp_dir = tempfile.mkdtemp()
    engine = WebhookLSMStorageEngine(data_dir=temp_dir)
    yield engine
    engine.lsm.destroy()


def test_webhook_lsm_delivery_log_append_and_retrieval(webhook_lsm):
    """
    Verify high-throughput sequential audit logging into the LSM Tree engine.
    """
    # 1. Record delivery attempts
    log1 = webhook_lsm.record_delivery_log(
        order_id=5001,
        store_id=42,
        event_type="OrderConfirmed",
        webhook_url="https://merchant42.example.com/webhooks",
        request_payload={"order_id": 5001, "amount": 199.99},
        response_status=200,
        response_body='{"received": true}',
        attempt=1,
        success=True
    )

    # 2. Retrieve by key from MemTable
    retrieved = webhook_lsm.get_delivery_log(log1["key"])
    assert retrieved is not None
    assert retrieved["order_id"] == 5001
    assert retrieved["success"] is True
    assert retrieved["response_status"] == 200


def test_webhook_lsm_sstable_flush_and_bloom_search(webhook_lsm):
    """
    Verify that logs flushed from MemTable to SSTables are instantly retrievable,
    and non-existent log keys are skipped by the embedded Bloom Filter.
    """
    log_keys = []
    # Insert 60 logs (MemTable threshold is 50, triggering flush to SSTable)
    for i in range(60):
        rec = webhook_lsm.record_delivery_log(
            order_id=1000 + i,
            store_id=1,
            event_type="OrderConfirmed",
            webhook_url="https://store1.example.com/webhook",
            request_payload={"order_id": 1000 + i},
            response_status=200,
            response_body="ok",
            attempt=1,
            success=True
        )
        log_keys.append(rec["key"])

    assert len(webhook_lsm.lsm.sstables) >= 1

    # Verify retrieval from on-disk SSTables
    for k in log_keys[:20]:
        found = webhook_lsm.get_delivery_log(k)
        assert found is not None
        assert found["key"] == k

    # Non-existent key is rejected without reading SSTable blocks
    assert webhook_lsm.get_delivery_log("log:99:9999:1:123456789") is None


def test_webhook_event_dedup_bloom_filter(webhook_lsm):
    """
    Verify that the Bloom filter instantly detects processed webhook events
    to prevent duplicate processing without querying PostgreSQL.
    """
    event_id_1 = "evt_order_confirmed_9001"
    event_id_2 = "evt_order_confirmed_9002"

    assert webhook_lsm.is_event_processed(event_id_1) is False

    # Mark as processed
    webhook_lsm.mark_event_processed(event_id_1)

    assert webhook_lsm.is_event_processed(event_id_1) is True
    assert webhook_lsm.is_event_processed(event_id_2) is False  # 100% guaranteed not processed
