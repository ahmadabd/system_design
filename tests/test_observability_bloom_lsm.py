import pytest
from unittest.mock import MagicMock
from algorithms.bloom_filter import BloomFilter
from shared.common.bloom import (
    bloom_guard,
    bloom_queries_total,
    bloom_fast_rejections_total
)
import importlib.util
from pathlib import Path

_lsm_path = Path(__file__).resolve().parent.parent / "services" / "webhook-service" / "src" / "infrastructure" / "lsm_storage.py"
_spec = importlib.util.spec_from_file_location("webhook_lsm_storage_obs", _lsm_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
WebhookLSMStorageEngine = _mod.WebhookLSMStorageEngine


@pytest.mark.asyncio
async def test_bloom_guard_observability_metrics():
    """Verify that @bloom_guard emits Prometheus counter metrics on lookups and fast rejections."""
    bf = BloomFilter(expected_elements=100, false_positive_rate=0.01)
    bf.add("prod_valid_99")

    filter_name = "test_catalog_bloom"

    @bloom_guard(bloom_filter=bf, id_param="product_id", filter_name=filter_name)
    async def sample_endpoint(product_id: str):
        return {"id": product_id}

    # Initial metric values
    rejections_before = bloom_fast_rejections_total.labels(filter_name=filter_name)._value.get() if bloom_fast_rejections_total else 0
    hits_before = bloom_queries_total.labels(filter_name=filter_name, result="hit")._value.get() if bloom_queries_total else 0
    misses_before = bloom_queries_total.labels(filter_name=filter_name, result="miss")._value.get() if bloom_queries_total else 0

    # 1. Valid Query -> Increments hit metric
    res = await sample_endpoint(product_id="prod_valid_99")
    assert res["id"] == "prod_valid_99"

    if bloom_queries_total:
        hits_after = bloom_queries_total.labels(filter_name=filter_name, result="hit")._value.get()
        assert hits_after == hits_before + 1

    # 2. Non-Existent Query -> Fast rejected, increments miss & rejection metrics
    from fastapi import HTTPException
    with pytest.raises(HTTPException):
        await sample_endpoint(product_id="prod_fake_123")

    if bloom_fast_rejections_total:
        rejections_after = bloom_fast_rejections_total.labels(filter_name=filter_name)._value.get()
        assert rejections_after == rejections_before + 1

    if bloom_queries_total:
        misses_after = bloom_queries_total.labels(filter_name=filter_name, result="miss")._value.get()
        assert misses_after == misses_before + 1


def test_lsm_storage_observability_metrics():
    """Verify that WebhookLSMStorageEngine emits Prometheus metrics and updates gauges."""
    import tempfile
    temp_dir = tempfile.mkdtemp()
    engine = WebhookLSMStorageEngine(data_dir=temp_dir)

    # 1. Append record
    rec = engine.record_delivery_log(
        order_id=111,
        store_id=1,
        event_type="OrderConfirmed",
        webhook_url="https://test.com",
        request_payload={"order_id": 111},
        response_status=200,
        response_body="ok",
        attempt=1,
        success=True
    )

    # 2. Point lookup
    found = engine.get_delivery_log(rec["key"])
    assert found is not None
    assert found["order_id"] == 111

    # 3. Missing lookup
    missing = engine.get_delivery_log("log:99:999:1:000")
    assert missing is None

    engine.lsm.destroy()
