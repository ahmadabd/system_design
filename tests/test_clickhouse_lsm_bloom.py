import sys
import os
import pytest
from unittest.mock import MagicMock
from pathlib import Path

_service_path = str(Path(__file__).resolve().parent.parent / "services" / "merchant-copilot-service")
if _service_path not in sys.path:
    sys.path.insert(0, _service_path)

from src.infrastructure.clickhouse_client import ClickHouseClient
from src.infrastructure.schema_catalog import CLICKHOUSE_TABLE_SCHEMAS


def test_clickhouse_schema_catalog_lsm_and_bloom_indexes():
    """
    Verify that ClickHouse DDL catalogs configure the LSM ReplacingMergeTree engine
    and secondary Bloom Filter skip indexes for high-speed OLAP skipping.
    """
    # 1. products_analytics
    prod_ddl = CLICKHOUSE_TABLE_SCHEMAS["products_analytics"]["ddl"]
    assert "ENGINE = ReplacingMergeTree" in prod_ddl
    assert "INDEX idx_store_id (store_id) TYPE bloom_filter(0.01)" in prod_ddl

    # 2. orders_analytics
    order_ddl = CLICKHOUSE_TABLE_SCHEMAS["orders_analytics"]["ddl"]
    assert "ENGINE = ReplacingMergeTree" in order_ddl
    assert "INDEX idx_user_id (user_id) TYPE bloom_filter(0.01)" in order_ddl

    # 3. order_items_analytics
    item_ddl = CLICKHOUSE_TABLE_SCHEMAS["order_items_analytics"]["ddl"]
    assert "ENGINE = ReplacingMergeTree" in item_ddl
    assert "INDEX idx_product_id (product_id) TYPE bloom_filter(0.01)" in item_ddl


def test_clickhouse_client_init_ddl_execution():
    """
    Verify that ClickHouseClient initializes tables with ReplacingMergeTree and Bloom Filter skip indexes.
    """
    client = ClickHouseClient()
    mock_ch_driver = MagicMock()
    client._client = mock_ch_driver

    client.init_database_and_tables()

    # Verify that DDL commands were issued
    assert mock_ch_driver.command.call_count >= 5
    executed_sql = " ".join(call.args[0] for call in mock_ch_driver.command.call_args_list)

    assert "ReplacingMergeTree" in executed_sql
    assert "TYPE bloom_filter(0.01)" in executed_sql
    assert "idx_store_id" in executed_sql
    assert "idx_user_id" in executed_sql
    assert "idx_product_id" in executed_sql
    assert "idx_transaction_id" in executed_sql
