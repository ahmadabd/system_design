import logging
from typing import List, Dict, Any, Optional
import clickhouse_connect
from opentelemetry import trace
from src.infrastructure.config import settings

logger = logging.getLogger("ClickHouseClient")

class ClickHouseClient:
    """Manages ClickHouse OLAP database connections, table schemas, and queries"""
    def __init__(self):
        self.host = settings.CLICKHOUSE_HOST
        self.port = settings.CLICKHOUSE_PORT
        self.database = settings.CLICKHOUSE_DB
        self.username = settings.CLICKHOUSE_USER
        self.password = settings.CLICKHOUSE_PASSWORD
        self._client = None

    def get_client(self):
        """Returns or lazily creates a ClickHouse client with async_insert support"""
        if self._client is None:
            try:
                self._client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    username=self.username,
                    password=self.password,
                    connect_timeout=5,
                    send_receive_timeout=15,
                    settings={
                        'async_insert': 1,
                        'wait_for_async_insert': 1,
                        'async_insert_busy_timeout_ms': 1000,
                        'async_insert_max_data_size': 100000
                    }
                )
                logger.info(f"Connected to ClickHouse OLAP at {self.host}:{self.port}/{self.database}")
            except Exception as e:
                logger.warning(f"Could not connect to ClickHouse at {self.host}:{self.port} ({e}). Running in fallback mode.")
                return None
        return self._client

    def init_database_and_tables(self) -> None:
        """Initializes database and ReplacingMergeTree analytical tables in ClickHouse"""
        client = self.get_client()
        if not client:
            logger.warning("ClickHouse unavailable. Skipping DDL schema creation.")
            return

        try:
            # 1. Create Database
            client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")

            # 2. Products Analytics Table (ReplacingMergeTree for deduplication + Bloom Filter skip index)
            client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.products_analytics (
                id UInt64,
                tenant_id LowCardinality(String),
                name String,
                category LowCardinality(String),
                price Float64,
                stock UInt32,
                store_id UInt32,
                updated_at DateTime DEFAULT now(),
                INDEX idx_store_id (store_id) TYPE bloom_filter(0.01) GRANULARITY 1
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (tenant_id, id);
            """)

            # 3. Orders Analytics Table (Bloom Filter on user_id for fast customer segmentation)
            client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.orders_analytics (
                id String,
                tenant_id LowCardinality(String),
                user_id UInt64,
                total_amount Float64,
                status LowCardinality(String),
                created_at DateTime DEFAULT now(),
                INDEX idx_user_id (user_id) TYPE bloom_filter(0.01) GRANULARITY 1
            ) ENGINE = ReplacingMergeTree(created_at)
            ORDER BY (tenant_id, created_at, id);
            """)

            # 4. Order Items Analytics Table (Bloom Filter on product_id for fast item lookups)
            client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.order_items_analytics (
                id String,
                order_id String,
                tenant_id LowCardinality(String),
                product_id UInt64,
                product_name String,
                category LowCardinality(String),
                unit_price Float64,
                quantity UInt32,
                created_at DateTime DEFAULT now(),
                INDEX idx_product_id (product_id) TYPE bloom_filter(0.01) GRANULARITY 1
            ) ENGINE = ReplacingMergeTree(created_at)
            ORDER BY (tenant_id, order_id, product_id);
            """)

            # 5. Payments Analytics Table (Bloom Filter on transaction_id for audit tracking)
            client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.payments_analytics (
                id String,
                order_id String,
                tenant_id LowCardinality(String),
                amount Float64,
                status LowCardinality(String),
                payment_method LowCardinality(String),
                transaction_id String,
                created_at DateTime DEFAULT now(),
                INDEX idx_transaction_id (transaction_id) TYPE bloom_filter(0.01) GRANULARITY 1
            ) ENGINE = ReplacingMergeTree(created_at)
            ORDER BY (tenant_id, order_id, id);
            """)

            logger.info("Successfully verified and initialized all ClickHouse ReplacingMergeTree tables with Bloom Filter skip indexes.")
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse tables: {e}", exc_info=True)

    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Executes a read-only SQL query and returns a list of dictionaries"""
        tracer = trace.get_tracer("merchant-copilot-service")
        with tracer.start_as_current_span("ClickHouse: execute_query") as span:
            span.set_attribute("db.system", "clickhouse")
            span.set_attribute("db.name", self.database)
            span.set_attribute("db.statement", query)
            span.set_attribute("db.operation", "SELECT")

            client = self.get_client()
            if not client:
                logger.warning("ClickHouse client offline. Returning empty query result.")
                return []

            try:
                # Enforce execution timeout
                result = client.query(query, parameters=parameters or {}, settings={"max_execution_time": 10})
                columns = result.column_names
                rows = []
                for row in result.result_rows:
                    row_dict = {}
                    for col, val in zip(columns, row):
                        row_dict[col] = val
                    rows.append(row_dict)
                span.set_attribute("db.rows_returned", len(rows))
                return rows
            except Exception as e:
                span.record_exception(e)
                logger.warning(f"ClickHouse query execution failed: {e}")
                raise

    def insert_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Inserts a batch of rows into ClickHouse in a single vectorized call"""
        if not rows:
            return 0
        tracer = trace.get_tracer("merchant-copilot-service")
        with tracer.start_as_current_span("ClickHouse: insert_batch") as span:
            span.set_attribute("db.system", "clickhouse")
            span.set_attribute("db.name", self.database)
            span.set_attribute("db.sql.table", table_name)
            span.set_attribute("db.operation", "INSERT")
            span.set_attribute("db.record_count", len(rows))

            client = self.get_client()
            if not client:
                return 0

            try:
                column_names = list(rows[0].keys())
                data = [[r.get(col) for col in column_names] for r in rows]
                client.insert(f"{self.database}.{table_name}", data, column_names=column_names)
                return len(rows)
            except Exception as e:
                span.record_exception(e)
                logger.error(f"Batch insert into ClickHouse table '{table_name}' failed: {e}")
                raise


clickhouse_client = ClickHouseClient()
