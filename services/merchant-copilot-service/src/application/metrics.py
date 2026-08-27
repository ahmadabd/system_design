from prometheus_client import Counter, Histogram

# Metrics for Merchant Copilot
copilot_requests_total = Counter(
    "merchant_copilot_requests_total",
    "Total requests processed by Merchant Copilot",
    ["intent", "status", "tenant_id"]
)

copilot_node_duration_seconds = Histogram(
    "merchant_copilot_node_duration_seconds",
    "Duration of individual LangGraph node executions in seconds",
    ["node_name"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

sql_self_corrections_total = Counter(
    "merchant_copilot_sql_self_corrections_total",
    "Total number of SQL self-correction healing loops triggered",
    ["reason", "tenant_id"]
)

clickhouse_query_duration_seconds = Histogram(
    "clickhouse_query_duration_seconds",
    "Duration of ClickHouse OLAP query execution in seconds",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)
