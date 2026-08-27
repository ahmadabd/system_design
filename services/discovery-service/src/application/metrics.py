"""
Prometheus Metrics definitions for Discovery Service.
"""
from prometheus_client import Counter, Histogram

# 1. Request throughput counter
discovery_requests_total = Counter(
    "discovery_requests_total",
    "Total number of product discovery and bundle requests",
    ["request_type", "tenant_id", "status"]
)

# 2. End-to-end and per-node execution latency
discovery_node_duration_seconds = Histogram(
    "discovery_node_duration_seconds",
    "Latency of individual LangGraph node execution in seconds",
    ["node_name"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# 3. Vector search duration in Qdrant
qdrant_search_duration_seconds = Histogram(
    "qdrant_search_duration_seconds",
    "Latency of payload-filtered Qdrant vector searches in seconds",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

# 4. Bundle items generated gauge/histogram
discovery_bundle_items_count = Histogram(
    "discovery_bundle_items_count",
    "Number of products included in generated bundles",
    buckets=[1, 2, 3, 4, 5, 8, 10]
)

# 5. Circuit breaker trips counter
discovery_circuit_breaker_trips_total = Counter(
    "discovery_circuit_breaker_trips_total",
    "Total number of discovery requests degraded due to open circuit breakers",
    ["target_resource"]
)
