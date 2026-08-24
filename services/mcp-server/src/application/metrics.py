"""
Prometheus metrics definition for the E-Commerce Model Context Protocol (MCP) service.
"""
from prometheus_client import Counter, Histogram, Gauge

# Counter tracking total tool invocations
mcp_tool_calls_total = Counter(
    "mcp_tool_calls_total",
    "Total number of MCP tool invocations by AI agents",
    ["tool_name", "status", "tenant_id"]
)

# Histogram tracking tool execution latency
mcp_tool_duration_seconds = Histogram(
    "mcp_tool_duration_seconds",
    "Latency of MCP tool execution in seconds",
    ["tool_name"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

# Counter tracking resource reads (e.g. policies, catalog context)
mcp_resource_reads_total = Counter(
    "mcp_resource_reads_total",
    "Total number of MCP resource read requests",
    ["resource_uri"]
)

# Counter tracking prompt template fetches
mcp_prompt_requests_total = Counter(
    "mcp_prompt_requests_total",
    "Total number of MCP prompt template requests",
    ["prompt_name"]
)

# Counter tracking degraded/tripped circuit breaker encounters
mcp_circuit_breaker_trips_total = Counter(
    "mcp_circuit_breaker_trips_total",
    "Total number of MCP tool requests degraded due to open circuit breakers",
    ["tool_name"]
)
