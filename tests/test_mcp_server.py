import pytest
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure services/mcp-server is importable
sys.path.insert(0, os.path.abspath("services/mcp-server"))

from src.server import create_mcp_server
from src.adapter.gateway_adapter import GatewayAdapter
from shared.common.resilience import CircuitBreakerOpenException


@pytest.fixture
def mock_gateway_adapter():
    """Provides a mocked GatewayAdapter with predefined successful responses."""
    adapter = MagicMock(spec=GatewayAdapter)

    # Mock register_user
    adapter.register_user = AsyncMock(return_value={
        "success": True,
        "status": "created",
        "data": {"id": 42, "username": "ai_shopper", "email": "ai_shopper@example.com"},
        "idempotency_key": "mcp-reg-test-123"
    })

    # Mock get_user_profile
    adapter.get_user_profile = AsyncMock(return_value={
        "success": True,
        "status": "found",
        "data": {"id": 42, "username": "ai_shopper", "email": "ai_shopper@example.com"}
    })

    # Mock list_products
    adapter.list_products = AsyncMock(return_value={
        "success": True,
        "status": "success",
        "data": [
            {"id": 1, "name": "Mechanical Keyboard", "price": 99.99, "stock": 15, "store_id": 1},
            {"id": 2, "name": "Wireless Mouse", "price": 49.99, "stock": 0, "store_id": 1}
        ]
    })

    # Mock get_product
    adapter.get_product = AsyncMock(return_value={
        "success": True,
        "status": "found",
        "data": {"id": 1, "name": "Mechanical Keyboard", "price": 99.99, "stock": 15, "store_id": 1},
        "in_stock": True
    })

    # Mock create_order
    adapter.create_order = AsyncMock(return_value={
        "success": True,
        "status": "submitted",
        "data": {
            "id": 101,
            "user_id": 42,
            "product_id": 1,
            "quantity": 2,
            "total_price": 199.98,
            "status": "PENDING",
            "store_id": 1
        },
        "idempotency_key": "mcp-ord-test-456",
        "message": "Order created in PENDING state. Asynchronous Saga initiated."
    })

    # Mock get_order_status
    adapter.get_order_status = AsyncMock(return_value={
        "success": True,
        "status": "found",
        "data": {
            "id": 101,
            "status": "CONFIRMED",
            "total_price": 199.98
        }
    })

    # Mock cancel_order
    adapter.cancel_order = AsyncMock(return_value={
        "success": True,
        "status": "cancelled",
        "data": {"id": 101, "status": "CANCELLED"},
        "message": "Order #101 has been cancelled successfully. Refund initiated."
    })

    return adapter


@pytest.fixture
def mcp_server(mock_gateway_adapter):
    """Initializes an MCP server with the mocked adapter."""
    return create_mcp_server(adapter=mock_gateway_adapter)


@pytest.mark.asyncio
async def test_mcp_server_capability_discovery(mcp_server):
    """Verify that all tools, resources, and prompts are properly registered."""
    tools = await mcp_server.list_tools()
    tool_names = [t.name for t in tools]
    assert "register_user" in tool_names
    assert "get_user_profile" in tool_names
    assert "list_products" in tool_names
    assert "get_product_details" in tool_names
    assert "create_order" in tool_names
    assert "get_order_status" in tool_names
    assert "cancel_order" in tool_names

    resources = await mcp_server.list_resources()
    resource_uris = [str(r.uri) for r in resources]
    assert "ecommerce://policies/returns" in resource_uris
    assert "ecommerce://policies/shipping" in resource_uris
    assert "ecommerce://support/faq" in resource_uris

    prompts = await mcp_server.list_prompts()
    prompt_names = [p.name for p in prompts]
    assert "shopping_assistant" in prompt_names
    assert "order_troubleshooting" in prompt_names


@pytest.mark.asyncio
async def test_mcp_register_user_tool(mcp_server, mock_gateway_adapter):
    """Test user registration tool execution."""
    result = await mcp_server.call_tool("register_user", {
        "username": "ai_shopper",
        "email": "ai_shopper@example.com",
        "password": "securepassword123",
        "tenant_id": "public"
    })
    assert not result.is_error
    data = result.structured_content
    assert data["result"]["success"] is True
    assert data["result"]["data"]["username"] == "ai_shopper"
    assert data["result"]["idempotency_key"] == "mcp-reg-test-123"

    mock_gateway_adapter.register_user.assert_called_once_with(
        username="ai_shopper",
        email="ai_shopper@example.com",
        password="securepassword123",
        tenant_id="public"
    )


@pytest.mark.asyncio
async def test_mcp_catalog_and_stock_tools(mcp_server, mock_gateway_adapter):
    """Test product catalog listing and stock verification tools."""
    # List products
    res_list = await mcp_server.call_tool("list_products", {"tenant_id": "store_tech"})
    assert not res_list.is_error
    assert len(res_list.structured_content["result"]["data"]) == 2

    # Get details
    res_detail = await mcp_server.call_tool("get_product_details", {
        "product_id": 1,
        "tenant_id": "store_tech"
    })
    assert not res_detail.is_error
    assert res_detail.structured_content["result"]["in_stock"] is True
    assert res_detail.structured_content["result"]["data"]["price"] == 99.99


@pytest.mark.asyncio
async def test_mcp_create_order_saga_tool(mcp_server, mock_gateway_adapter):
    """Test order creation initiating Kafka Saga."""
    result = await mcp_server.call_tool("create_order", {
        "user_id": 42,
        "product_id": 1,
        "quantity": 2,
        "total_price": 199.98,
        "store_id": 1,
        "payment_method": "CREDIT_CARD",
        "tenant_id": "store_tech"
    })
    assert not result.is_error
    order_res = result.structured_content["result"]
    assert order_res["success"] is True
    assert order_res["status"] == "submitted"
    assert order_res["data"]["status"] == "PENDING"
    assert order_res["idempotency_key"] == "mcp-ord-test-456"


@pytest.mark.asyncio
async def test_mcp_order_tracking_and_cancellation(mcp_server, mock_gateway_adapter):
    """Test tracking order and cancelling with Saga compensation."""
    # Track order
    res_track = await mcp_server.call_tool("get_order_status", {
        "order_id": 101,
        "tenant_id": "store_tech"
    })
    assert not res_track.is_error
    assert res_track.structured_content["result"]["data"]["status"] == "CONFIRMED"

    # Cancel order
    res_cancel = await mcp_server.call_tool("cancel_order", {
        "order_id": 101,
        "reason": "Found better deal",
        "tenant_id": "store_tech"
    })
    assert not res_cancel.is_error
    cancel_res = result = res_cancel.structured_content["result"]
    assert cancel_res["success"] is True
    assert cancel_res["status"] == "cancelled"


@pytest.mark.asyncio
async def test_mcp_circuit_breaker_resilience():
    """Verify tool behavior when downstream circuit breaker trips."""
    real_adapter = GatewayAdapter()

    # Mock client to raise CircuitBreakerOpenException
    real_adapter.client = MagicMock()
    real_adapter.client.post = AsyncMock(side_effect=CircuitBreakerOpenException("Host 'order-service' circuit breaker is OPEN"))

    res = await real_adapter.create_order(
        user_id=1,
        product_id=1,
        quantity=1,
        total_price=50.0
    )
    assert res["success"] is False
    assert res["status"] == "degraded"
    assert "degraded" in res["message"].lower()


@pytest.mark.asyncio
async def test_mcp_resources(mcp_server):
    """Test reading MCP contextual resources."""
    content = await mcp_server.read_resource("ecommerce://policies/returns")
    assert len(content) > 0
    assert "Store Return & Refund Policy" in content[0].content


@pytest.mark.asyncio
async def test_mcp_prompts(mcp_server):
    """Test rendering MCP prompt templates."""
    prompt_res = await mcp_server.get_prompt("shopping_assistant", {"customer_name": "Alice", "tenant_id": "store_tech"})
    assert prompt_res is not None
    assert "Alice" in prompt_res.messages[0].content.text
    assert "store_tech" in prompt_res.messages[0].content.text


@pytest.mark.asyncio
async def test_mcp_observability_endpoints(mcp_server):
    """Test that /metrics and /health endpoints are exposed and return valid telemetry data."""
    from starlette.testclient import TestClient
    from prometheus_client.parser import text_string_to_metric_families

    # 1. Execute a tool call to populate metrics
    await mcp_server.call_tool("list_products", {"tenant_id": "store_tech"})

    # 2. Query SSE Starlette app HTTP endpoints
    app = mcp_server.sse_app()
    with TestClient(app) as client:
        # Test health endpoint
        health_resp = client.get("/health")
        assert health_resp.status_code == 200
        assert health_resp.json()["status"] == "healthy"
        assert health_resp.json()["service"] == "mcp-service"

        # Test Prometheus metrics endpoint
        metrics_resp = client.get("/metrics")
        assert metrics_resp.status_code == 200
        assert "mcp_tool_calls_total" in metrics_resp.text
        assert "mcp_tool_duration_seconds" in metrics_resp.text
        assert "mcp_resource_reads_total" in metrics_resp.text


@pytest.mark.asyncio
async def test_mcp_opentelemetry_tracing(mcp_server):
    """Verify that MCP tool invocations generate valid OpenTelemetry spans for Jaeger."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from opentelemetry import trace

    # Setup in-memory tracer to capture spans
    memory_exporter = InMemorySpanExporter()
    test_tracer_provider = TracerProvider()
    test_tracer_provider.add_span_processor(SimpleSpanProcessor(memory_exporter))
    trace.set_tracer_provider(test_tracer_provider)

    # Invoke tool
    await mcp_server.call_tool("create_order", {
        "user_id": 99,
        "product_id": 5,
        "quantity": 1,
        "total_price": 49.99,
        "tenant_id": "store_gaming"
    })

    # Assert captured spans
    spans = memory_exporter.get_finished_spans()
    span_names = [s.name for s in spans]
    assert "MCP tool: create_order" in span_names

    order_span = next(s for s in spans if s.name == "MCP tool: create_order")
    assert order_span.attributes["mcp.tool_name"] == "create_order"
    assert order_span.attributes["tenant.id"] == "store_gaming"
    assert order_span.attributes["mcp.user_id"] == 99
    assert order_span.attributes["mcp.product_id"] == 5
