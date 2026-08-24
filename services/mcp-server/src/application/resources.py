"""
MCP Resources for E-Commerce Platform
Provides static and semi-static read-only context (policies, FAQs, catalogs)
with Prometheus metrics and OpenTelemetry tracing.
"""
from opentelemetry import trace
from src.application.metrics import mcp_resource_reads_total

tracer = trace.get_tracer("mcp-server")


def register_resources(mcp):
    """Registers all MCP resources on the MCPServer instance."""

    @mcp.resource("ecommerce://policies/returns")
    def return_and_refund_policy() -> str:
        """Official store cancellation, return, and refund policies."""
        with tracer.start_as_current_span("MCP resource: returns_policy") as span:
            span.set_attribute("mcp.resource_uri", "ecommerce://policies/returns")
            mcp_resource_reads_total.labels(resource_uri="ecommerce://policies/returns").inc()
            return """# Store Return & Refund Policy

1. **Order Cancellation**:
   - Customers or their AI agents can cancel any order while it is in `PENDING` or `CONFIRMED` state.
   - Once an order enters `SHIPPED` status, cancellation is no longer possible through the automated API; a physical return must be initiated upon delivery.

2. **Refund Processing**:
   - When an order is cancelled, the automated Choreographed Saga immediately initiates a payment reversal.
   - Credit card and wallet refunds are completed within 1-3 business days.

3. **Damaged / Defective Items**:
   - Customers may report defective items within 30 days of delivery for a 100% money-back guarantee.
"""

    @mcp.resource("ecommerce://policies/shipping")
    def shipping_policy() -> str:
        """Official delivery service level agreements (SLAs) and shipping options."""
        with tracer.start_as_current_span("MCP resource: shipping_policy") as span:
            span.set_attribute("mcp.resource_uri", "ecommerce://policies/shipping")
            mcp_resource_reads_total.labels(resource_uri="ecommerce://policies/shipping").inc()
            return """# Shipping & Delivery Guidelines

1. **Standard Shipping**: 3-5 business days. Free on orders above $50.
2. **Express Shipping**: 1-2 business days with real-time package tracking.
3. **Multi-Tenant Fulfillment**: Orders containing items from different store tenants (e.g., `store_tech`, `store_fashion`) are packaged and dispatched independently by their respective merchant facilities.
"""

    @mcp.resource("ecommerce://support/faq")
    def customer_faq() -> str:
        """Frequently asked questions for customer assistance."""
        with tracer.start_as_current_span("MCP resource: customer_faq") as span:
            span.set_attribute("mcp.resource_uri", "ecommerce://support/faq")
            mcp_resource_reads_total.labels(resource_uri="ecommerce://support/faq").inc()
            return """# Customer Support FAQ

Q: How can I track my order?
A: Ask your AI agent to call the `get_order_status` tool with your numeric Order ID.

Q: What payment methods are supported?
A: We support CREDIT_CARD, WALLET, and CASH_ON_DELIVERY.

Q: Can I change the quantity of an order after placing it?
A: You cannot edit an active order directly. You must cancel the current order using `cancel_order` and place a new one with the updated quantity.
"""
