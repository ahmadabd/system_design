"""
MCP Prompts for E-Commerce Platform
Provides reusable prompt templates to guide AI agents through common e-commerce workflows
with Prometheus metrics and OpenTelemetry tracing.
"""
from opentelemetry import trace
from src.application.metrics import mcp_prompt_requests_total

tracer = trace.get_tracer("mcp-server")


def register_prompts(mcp):
    """Registers all MCP prompt templates on the MCPServer instance."""

    @mcp.prompt("shopping_assistant")
    def shopping_assistant_workflow(customer_name: str = "Customer", tenant_id: str = "store_tech") -> str:
        """Prompt template guiding the AI agent to assist a customer with discovering products and completing checkouts."""
        with tracer.start_as_current_span("MCP prompt: shopping_assistant") as span:
            span.set_attribute("mcp.prompt_name", "shopping_assistant")
            span.set_attribute("tenant.id", tenant_id)
            span.set_attribute("mcp.customer_name", customer_name)
            mcp_prompt_requests_total.labels(prompt_name="shopping_assistant").inc()

            return f"""You are an intelligent, courteous E-Commerce Shopping Assistant for store tenant '{tenant_id}'.
Your goal is to assist {customer_name} in finding products, checking inventory, and securely placing orders.

Follow these strict operational guidelines:
1. When the user asks for recommendations, use `list_products` or `get_product_details` to verify product information, pricing, and stock.
2. ALWAYS check stock availability before recommending or placing an order. If `in_stock` is false, inform the customer immediately.
3. When the user confirms they want to buy, call `create_order` with their `user_id`, `product_id`, `quantity`, and calculated `total_price`.
4. Inform the customer that their order has been submitted in `PENDING` status and that the decentralized Saga will process payment and reservation asynchronously.
5. Provide the generated Order ID and idempotency key for their reference.
"""

    @mcp.prompt("order_troubleshooting")
    def order_troubleshooting_workflow(order_id: int, customer_issue: str = "Delay") -> str:
        """Prompt template guiding the AI agent to troubleshoot order issues or process cancellations."""
        with tracer.start_as_current_span("MCP prompt: order_troubleshooting") as span:
            span.set_attribute("mcp.prompt_name", "order_troubleshooting")
            span.set_attribute("mcp.order_id", order_id)
            span.set_attribute("mcp.issue", customer_issue)
            mcp_prompt_requests_total.labels(prompt_name="order_troubleshooting").inc()

            return f"""You are a dedicated Customer Support Resolver assisting with Order #{order_id}.
Reported Issue: {customer_issue}

Follow these operational steps:
1. Immediately invoke `get_order_status` for order_id={order_id} to inspect the real-time status.
2. Read the `ecommerce://policies/returns` resource to review cancellation eligibility.
3. If the status is `PENDING` or `CONFIRMED` and the customer wants a refund, confirm with them before invoking `cancel_order`.
4. If the status is `SHIPPED` or `DELIVERED`, explain that cancellation is no longer possible via automated API and explain the physical return process.
5. Remain polite, transparent, and empathetic at all times.
"""
