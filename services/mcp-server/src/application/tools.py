"""
MCP Tools for E-Commerce Platform
Defines the executable tools exposed to AI agents with schema validation,
resilience decorators, idempotency keys, multi-tenancy support, Prometheus metrics,
and OpenTelemetry distributed tracing for Jaeger waterfalls.
"""
import logging
import time
from typing import Dict, Any, Optional
from pydantic import Field, EmailStr
from opentelemetry import trace

from src.adapter.gateway_adapter import gateway_adapter
from src.application.metrics import (
    mcp_tool_calls_total,
    mcp_tool_duration_seconds,
    mcp_circuit_breaker_trips_total
)

logger = logging.getLogger("MCPTools")
tracer = trace.get_tracer("mcp-server")


def register_tools(mcp, adapter=gateway_adapter):
    """Registers all operational e-commerce tools on the MCPServer instance."""

    # =========================================================================
    # 1. User Account & Identity Tools
    # =========================================================================

    @mcp.tool()
    async def register_user(
        username: str = Field(..., description="Unique username for the new account (alphanumeric and underscores)"),
        email: EmailStr = Field(..., description="Customer valid email address for account identity and order receipts"),
        password: str = Field(..., min_length=6, description="Account password (minimum 6 characters)"),
        tenant_id: str = Field(default="public", description="Multi-tenant store context (default: 'public')")
    ) -> Dict[str, Any]:
        """
        Register a new customer user account on the e-commerce platform.
        Generates an idempotency key to prevent accidental duplicate account registrations.
        """
        with tracer.start_as_current_span("MCP tool: register_user") as span:
            span.set_attribute("mcp.tool_name", "register_user")
            span.set_attribute("mcp.username", username)
            span.set_attribute("mcp.email", str(email))
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: register_user] Registering user={username}, email={email}")
            result = await adapter.register_user(
                username=username,
                email=email,
                password=password,
                tenant_id=tenant_id
            )
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))
            if "idempotency_key" in result:
                span.set_attribute("mcp.idempotency_key", result["idempotency_key"])

            mcp_tool_calls_total.labels(tool_name="register_user", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="register_user").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="register_user").inc()
            return result

    @mcp.tool()
    async def get_user_profile(
        user_id: int = Field(..., description="Numeric customer identity ID"),
        tenant_id: str = Field(default="public", description="Store tenant identifier (default: 'public')")
    ) -> Dict[str, Any]:
        """
        Retrieve profile information for an existing customer by their numeric User ID.
        """
        with tracer.start_as_current_span("MCP tool: get_user_profile") as span:
            span.set_attribute("mcp.tool_name", "get_user_profile")
            span.set_attribute("mcp.user_id", user_id)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: get_user_profile] Fetching profile for user_id={user_id}")
            result = await adapter.get_user_profile(user_id=user_id, tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="get_user_profile", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="get_user_profile").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="get_user_profile").inc()
            return result

    # =========================================================================
    # 2. Product Catalog & Inventory Tools
    # =========================================================================

    @mcp.tool()
    async def list_products(
        tenant_id: str = Field(default="store_tech", description="Store tenant identifier to list catalog from (e.g. 'store_tech')")
    ) -> Dict[str, Any]:
        """
        List all available products in the store's catalog including prices, stock levels, and store IDs.
        """
        with tracer.start_as_current_span("MCP tool: list_products") as span:
            span.set_attribute("mcp.tool_name", "list_products")
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: list_products] Listing catalog for tenant={tenant_id}")
            result = await adapter.list_products(tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="list_products", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="list_products").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="list_products").inc()
            return result

    @mcp.tool()
    async def get_product_details(
        product_id: int = Field(..., description="Numeric ID of the product in the catalog"),
        tenant_id: str = Field(default="store_tech", description="Store tenant identifier (e.g. 'store_tech')")
    ) -> Dict[str, Any]:
        """
        Fetch real-time details, price, and current stock level for a specific product.
        Returns `in_stock: true/false` to help agents determine purchase eligibility.
        """
        with tracer.start_as_current_span("MCP tool: get_product_details") as span:
            span.set_attribute("mcp.tool_name", "get_product_details")
            span.set_attribute("mcp.product_id", product_id)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: get_product_details] Fetching product_id={product_id}, tenant={tenant_id}")
            result = await adapter.get_product(product_id=product_id, tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))
            if "in_stock" in result:
                span.set_attribute("mcp.in_stock", result["in_stock"])

            mcp_tool_calls_total.labels(tool_name="get_product_details", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="get_product_details").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="get_product_details").inc()
            return result

    # =========================================================================
    # 3. Order Lifecycle & Saga Workflow Tools
    # =========================================================================

    @mcp.tool()
    async def create_order(
        user_id: int = Field(..., description="Numeric customer ID placing the order"),
        product_id: int = Field(..., description="Numeric product ID to purchase"),
        quantity: int = Field(..., gt=0, description="Quantity of units to purchase (must be greater than 0)"),
        total_price: float = Field(..., gt=0, description="Calculated total price for the order"),
        store_id: int = Field(default=1, description="Numeric store ID fulfilling the order"),
        payment_method: str = Field(
            default="CREDIT_CARD",
            description="Payment method: 'CREDIT_CARD', 'WALLET', or 'CASH_ON_DELIVERY'"
        ),
        tenant_id: str = Field(default="store_tech", description="Store tenant context (e.g. 'store_tech')"),
        idempotency_key: Optional[str] = Field(
            default=None,
            description="Optional unique idempotency key. If omitted, one is generated automatically to prevent duplicate orders."
        )
    ) -> Dict[str, Any]:
        """
        Submit a new order. Creates an order in 'PENDING' status and initiates the
        asynchronous Kafka Choreographed Saga (inventory reservation and payment processing).
        Protected by Redis API Idempotency and Circuit Breakers.
        """
        with tracer.start_as_current_span("MCP tool: create_order") as span:
            span.set_attribute("mcp.tool_name", "create_order")
            span.set_attribute("mcp.user_id", user_id)
            span.set_attribute("mcp.product_id", product_id)
            span.set_attribute("mcp.quantity", quantity)
            span.set_attribute("mcp.total_price", total_price)
            span.set_attribute("mcp.payment_method", payment_method)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(
                f"[MCP Tool: create_order] user_id={user_id}, product_id={product_id}, qty={quantity}, total={total_price}"
            )
            result = await adapter.create_order(
                user_id=user_id,
                product_id=product_id,
                quantity=quantity,
                total_price=total_price,
                store_id=store_id,
                payment_method=payment_method,
                tenant_id=tenant_id,
                idempotency_key=idempotency_key
            )
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))
            if "idempotency_key" in result:
                span.set_attribute("mcp.idempotency_key", result["idempotency_key"])

            mcp_tool_calls_total.labels(tool_name="create_order", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="create_order").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="create_order").inc()
            return result

    @mcp.tool()
    async def get_order_status(
        order_id: int = Field(..., description="Numeric order ID to track"),
        tenant_id: str = Field(default="store_tech", description="Store tenant context")
    ) -> Dict[str, Any]:
        """
        Look up the real-time status and details of an order (e.g. PENDING, CONFIRMED, CANCELLED, SHIPPED).
        """
        with tracer.start_as_current_span("MCP tool: get_order_status") as span:
            span.set_attribute("mcp.tool_name", "get_order_status")
            span.set_attribute("mcp.order_id", order_id)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: get_order_status] Looking up order_id={order_id}")
            result = await adapter.get_order_status(order_id=order_id, tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="get_order_status", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="get_order_status").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="get_order_status").inc()
            return result

    @mcp.tool()
    async def cancel_order(
        order_id: int = Field(..., description="Numeric ID of the order to cancel"),
        reason: str = Field(
            default="Cancelled by customer via AI Agent",
            description="Customer or agent reason for order cancellation"
        ),
        tenant_id: str = Field(default="store_tech", description="Store tenant context")
    ) -> Dict[str, Any]:
        """
        Cancel an existing order and initiate automatic Saga compensation
        (releasing reserved stock and executing payment refund).
        """
        with tracer.start_as_current_span("MCP tool: cancel_order") as span:
            span.set_attribute("mcp.tool_name", "cancel_order")
            span.set_attribute("mcp.order_id", order_id)
            span.set_attribute("mcp.reason", reason)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: cancel_order] Cancelling order_id={order_id}, reason='{reason}'")
            result = await adapter.cancel_order(order_id=order_id, reason=reason, tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="cancel_order", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="cancel_order").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="cancel_order").inc()
            return result

    @mcp.tool()
    async def discover_product_bundle(
        query: str = Field(..., description="Natural language search or setup description (e.g. 'Coding desk setup with monitor and keyboard under $500')"),
        budget: Optional[float] = Field(default=None, description="Optional maximum price limit"),
        tenant_id: str = Field(default="store_tech", description="Store tenant context")
    ) -> Dict[str, Any]:
        """
        Use Advanced RAG (HyDE, Query Decomposition, Qdrant payload filtering, and LangGraph Knapsack optimization)
        to discover matching products and build price-optimized product bundles under budget constraints.
        """
        with tracer.start_as_current_span("MCP tool: discover_product_bundle") as span:
            span.set_attribute("mcp.tool_name", "discover_product_bundle")
            span.set_attribute("mcp.query", query)
            if budget:
                span.set_attribute("mcp.budget", budget)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: discover_product_bundle] query='{query}', budget={budget}, tenant={tenant_id}")
            result = await adapter.discover_bundle(query=query, budget=budget, tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="discover_product_bundle", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="discover_product_bundle").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="discover_product_bundle").inc()
            return result

    @mcp.tool(
        name="merchant_copilot_query",
        description="Query merchant analytical metrics (ClickHouse SQL) and store policies/SLAs (Qdrant Vector RAG) with self-correction and AST safety."
    )
    async def merchant_copilot_query(
        query: str = Field(..., description="Natural language business or policy question (e.g. 'Show total revenue for store_tech and return SLA')"),
        tenant_id: str = Field(default="store_tech", description="Store tenant context")
    ) -> Dict[str, Any]:
        """
        Execute Hybrid Text-to-SQL + Policy RAG via Merchant Copilot.
        Generates validated ClickHouse SQL and retrieves matching SLA policy guidelines.
        """
        with tracer.start_as_current_span("MCP tool: merchant_copilot_query") as span:
            span.set_attribute("mcp.tool_name", "merchant_copilot_query")
            span.set_attribute("mcp.query", query)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: merchant_copilot_query] query='{query}', tenant={tenant_id}")
            result = await adapter.query_merchant_copilot(query=query, tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="merchant_copilot_query", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="merchant_copilot_query").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="merchant_copilot_query").inc()
            return result

    @mcp.tool(
        name="graph_rag_query",
        description="Execute Microsoft GraphRAG inquiry: multi-hop causal reasoning across products, components, suppliers, batches, and defects with Louvain community detection."
    )
    async def graph_rag_query(
        query: str = Field(..., description="Natural language root-cause, defect, or supplier question (e.g. 'Why is the laptop overheating and which supplier is it?')"),
        search_mode: str = Field(default="auto", description="Search mode: 'auto', 'local_multihop', 'global_community'"),
        tenant_id: str = Field(default="store_tech", description="Store tenant context")
    ) -> Dict[str, Any]:
        """
        Execute Graph-Augmented RAG (GraphRAG) query.
        Traverses multi-hop subgraphs or aggregates hierarchical community summaries to explain root causes.
        """
        with tracer.start_as_current_span("MCP tool: graph_rag_query") as span:
            span.set_attribute("mcp.tool_name", "graph_rag_query")
            span.set_attribute("mcp.query", query)
            span.set_attribute("mcp.search_mode", search_mode)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: graph_rag_query] query='{query}', mode={search_mode}, tenant={tenant_id}")
            result = await adapter.query_graph_rag(query=query, search_mode=search_mode, tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="graph_rag_query", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="graph_rag_query").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="graph_rag_query").inc()
            return result

    # =========================================================================
    # 7. Dispute Resolution & Claims Arbitration Tools
    # =========================================================================

    @mcp.tool(
        name="submit_dispute_claim",
        description="Submit a customer dispute claim to trigger the Multi-Agent Negotiation Arena (Buyer Advocate vs Merchant Defender) with Self-RAG policy grounding and GraphRAG defect verification."
    )
    async def submit_dispute_claim(
        order_id: str = Field(..., description="Order identifier under dispute (e.g. 'ord-101')"),
        customer_id: int = Field(..., description="Customer account ID filing the dispute"),
        product_name: str = Field(..., description="Full product name / SKU description"),
        claim_amount: float = Field(..., gt=0, description="Dollar amount claimed for reimbursement"),
        reason: str = Field(..., description="Dispute category: 'DEFECTIVE_PRODUCT', 'BUYER_REMORSE', 'TRANSIT_DAMAGE', 'UNAUTHORIZED_TRANSACTION'"),
        customer_statement: str = Field(..., description="Customer's narrative describing the defect or issue"),
        delivery_days_ago: int = Field(default=5, ge=0, description="Number of calendar days elapsed since confirmed delivery"),
        idempotency_key: Optional[str] = Field(default=None, description="Optional idempotency key to prevent duplicate filings"),
        tenant_id: str = Field(default="store_tech", description="Store tenant context")
    ) -> Dict[str, Any]:
        """
        Submit a dispute claim for multi-agent arbitration.
        Returns adversarial debate arguments, judicial verdict, and supplier CAR notices.
        """
        with tracer.start_as_current_span("MCP tool: submit_dispute_claim") as span:
            span.set_attribute("mcp.tool_name", "submit_dispute_claim")
            span.set_attribute("mcp.order_id", order_id)
            span.set_attribute("mcp.reason", reason)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: submit_dispute_claim] order={order_id}, reason={reason}, amount={claim_amount}")
            result = await adapter.submit_dispute_claim(
                order_id=order_id,
                customer_id=customer_id,
                product_name=product_name,
                claim_amount=claim_amount,
                reason=reason,
                customer_statement=customer_statement,
                delivery_days_ago=delivery_days_ago,
                idempotency_key=idempotency_key,
                tenant_id=tenant_id
            )
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="submit_dispute_claim", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="submit_dispute_claim").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="submit_dispute_claim").inc()
            return result

    @mcp.tool(
        name="get_dispute_status",
        description="Retrieve full details, adversarial debate transcript, and arbitration outcome for an existing dispute claim."
    )
    async def get_dispute_status(
        claim_id: str = Field(..., description="Unique claim identifier (e.g. 'claim_5e968c81')"),
        tenant_id: str = Field(default="store_tech", description="Store tenant context")
    ) -> Dict[str, Any]:
        """Lookup dispute claim status and judicial rationale."""
        with tracer.start_as_current_span("MCP tool: get_dispute_status") as span:
            span.set_attribute("mcp.tool_name", "get_dispute_status")
            span.set_attribute("mcp.claim_id", claim_id)
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            logger.info(f"[MCP Tool: get_dispute_status] claim_id={claim_id}, tenant={tenant_id}")
            result = await adapter.get_dispute_claim(claim_id=claim_id, tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            span.set_attribute("mcp.status", status)
            span.set_attribute("mcp.success", result.get("success", False))

            mcp_tool_calls_total.labels(tool_name="get_dispute_status", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="get_dispute_status").observe(duration)
            if status == "degraded":
                mcp_circuit_breaker_trips_total.labels(tool_name="get_dispute_status").inc()
            return result

    @mcp.tool(
        name="get_dispute_statistics",
        description="Retrieve platform-wide dispute metrics including total claims, auto-settlement ratio, and refunded amounts."
    )
    async def get_dispute_statistics(
        tenant_id: str = Field(default="store_tech", description="Store tenant context")
    ) -> Dict[str, Any]:
        """Retrieve aggregated dispute and claim resolution metrics."""
        with tracer.start_as_current_span("MCP tool: get_dispute_statistics") as span:
            span.set_attribute("mcp.tool_name", "get_dispute_statistics")
            span.set_attribute("tenant.id", tenant_id)

            start = time.perf_counter()
            result = await adapter.get_dispute_stats(tenant_id=tenant_id)
            duration = time.perf_counter() - start
            status = result.get("status", "unknown")

            mcp_tool_calls_total.labels(tool_name="get_dispute_statistics", status=status, tenant_id=tenant_id).inc()
            mcp_tool_duration_seconds.labels(tool_name="get_dispute_statistics").observe(duration)
            return result

