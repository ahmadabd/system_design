"""
Server initialization module for E-Commerce Model Context Protocol (MCP).
Exposes /metrics and /health custom endpoints for Prometheus and load-balancer health checks.
"""
import logging
from mcp.server import MCPServer
from starlette.responses import Response, JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.application.tools import register_tools
from src.application.resources import register_resources
from src.application.prompts import register_prompts
from src.adapter.gateway_adapter import gateway_adapter

logger = logging.getLogger("EcommerceMCPServer")


def create_mcp_server(adapter=gateway_adapter) -> MCPServer:
    """
    Factory function creating and configuring a full-featured MCPServer instance.
    Registers domain tools, contextual resources, prompt templates, and Prometheus observability endpoints.
    """
    server = MCPServer(
        name="EcommercePlatformMCP",
        version="1.0.0",
        instructions="""This MCP server connects AI agents to a production-grade multi-tenant e-commerce platform.
Agents can manage customer accounts, browse catalogs with real-time inventory, place orders backed by decentralized Sagas,
track order states, and request cancellations with automatic refunds."""
    )

    # Register capabilities
    register_tools(server, adapter=adapter)
    register_resources(server)
    register_prompts(server)

    # Expose Prometheus /metrics endpoint for scraping
    @server.custom_route("/metrics", methods=["GET"])
    async def prometheus_metrics_endpoint(request):
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # Expose /health endpoint for Gateway / Traefik health checking
    @server.custom_route("/health", methods=["GET"])
    async def health_check_endpoint(request):
        return JSONResponse({"status": "healthy", "service": "mcp-service"})

    logger.info("Configured EcommercePlatformMCP server with tools, resources, prompts, /metrics and /health.")
    return server
