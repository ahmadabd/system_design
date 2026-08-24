"""
Main entrypoint for E-Commerce Model Context Protocol (MCP) microservice.
Supports SSE/HTTP transport for microservice deployment and stdio transport for local AI development.
"""
import logging
import sys
from src.infrastructure.config import settings
from src.infrastructure.observability import setup_mcp_observability
from src.server import create_mcp_server

logger = logging.getLogger("MCPMain")


def main():
    setup_mcp_observability(settings.SERVICE_NAME)
    logger.info(
        f"Starting E-Commerce MCP Service [{settings.SERVICE_NAME}] "
        f"in mode: '{settings.TRANSPORT_MODE}' (Environment: {settings.ENVIRONMENT})"
    )

    server = create_mcp_server()

    if settings.TRANSPORT_MODE == "stdio":
        logger.info("Executing MCP Server on standard I/O (stdio) transport for local AI agent host...")
        server.run(transport="stdio")
    else:
        logger.info(
            f"Executing MCP Server on SSE/HTTP transport at http://{settings.HOST}:{settings.PORT}/sse "
            f"(Targeting API Gateway: {settings.API_GATEWAY_URL})"
        )
        server.run(
            transport="sse",
            host=settings.HOST,
            port=settings.PORT,
            sse_path="/sse",
            message_path="/messages/"
        )


if __name__ == "__main__":
    main()
