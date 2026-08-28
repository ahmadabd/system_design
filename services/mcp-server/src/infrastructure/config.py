import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class MCPSettings(BaseSettings):
    """Configuration settings for the MCP microservice."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SERVICE_NAME: str = "mcp-service"
    PORT: int = 8008
    HOST: str = "0.0.0.0"
    ENVIRONMENT: str = "production"

    # Gateway & microservice connection settings
    API_GATEWAY_URL: str = os.getenv("API_GATEWAY_URL", "http://localhost")
    USER_SERVICE_URL: str = os.getenv("USER_SERVICE_URL", "http://localhost/users")
    PRODUCT_SERVICE_URL: str = os.getenv("PRODUCT_SERVICE_URL", "http://localhost/products")
    ORDER_SERVICE_URL: str = os.getenv("ORDER_SERVICE_URL", "http://localhost/orders")
    PAYMENT_SERVICE_URL: str = os.getenv("PAYMENT_SERVICE_URL", "http://localhost/payments")
    REPORTING_SERVICE_URL: str = os.getenv("REPORTING_SERVICE_URL", "http://localhost/reporting")
    DISCOVERY_SERVICE_URL: str = os.getenv("DISCOVERY_SERVICE_URL", "http://localhost/discovery")
    COPILOT_SERVICE_URL: str = os.getenv("COPILOT_SERVICE_URL", "http://localhost/copilot")
    GRAPHRAG_SERVICE_URL: str = os.getenv("GRAPHRAG_SERVICE_URL", "http://localhost/graphrag")
    DISPUTE_SERVICE_URL: str = os.getenv("DISPUTE_SERVICE_URL", "http://localhost/disputes")

    # Multi-tenancy
    DEFAULT_TENANT: str = "store_tech"

    # Transport mode: "sse" (HTTP network) or "stdio" (local IDE)
    TRANSPORT_MODE: str = os.getenv("TRANSPORT_MODE", "sse")

    # Observability & Redis
    OTEL_EXPORTER_OTLP_ENDPOINT: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")


settings = MCPSettings()
