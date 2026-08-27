import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PORT: int = int(os.getenv("PORT", "8010"))
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "merchant-copilot-service")
    
    # ClickHouse OLAP Database Configuration
    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_DB: str = os.getenv("CLICKHOUSE_DB", "copilot_analytics")
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123")
    
    # Qdrant Vector Store
    QDRANT_HOST: str = os.getenv("QDRANT_HOST", "qdrant")
    QDRANT_PORT: int = int(os.getenv("QDRANT_PORT", "6333"))
    
    # Microservice Endpoints
    PRODUCT_SERVICE_URL: str = os.getenv("PRODUCT_SERVICE_URL", "http://product-service:8002")
    ORDER_SERVICE_URL: str = os.getenv("ORDER_SERVICE_URL", "http://order-service:8003")
    PAYMENT_SERVICE_URL: str = os.getenv("PAYMENT_SERVICE_URL", "http://payment-service:8004")
    API_GATEWAY_URL: str = os.getenv("API_GATEWAY_URL", "http://traefik-master:80")
    
    # Kafka Messaging
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    
    # OpenTelemetry Exporter
    OTEL_EXPORTER_OTLP_ENDPOINT: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    
    # LLM Settings (OpenAI / OpenRouter)
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    class Config:
        env_file = ".env"
        extra = "allow"

settings = Settings()
