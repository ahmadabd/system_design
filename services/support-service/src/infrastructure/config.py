import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class SupportSettings(BaseSettings):
    """Configuration settings for Support Service loaded from environment variables"""
    PORT: int = 8007
    ENVIRONMENT: str = "production"
    SERVICE_NAME: str = "support-service"

    # OpenRouter LLM Settings
    OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
    OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"
    OPENROUTER_MODEL: str = "nvidia/nemotron-3.5-lightning:free"
    LLM_TEMPERATURE: float = 0.2
    LLM_MAX_TOKENS: int = 1024
    LLM_REQUEST_TIMEOUT: float = 30.0

    # Qdrant Vector Store Settings
    QDRANT_URL: str = os.getenv("QDRANT_URL", "http://qdrant:6333")
    QDRANT_COLLECTION_NAME: str = "ecommerce_support_knowledge_base"
    EMBEDDING_MODEL_NAME: str = "BAAI/bge-small-en-v1.5"
    EMBEDDING_VECTOR_SIZE: int = 384
    KNOWLEDGE_BASE_DIR: str = os.getenv("KNOWLEDGE_BASE_DIR", "/app/knowledge_base")

    # Hybrid Search & Cross-Encoder Reranking Settings
    RERANKER_MODEL_NAME: str = "ms-marco-TinyBERT-L-2-v2"
    HYBRID_DENSE_K: int = 10
    HYBRID_SPARSE_K: int = 10
    RERANK_TOP_K: int = 3
    RRF_K_CONSTANT: int = 60


    # Infrastructure & Observability
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://:sys_design_secure_cache_pass_2026@redis:6379")
    OTEL_EXPORTER_OTLP_ENDPOINT: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    # Downstream Microservices
    ORDER_SERVICE_URL: str = os.getenv("ORDER_SERVICE_URL", "http://order-service:8003")
    PRODUCT_SERVICE_URL: str = os.getenv("PRODUCT_SERVICE_URL", "http://product-service:8002")
    USER_SERVICE_URL: str = os.getenv("USER_SERVICE_URL", "http://user-service:8001")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = SupportSettings()
