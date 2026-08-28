import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SERVICE_NAME: str = "dispute-resolution-service"
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
    PORT: int = int(os.getenv("PORT", "8012"))

    # Redis Cache & Idempotency Store
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "sys_design_secure_cache_pass_2026")
    REDIS_URL: str = os.getenv("REDIS_URL", f"redis://:{os.getenv('REDIS_PASSWORD', 'sys_design_secure_cache_pass_2026')}@{os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', '6379')}/0")

    # Qdrant Vector DB for Policy Self-RAG
    QDRANT_HOST: str = os.getenv("QDRANT_HOST", "qdrant")
    QDRANT_PORT: int = int(os.getenv("QDRANT_PORT", "6333"))
    POLICY_COLLECTION: str = "dispute_resolution_policies"

    # GraphRAG Service URL for Supplier Defect Traversal
    GRAPHRAG_SERVICE_URL: str = os.getenv("GRAPHRAG_SERVICE_URL", "http://knowledge-graph-rag-service:8011")

    # ClickHouse OLAP for Fraud Scoring
    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_DB: str = os.getenv("CLICKHOUSE_DB", "copilot_analytics")
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "")

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    # Auto-Settlement Thresholds
    AUTO_SETTLE_MAX_AMOUNT: float = float(os.getenv("AUTO_SETTLE_MAX_AMOUNT", "200.0"))
    FRAUD_RISK_THRESHOLD: float = float(os.getenv("FRAUD_RISK_THRESHOLD", "0.65"))

    class Config:
        env_file = ".env"
        extra = "allow"


settings = Settings()
