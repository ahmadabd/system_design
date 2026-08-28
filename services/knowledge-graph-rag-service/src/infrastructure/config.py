import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SERVICE_NAME: str = "knowledge-graph-rag-service"
    PORT: int = 8011
    ENVIRONMENT: str = "production"
    
    # Vector Database
    QDRANT_HOST: str = os.getenv("QDRANT_HOST", "qdrant")
    QDRANT_PORT: int = int(os.getenv("QDRANT_PORT", "6333"))
    
    # Kafka Broker
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    # LLM Settings (OpenAI / OpenRouter / Local Heuristic fallback)
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_BASE_URL: str = os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
    MODEL_NAME: str = os.getenv("MODEL_NAME", "google/gemini-2.5-flash")
    
    # OpenTelemetry
    OTEL_EXPORTER_OTLP_ENDPOINT: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    
    # Graph Storage File
    GRAPH_STORAGE_PATH: str = os.getenv("GRAPH_STORAGE_PATH", "/tmp/knowledge_graph.json")

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
