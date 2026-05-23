from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PORT: int = 8003
    DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str
    SERVICE_NAME: str = "order-service"
    REDIS_URL: str = "redis://:sys_design_secure_cache_pass_2026@localhost:6379"
    USER_SERVICE_URL: str = "http://user-service:8001"
    PRODUCT_SERVICE_URL: str = "http://product-service:8002"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
