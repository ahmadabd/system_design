from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PORT: int = 8004
    DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str
    SERVICE_NAME: str = "payment-service"
    REDIS_URL: str = "redis://:sys_design_secure_cache_pass_2026@localhost:6379"
    ORDER_SERVICE_URL: str = "http://order-service:8003"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
