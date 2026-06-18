from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://user:pass@localhost:5432/db"
    broker_url: str = "redis://localhost:6379/0"
    app_workers: int = 4
    consumer_workers: int = 4
    redis_stream: str = "log-events"
    redis_consumer_group: str = "log-aggregators"
    publish_timeout_seconds: float = 180.0
    result_ttl_seconds: int = 300
    claim_idle_ms: int = 30_000


settings = Settings()
