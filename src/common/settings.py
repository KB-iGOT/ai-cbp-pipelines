"""Settings for pipeline workers and jobs.

Workers only need the database, the KB API, Gemini and Kafka. Values come from environment
variables or a ``.env`` file in the working directory (see .env.example).
"""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class PipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    LOG_LEVEL: str = "INFO"

    # ── Database ──────────────────────────────────────────────────────────────
    DATABASE_URL: str
    DB_POOL_MIN_SIZE: int = 1
    DB_POOL_MAX_SIZE: int = 10

    # ── KB API ────────────────────────────────────────────────────────────────
    KB_BASE_URL: str
    KB_AUTH_TOKEN: str
    KB_HTTP_TIMEOUT_SECONDS: float = 60.0
    KB_HTTP_RETRIES: int = 2

    # ── Gemini embeddings (not needed by course_retirement) ──────────────────
    GOOGLE_API_KEY: str = ""
    GOOGLE_EMBEDDING_MODEL: str = "gemini-embedding-2"
    EMBEDDING_OUTPUT_DIMENSIONALITY: int = 1536
    EMBEDDING_CONCURRENCY: int = Field(default=20, description="Max concurrent Gemini embedding calls per process")

    # ── Kafka connection ──────────────────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_SECURITY_PROTOCOL: str = "PLAINTEXT"  # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
    KAFKA_SASL_MECHANISM: str = ""  # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
    KAFKA_SASL_USERNAME: str = ""
    KAFKA_SASL_PASSWORD: str = ""
    KAFKA_SSL_CAFILE: str = ""

    # ── Kafka consumer behaviour ─────────────────────────────────────────────
    KAFKA_CONSUMER_GROUP_PREFIX: str = "cbp-ai"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_MAX_POLL_RECORDS: int = 50
    KAFKA_POLL_TIMEOUT_MS: int = 1000
    KAFKA_MAX_POLL_INTERVAL_MS: int = Field(default=600_000, description="Embedding a batch can be slow; keep this generous")
    KAFKA_SESSION_TIMEOUT_MS: int = 30_000
    KAFKA_DLQ_TOPIC_TEMPLATE: str = "{prefix}.{pipeline}.dlq"

    # ── Topics (one per pipeline; each carries only that pipeline's events) ──
    KAFKA_COURSE_INGESTION_TOPIC: str = "local.course.ingestion"
    KAFKA_COURSE_RETIREMENT_TOPIC: str = "local.course.retirement"

    # ── Processing ────────────────────────────────────────────────────────────
    PIPELINE_CONCURRENCY: int = Field(default=10, description="Events handled in parallel within a batch")
    PIPELINE_MAX_ATTEMPTS: int = Field(default=4, description="Attempts per event before it is dead-lettered")
    PIPELINE_RETRY_BACKOFF_SECONDS: float = Field(default=5.0, description="Base delay; doubles each attempt")
    PIPELINE_HEARTBEAT_FILE: str = "/tmp/pipeline-heartbeat"

    @property
    def asyncpg_dsn(self) -> str:
        """asyncpg wants postgresql://, the app's .env uses the SQLAlchemy postgresql+asyncpg:// form."""
        return self.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)

    def require(self, name: str) -> str:
        value = getattr(self, name)
        if not value:
            raise RuntimeError(f"{name} must be set to run this pipeline")
        return value

    def consumer_group(self, pipeline: str) -> str:
        return f"{self.KAFKA_CONSUMER_GROUP_PREFIX}.{pipeline}"

    def dlq_topic(self, pipeline: str) -> str:
        return self.KAFKA_DLQ_TOPIC_TEMPLATE.format(prefix=self.KAFKA_CONSUMER_GROUP_PREFIX, pipeline=pipeline)
