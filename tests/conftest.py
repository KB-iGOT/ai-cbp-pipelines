import pytest

from src.common.settings import PipelineSettings


@pytest.fixture
def settings() -> PipelineSettings:
    return PipelineSettings(
        _env_file=None,
        DATABASE_URL="postgresql+asyncpg://u:p@localhost:5432/db",
        KB_BASE_URL="https://kb.example",
        KB_AUTH_TOKEN="Bearer test",
        KAFKA_COURSE_INGESTION_TOPIC="course-ingestion",
        KAFKA_COURSE_RETIREMENT_TOPIC="course-retirement",
        PIPELINE_MAX_ATTEMPTS=3,
        PIPELINE_RETRY_BACKOFF_SECONDS=1,
    )
