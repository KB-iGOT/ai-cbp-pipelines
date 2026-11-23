from __future__ import annotations

from aiokafka import AIOKafkaConsumer

from ..settings import PipelineSettings
from .config import connection_kwargs


def build_consumer(settings: PipelineSettings, topics: list[str], group_id: str) -> AIOKafkaConsumer:
    """Offsets are committed manually by the worker, only after a batch has been handled."""
    return AIOKafkaConsumer(
        *topics,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=settings.KAFKA_AUTO_OFFSET_RESET,
        max_poll_records=settings.KAFKA_MAX_POLL_RECORDS,
        max_poll_interval_ms=settings.KAFKA_MAX_POLL_INTERVAL_MS,
        session_timeout_ms=settings.KAFKA_SESSION_TIMEOUT_MS,
        **connection_kwargs(settings),
    )
