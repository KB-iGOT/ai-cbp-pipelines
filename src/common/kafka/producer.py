from __future__ import annotations

from aiokafka import AIOKafkaProducer

from ..settings import PipelineSettings
from .config import connection_kwargs


def build_producer(settings: PipelineSettings) -> AIOKafkaProducer:
    return AIOKafkaProducer(acks="all", enable_idempotence=True, **connection_kwargs(settings))
