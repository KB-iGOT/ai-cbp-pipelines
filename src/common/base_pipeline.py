"""The contract every pipeline implements. Kafka, retries, DLQ and commits live in the
worker, so a pipeline only says which topic it reads, how to parse a message and what
to do with one event."""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from .errors import InvalidEventError
from .log import get_logger
from .types import HandleResult

if TYPE_CHECKING:
    from .context import PipelineContext

E = TypeVar("E")


class BasePipeline(ABC, Generic[E]):
    name: ClassVar[str]
    needs_embeddings: ClassVar[bool] = True

    def __init__(self, ctx: "PipelineContext") -> None:
        self.ctx = ctx
        self.settings = ctx.settings
        self.log = get_logger(self.name)

    @abstractmethod
    def topics(self) -> list[str]:
        """Kafka topics to subscribe to."""

    @abstractmethod
    def parse(self, payload: Any) -> E | None:
        """Turn a decoded JSON payload into an event, or None to ignore the message."""

    @abstractmethod
    async def handle(self, event: E) -> HandleResult:
        """Process one event. Raise errors from ``common.errors`` to control retry/DLQ."""

    def decode(self, record: Any) -> E | None:
        """Empty messages and invalid JSON raise InvalidEventError (straight to the DLQ)."""
        if record.value is None:
            raise InvalidEventError("empty message (no value)")
        try:
            payload = json.loads(record.value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidEventError(f"invalid JSON: {exc}") from exc
        return self.parse(payload)

    def dedupe_key(self, event: E) -> str | None:
        """Events with the same key in one batch collapse to the latest one."""
        return getattr(event, "identifier", None)

    def describe(self, event: E) -> str:
        return str(getattr(event, "identifier", event))

    async def on_start(self) -> None:
        pass

    async def on_stop(self) -> None:
        pass
