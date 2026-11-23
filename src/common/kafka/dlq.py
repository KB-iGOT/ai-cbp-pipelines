from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..log import get_logger

log = get_logger("dlq")

SOURCE_TOPIC_HEADER = "x-source-topic"
# How many times replay_dlq has already sent this message back. Set by replay_dlq on the
# replayed message and carried over here if it fails again, so replays can be capped.
REPLAY_COUNT_HEADER = "x-replay-count"


def get_header(record: Any, name: str) -> str | None:
    for key, value in getattr(record, "headers", None) or ():
        if key == name and value is not None:
            return value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)
    return None


def replay_count(record: Any) -> int:
    try:
        return int(get_header(record, REPLAY_COUNT_HEADER) or 0)
    except ValueError:
        return 0


class DeadLetterPublisher:
    """Copies a failed message to the pipeline's DLQ topic with the reason in headers.

    The original key and value are kept byte-for-byte so the message can be replayed
    with ``python -m src.jobs.replay_dlq``.
    """

    def __init__(self, producer: Any, topic: str, pipeline: str) -> None:
        self._producer = producer
        self.topic = topic
        self._pipeline = pipeline

    async def publish(self, record: Any, error: BaseException, attempts: int) -> None:
        headers = [
            ("x-pipeline", self._pipeline),
            (SOURCE_TOPIC_HEADER, record.topic),
            ("x-source-partition", str(record.partition)),
            ("x-source-offset", str(record.offset)),
            ("x-error-type", type(error).__name__),
            ("x-error-message", str(error)[:1000]),
            ("x-attempts", str(attempts)),
            ("x-failed-at", datetime.now(timezone.utc).isoformat()),
            (REPLAY_COUNT_HEADER, str(replay_count(record))),
        ]
        await self._producer.send_and_wait(
            self.topic,
            value=record.value,
            key=record.key,
            headers=[(k, v.encode("utf-8")) for k, v in headers],
        )
        log.warning(
            "Dead-lettered %s[%s]@%s to %s after %d attempt(s) (replayed %d time(s) before): %s: %s",
            record.topic, record.partition, record.offset, self.topic, attempts, replay_count(record),
            type(error).__name__, error,
        )
