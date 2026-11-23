"""Consume → parse → dedupe → handle (with retry) → dead-letter failures → commit.

Delivery is at-least-once: offsets are committed only after every message in the
batch has either been handled or written to the DLQ. If the DLQ write itself fails,
or a handler raises FatalPipelineError, the worker exits without committing and the
batch is re-read after restart. Handlers are idempotent, so re-processing is safe.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any, Awaitable, Callable

from .base_pipeline import BasePipeline
from .errors import FatalPipelineError, NonRetryableError, SkipEvent
from .health import Heartbeat
from .kafka.dlq import DeadLetterPublisher
from .log import get_logger
from .settings import PipelineSettings


class PipelineWorker:
    def __init__(
        self,
        pipeline: BasePipeline,
        consumer: Any,
        dlq: DeadLetterPublisher,
        settings: PipelineSettings,
        *,
        heartbeat: Heartbeat | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.pipeline = pipeline
        self.consumer = consumer
        self.dlq = dlq
        self.settings = settings
        self.heartbeat = heartbeat
        self._sleep = sleep
        self.log = get_logger(f"{pipeline.name}.worker")

    async def run(self, stop: asyncio.Event) -> None:
        await self.consumer.start()
        self.log.info("Consuming %s", ", ".join(self.pipeline.topics()))
        try:
            while not stop.is_set():
                batch = await self.consumer.getmany(
                    timeout_ms=self.settings.KAFKA_POLL_TIMEOUT_MS,
                    max_records=self.settings.KAFKA_MAX_POLL_RECORDS,
                )
                if self.heartbeat:
                    self.heartbeat.beat()
                records = [record for partition_records in batch.values() for record in partition_records]
                if not records:
                    continue
                await self.process_records(records)
                await self.consumer.commit()
        finally:
            await self.consumer.stop()
            self.log.info("Consumer stopped")

    async def process_records(self, records: list[Any]) -> Counter:
        stats: Counter = Counter()
        latest: dict[str, tuple[Any, Any]] = {}
        unkeyed: list[tuple[Any, Any]] = []

        for record in records:
            try:
                event = self.pipeline.decode(record)
            except NonRetryableError as exc:
                await self.dlq.publish(record, exc, attempts=1)
                stats["dead_lettered"] += 1
                continue
            if event is None:
                stats["ignored"] += 1
                continue
            key = self.pipeline.dedupe_key(event)
            if key is None:
                unkeyed.append((record, event))
            else:
                if key in latest:
                    stats["superseded"] += 1
                latest[key] = (record, event)

        work = unkeyed + list(latest.values())
        if work:
            semaphore = asyncio.Semaphore(self.settings.PIPELINE_CONCURRENCY)
            results: list[asyncio.Task] = []

            async def run_one(record: Any, event: Any) -> str:
                async with semaphore:
                    return await self._handle_with_retry(record, event)

            # TaskGroup cancels the siblings if one raises FatalPipelineError or a DLQ write fails.
            async with asyncio.TaskGroup() as group:
                for record, event in work:
                    results.append(group.create_task(run_one(record, event)))
            stats.update(task.result() for task in results)

        self.log.info(
            "Batch of %d message(s): %s", len(records), ", ".join(f"{k}={v}" for k, v in sorted(stats.items())) or "-"
        )
        return stats

    async def _handle_with_retry(self, record: Any, event: Any) -> str:
        label = self.pipeline.describe(event)
        max_attempts = max(1, self.settings.PIPELINE_MAX_ATTEMPTS)
        for attempt in range(1, max_attempts + 1):
            try:
                result = await self.pipeline.handle(event)
            except SkipEvent as exc:
                self.log.info("Skipped %s: %s", label, exc)
                return "skipped"
            except FatalPipelineError:
                raise
            except NonRetryableError as exc:
                await self.dlq.publish(record, exc, attempts=attempt)
                return "dead_lettered"
            except Exception as exc:  # noqa: BLE001 - anything else is worth a retry
                if attempt >= max_attempts:
                    self.log.exception("Giving up on %s after %d attempts", label, attempt)
                    await self.dlq.publish(record, exc, attempts=attempt)
                    return "dead_lettered"
                delay = self.settings.PIPELINE_RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1)
                self.log.warning(
                    "%s failed (attempt %d/%d), retrying in %.0fs: %s: %s",
                    label, attempt, max_attempts, delay, type(exc).__name__, exc,
                )
                await self._sleep(delay)
            else:
                self.log.info("%s -> %s%s", label, result.outcome.value, f" ({result.detail})" if result.detail else "")
                return result.outcome.value
        return "dead_lettered"  # unreachable
