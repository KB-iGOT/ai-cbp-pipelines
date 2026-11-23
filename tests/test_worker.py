import asyncio
import json
from dataclasses import dataclass
from typing import Any

import pytest

from src.common.base_pipeline import BasePipeline
from src.common.errors import FatalPipelineError, NonRetryableError, SkipEvent
from src.common.types import HandleResult, Outcome
from src.common.worker import PipelineWorker


@dataclass
class Record:
    value: bytes | None
    topic: str = "t"
    partition: int = 0
    offset: int = 0
    key: bytes | None = None


@dataclass(frozen=True)
class Event:
    identifier: str
    n: int = 0


class FakeCtx:
    def __init__(self, settings):
        self.settings = settings


class FakePipeline(BasePipeline[Event]):
    name = "fake"

    def __init__(self, ctx, behaviour):
        super().__init__(ctx)
        self.behaviour = behaviour  # identifier -> list of exceptions/results to yield in order
        self.calls: list[Event] = []

    def topics(self):
        return ["t"]

    def parse(self, payload: Any):
        return Event(payload["id"], payload.get("n", 0)) if "id" in payload else None

    async def handle(self, event):
        self.calls.append(event)
        steps = self.behaviour.get(event.identifier, [])
        step = steps.pop(0) if steps else HandleResult(Outcome.INSERTED)
        if isinstance(step, BaseException):
            raise step
        return step


class FakeDLQ:
    def __init__(self, fail=False):
        self.published = []
        self.fail = fail

    async def publish(self, record, error, attempts):
        if self.fail:
            raise RuntimeError("kafka down")
        self.published.append((record, type(error).__name__, attempts))


async def no_sleep(_):
    return None


def rec(payload, offset=0):
    return Record(json.dumps(payload).encode(), offset=offset)


def make_worker(settings, behaviour=None, dlq=None):
    pipeline = FakePipeline(FakeCtx(settings), behaviour or {})
    dlq = dlq or FakeDLQ()
    return PipelineWorker(pipeline, consumer=None, dlq=dlq, settings=settings, sleep=no_sleep), pipeline, dlq


def test_success_and_ignored(settings):
    worker, pipeline, dlq = make_worker(settings)
    stats = asyncio.run(worker.process_records([rec({"id": "a"}), rec({"other": 1})]))
    assert stats == {"inserted": 1, "ignored": 1}
    assert not dlq.published


def test_empty_message_goes_to_dlq(settings):
    worker, pipeline, dlq = make_worker(settings)
    stats = asyncio.run(worker.process_records([Record(None)]))
    assert stats == {"dead_lettered": 1}
    assert dlq.published[0][1:] == ("InvalidEventError", 1)
    assert not pipeline.calls


def test_retry_then_success(settings):
    worker, pipeline, dlq = make_worker(settings, {"a": [RuntimeError("boom"), RuntimeError("boom")]})
    stats = asyncio.run(worker.process_records([rec({"id": "a"})]))
    assert stats["inserted"] == 1
    assert len(pipeline.calls) == 3
    assert not dlq.published


def test_exhausted_retries_go_to_dlq(settings):
    worker, pipeline, dlq = make_worker(settings, {"a": [RuntimeError("x")] * 5})
    stats = asyncio.run(worker.process_records([rec({"id": "a"})]))
    assert stats["dead_lettered"] == 1
    assert dlq.published[0][1:] == ("RuntimeError", 3)


def test_non_retryable_goes_straight_to_dlq(settings):
    worker, pipeline, dlq = make_worker(settings, {"a": [NonRetryableError("bad")]})
    asyncio.run(worker.process_records([rec({"id": "a"})]))
    assert len(pipeline.calls) == 1
    assert dlq.published[0][1:] == ("NonRetryableError", 1)


def test_invalid_json_goes_to_dlq(settings):
    worker, pipeline, dlq = make_worker(settings)
    stats = asyncio.run(worker.process_records([Record(b"{not json")]))
    assert stats["dead_lettered"] == 1
    assert dlq.published[0][1:] == ("InvalidEventError", 1)
    assert not pipeline.calls


def test_skip_event(settings):
    worker, _, dlq = make_worker(settings, {"a": [SkipEvent("nope")]})
    assert asyncio.run(worker.process_records([rec({"id": "a"})]))["skipped"] == 1
    assert not dlq.published


def test_dedupe_keeps_latest(settings):
    worker, pipeline, _ = make_worker(settings)
    stats = asyncio.run(worker.process_records([rec({"id": "a", "n": 1}), rec({"id": "b"}), rec({"id": "a", "n": 2})]))
    assert stats["superseded"] == 1
    assert sorted((e.identifier, e.n) for e in pipeline.calls) == [("a", 2), ("b", 0)]


def test_fatal_error_propagates(settings):
    worker, _, dlq = make_worker(settings, {"a": [FatalPipelineError("401")]})
    with pytest.raises(BaseExceptionGroup) as info:
        asyncio.run(worker.process_records([rec({"id": "a"}), rec({"id": "b"})]))
    assert info.group_contains(FatalPipelineError)
    assert not dlq.published


def test_dlq_failure_propagates_so_offsets_are_not_committed(settings):
    worker, _, _ = make_worker(settings, {"a": [NonRetryableError("bad")]}, dlq=FakeDLQ(fail=True))
    with pytest.raises(BaseExceptionGroup):
        asyncio.run(worker.process_records([rec({"id": "a"})]))


class FakeConsumer:
    def __init__(self, batches, stop):
        self.batches = list(batches)
        self.stop_event = stop
        self.commits = 0
        self.started = self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def getmany(self, timeout_ms, max_records):
        if not self.batches:
            self.stop_event.set()
            return {}
        return {"tp": self.batches.pop(0)}

    async def commit(self):
        self.commits += 1


def test_run_commits_after_each_batch(settings):
    async def scenario():
        stop = asyncio.Event()
        consumer = FakeConsumer([[rec({"id": "a"})], [rec({"id": "b"})]], stop)
        worker, pipeline, _ = make_worker(settings)
        worker.consumer = consumer
        await worker.run(stop)
        return consumer, pipeline

    consumer, pipeline = asyncio.run(scenario())
    assert consumer.commits == 2
    assert consumer.started and consumer.stopped
    assert [e.identifier for e in pipeline.calls] == ["a", "b"]
