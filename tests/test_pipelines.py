import asyncio
import json
from dataclasses import dataclass

import pytest

from src.common.errors import InvalidEventError, NonRetryableError, StaleReadError
from src.common.types import Outcome
from src.common.worker import PipelineWorker
from src.course_ingestion.pipeline import CourseIngestionEvent, CourseIngestionPipeline
from src.course_retirement import pipeline as retirement_module
from src.course_retirement.pipeline import CourseRetirementPipeline, RetirementEvent
from src.course_retirement.references import CleanupResult, TableResult


class FakeContent:
    """Stands in for the KB content read API: returns the item, or None (404)."""

    def __init__(self, items=None):
        self.items = items or {}
        self.reads = []

    async def read_content(self, identifier, fields=None):
        self.reads.append((identifier, fields))
        return self.items.get(identifier)


class FakeRepository:
    def __init__(self, existing=()):
        self.existing = set(existing)
        self.deleted = []

    async def delete(self, identifiers):
        self.deleted.extend(identifiers)
        hits = self.existing & set(identifiers)
        self.existing -= hits
        return len(hits)


class FakeIndexer:
    def __init__(self):
        self.indexed = []

    async def index(self, course):
        self.indexed.append(course)
        return Outcome.INSERTED


class FakeCtx:
    def __init__(self, settings, content=None, existing=()):
        self.settings = settings
        self.content = content or FakeContent()
        self.repository = FakeRepository(existing)
        self.pool = object()


@dataclass
class Record:
    value: bytes | None
    topic: str = "local.course.retirement"
    partition: int = 0
    offset: int = 0
    key: bytes | None = None


class FakeDLQ:
    def __init__(self):
        self.published = []

    async def publish(self, record, error, attempts):
        self.published.append((record.offset, type(error).__name__, attempts))


def course(identifier="do_1", status="Live", version="200", category="Course", level="Beginner"):
    return {"identifier": identifier, "status": status, "versionKey": version, "name": "C",
            "primaryCategory": category, "courseCategory": category, "courseLevel": level}


def kafka_event(identifier="do_1", op="PUBLISHED", status=("Processing", "Live"), version="200", **overrides):
    props = {}
    if status:
        props["status"] = {"ov": status[0], "nv": status[1]}
    if version:
        props["versionKey"] = {"ov": "100", "nv": version}
    event = {
        "identifier": identifier, "operationType": op, "objectType": "Content",
        "primaryCategory": "Course", "courseCategory": "Course", "contentType": "Course",
        "transactionData": {"properties": props},
    }
    event.update(overrides)
    return event


def ingestion(ctx):
    pipeline = CourseIngestionPipeline(ctx)
    pipeline.kb_indexer = FakeIndexer()  # normally created in on_start()
    return pipeline


def run(coro):
    return asyncio.run(coro)


# ── course_ingestion: parse ──────────────────────────────────────────────────
def test_ingestion_parses_publish_event(settings):
    event = ingestion(FakeCtx(settings)).parse(kafka_event())
    assert event == CourseIngestionEvent("do_1", "PUBLISHED", "Live", "200")


def test_ingestion_parses_update_without_status_change(settings):
    event = ingestion(FakeCtx(settings)).parse(kafka_event(op="updated", status=None))
    assert (event.operation_type, event.status) == ("UPDATED", None)


@pytest.mark.parametrize("payload, reason", [
    (kafka_event(op="RETIRED"), "operationType must be PUBLISHED or UPDATED"),
    (kafka_event(op=None), "operationType must be PUBLISHED or UPDATED"),
    (kafka_event(identifier=""), "identifier is missing"),
    (kafka_event(identifier=None), "identifier is missing"),
    (kafka_event(identifier="do_1.img"), "draft image node"),
    (kafka_event(identifier="abc-1"), "must look like do_<id>"),
    (kafka_event(primaryCategory="Resource"), "not a Course"),
    (kafka_event(courseCategory="Program"), "not a Course"),
    (kafka_event(status=("Live", "Retired")), "status changed to 'Retired'"),
    (["not", "a", "dict"], "expected a JSON object"),
])
def test_ingestion_invalid_message_raises(settings, payload, reason):
    with pytest.raises(InvalidEventError, match=reason):
        ingestion(FakeCtx(settings)).parse(payload)


def test_ingestion_accepts_any_object_type(settings):
    assert ingestion(FakeCtx(settings)).parse(kafka_event(objectType="Collection")) is not None


# ── course_ingestion: handle ─────────────────────────────────────────────────
def test_ingestion_indexes_live_course(settings):
    content = FakeContent({"do_1": course()})
    pipeline = ingestion(FakeCtx(settings, content))
    result = run(pipeline.handle(CourseIngestionEvent("do_1", "PUBLISHED", "Live", "200")))
    assert result.outcome is Outcome.INSERTED
    assert pipeline.kb_indexer.indexed[0]["courseLevel"] == "Beginner"
    identifier, fields = content.reads[0]
    assert identifier == "do_1" and "courseLevel" in fields


@pytest.mark.parametrize("status", ["Live", None])
def test_ingestion_not_found_in_kb_goes_to_dlq(settings, status):
    pipeline = ingestion(FakeCtx(settings))
    with pytest.raises(NonRetryableError, match="not found in KB"):
        run(pipeline.handle(CourseIngestionEvent("do_1", "PUBLISHED", status)))


def test_ingestion_stale_when_kb_has_older_version(settings):
    pipeline = ingestion(FakeCtx(settings, FakeContent({"do_1": course(version="100")})))
    with pytest.raises(StaleReadError):
        run(pipeline.handle(CourseIngestionEvent("do_1", "PUBLISHED", "Live", "200")))


def test_ingestion_non_course_in_kb_goes_to_dlq(settings):
    pipeline = ingestion(FakeCtx(settings, FakeContent({"do_1": course(category="Resource")})))
    with pytest.raises(NonRetryableError, match="not a Course"):
        run(pipeline.handle(CourseIngestionEvent("do_1", "UPDATED")))
    assert not pipeline.kb_indexer.indexed


def test_ingestion_not_live_in_kb_goes_to_dlq(settings):
    pipeline = ingestion(FakeCtx(settings, FakeContent({"do_1": course(status="Draft")})))
    with pytest.raises(NonRetryableError, match="KB status is 'Draft'"):
        run(pipeline.handle(CourseIngestionEvent("do_1", "UPDATED")))


def test_ingestion_deletes_if_already_retired(settings):
    ctx = FakeCtx(settings, FakeContent({"do_1": course(status="Retired")}), existing={"do_1"})
    pipeline = ingestion(ctx)
    assert run(pipeline.handle(CourseIngestionEvent("do_1", "UPDATED"))).outcome is Outcome.DELETED
    assert not pipeline.kb_indexer.indexed


def test_ingestion_retired_and_not_in_table_is_unchanged(settings):
    pipeline = ingestion(FakeCtx(settings, FakeContent({"do_1": course(status="Retired")})))
    assert run(pipeline.handle(CourseIngestionEvent("do_1", "UPDATED"))).outcome is Outcome.UNCHANGED


def test_ingestion_invalid_messages_go_to_dlq_not_ignored(settings):
    dlq = FakeDLQ()
    pipeline = ingestion(FakeCtx(settings, FakeContent({"do_ok": course(identifier="do_ok")})))
    worker = PipelineWorker(pipeline, consumer=None, dlq=dlq, settings=settings)
    records = [
        Record(json.dumps(kafka_event(identifier="do_ok")).encode(), offset=0),
        Record(json.dumps(kafka_event(identifier="do_2", op="RETIRED")).encode(), offset=1),
        Record(json.dumps(kafka_event(identifier="do_3", primaryCategory="Resource")).encode(), offset=2),
        Record(json.dumps(kafka_event(identifier="do_missing")).encode(), offset=3),
        Record(None, offset=4),
    ]
    stats = run(worker.process_records(records))
    assert stats == {"inserted": 1, "dead_lettered": 4}
    assert sorted((offset, error) for offset, error, _ in dlq.published) == [
        (1, "InvalidEventError"), (2, "InvalidEventError"), (3, "NonRetryableError"), (4, "InvalidEventError"),
    ]
    assert all(attempts == 1 for _, _, attempts in dlq.published)  # never retried


# ── course_retirement ────────────────────────────────────────────────────────
def test_retirement_parse(settings):
    pipeline = CourseRetirementPipeline(FakeCtx(settings))
    assert pipeline.parse({"identifier": " do_1 ", "operationType": "retired"}) == RetirementEvent("do_1")


@pytest.mark.parametrize("payload, reason", [
    ({"identifier": "do_1", "operationType": "PUBLISHED"}, "operationType must be RETIRED"),
    ({"identifier": "do_1"}, "operationType must be RETIRED"),
    ({"operationType": "RETIRED"}, "identifier is missing"),
    ({"identifier": "  ", "operationType": "RETIRED"}, "identifier is missing"),
    ({"identifier": 123, "operationType": "RETIRED"}, "identifier is missing or not a string"),
    ({"identifier": "abc-1", "operationType": "RETIRED"}, "must look like do_<id>"),
    ({"identifier": "do_1.img", "operationType": "RETIRED"}, "must look like do_<id>"),
    ("RETIRED", "expected a JSON object"),
    (None, "expected a JSON object"),
])
def test_retirement_invalid_message_raises(settings, payload, reason):
    with pytest.raises(InvalidEventError, match=reason):
        CourseRetirementPipeline(FakeCtx(settings)).parse(payload)


def test_retirement_invalid_messages_go_to_dlq_not_ignored(settings, monkeypatch):
    cleanup, calls = fake_cleanup(rows=1)
    monkeypatch.setattr(retirement_module, "remove_course_references", cleanup)
    dlq = FakeDLQ()
    worker = PipelineWorker(CourseRetirementPipeline(FakeCtx(settings, existing={"do_ok"})),
                            consumer=None, dlq=dlq, settings=settings)
    records = [
        Record(json.dumps({"identifier": "do_ok", "operationType": "RETIRED"}).encode(), offset=0),
        Record(json.dumps({"identifier": "do_2", "operationType": "PUBLISHED"}).encode(), offset=1),
        Record(json.dumps({"operationType": "RETIRED"}).encode(), offset=2),
        Record(b"not json", offset=3),
        Record(None, offset=4),
    ]
    stats = run(worker.process_records(records))
    assert stats == {"deleted": 1, "dead_lettered": 4}
    assert [(offset, error) for offset, error, _ in dlq.published] == [
        (1, "InvalidEventError"), (2, "InvalidEventError"), (3, "InvalidEventError"), (4, "InvalidEventError"),
    ]
    assert all(attempts == 1 for _, _, attempts in dlq.published)  # never retried
    assert calls == ["do_ok"]


def fake_cleanup(rows):
    calls = []

    async def remove_course_references(pool, identifier):
        calls.append(identifier)
        return CleanupResult({"cbp_plans": TableResult(rows=rows, removed=rows)})

    return remove_course_references, calls


def test_retirement_deletes_row_then_references(settings, monkeypatch):
    cleanup, calls = fake_cleanup(rows=2)
    monkeypatch.setattr(retirement_module, "remove_course_references", cleanup)
    ctx = FakeCtx(settings, existing={"do_1"})
    result = run(CourseRetirementPipeline(ctx).handle(RetirementEvent("do_1")))
    assert result.outcome is Outcome.DELETED
    assert ctx.repository.deleted == ["do_1"]
    assert calls == ["do_1"]
    assert "cbp_plans 2/2" in result.detail


def test_retirement_skips_when_nothing_references_course(settings, monkeypatch):
    cleanup, calls = fake_cleanup(rows=0)
    monkeypatch.setattr(retirement_module, "remove_course_references", cleanup)
    result = run(CourseRetirementPipeline(FakeCtx(settings)).handle(RetirementEvent("do_1")))
    assert result.outcome is Outcome.SKIPPED
    assert calls == ["do_1"]
