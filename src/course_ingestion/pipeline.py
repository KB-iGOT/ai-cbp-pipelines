"""Embed and upsert KB courses when they are published or updated.

Event:

    {
      "identifier": "do_12345",
      "operationType": "PUBLISHED" | "UPDATED",
      "objectType": "Content",
      "primaryCategory": "Course",
      "courseCategory": "Course",
      "contentType": "Course",
      "transactionData": {
        "properties": {
          "status":     {"ov": "Processing",    "nv": "Live"},
          "versionKey": {"ov": "1700000000000", "nv": "1700000005000"}
        }
      }
    }

Nothing on the ingestion topic is silently ignored or skipped:

  * A message that fails validation (not a JSON object, operationType other than PUBLISHED /
    UPDATED, missing or non do_ identifier, not a Course, status changed
    to something other than Live, empty message) raises InvalidEventError → DLQ, not retried.
  * A valid event whose course KB can't index (not found, not a course, not Live) raises
    NonRetryableError → DLQ, not retried.

The event is a trigger: the course is always re-read from the KB content read API before it is
indexed, so KB is the source of truth.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..common.base_pipeline import BasePipeline
from ..common.course_repository import COURSE_TABLE
from ..common.errors import InvalidEventError, NonRetryableError, StaleReadError
from ..common.schema import ensure_schema
from ..common.types import HandleResult, Outcome
from .indexer import KbCourseIndexer, get_kb_course
from .utils import CONTENT_ID_RE, LIVE, RETIRED, is_course, is_older

INGESTION_OPERATIONS = ("PUBLISHED", "UPDATED")


@dataclass(frozen=True)
class CourseIngestionEvent:
    """A parsed publish / update message for a KB course."""

    identifier: str  # e.g. "do_12345"
    operation_type: str  # PUBLISHED | UPDATED
    status: str | None = None  # transactionData.properties.status.nv; None = not changed
    version: str | None = None  # transactionData.properties.versionKey.nv


def _new_value(properties: dict, name: str) -> Any:
    change = properties.get(name)
    return change.get("nv") if isinstance(change, dict) else None


class CourseIngestionPipeline(BasePipeline[CourseIngestionEvent]):
    name = "course_ingestion"

    def topics(self) -> list[str]:
        return [self.settings.require("KAFKA_COURSE_INGESTION_TOPIC")]

    async def on_start(self) -> None:
        await ensure_schema(self.ctx.pool, COURSE_TABLE, self.settings.EMBEDDING_OUTPUT_DIMENSIONALITY)
        self.kb_indexer = KbCourseIndexer(self.ctx.pool, COURSE_TABLE, self.ctx.embedder)

    def parse(self, payload: Any) -> CourseIngestionEvent:
        """Returns the event. Never returns None: an invalid message raises InvalidEventError,
        which the worker sends straight to the DLQ."""
        if not isinstance(payload, dict):
            raise InvalidEventError(f"expected a JSON object, got {type(payload).__name__}")

        raw_operation = payload.get("operationType")
        operation_type = str(raw_operation or "").strip().upper()
        if operation_type not in INGESTION_OPERATIONS:
            raise InvalidEventError(f"operationType must be PUBLISHED or UPDATED, got {raw_operation!r}")

        raw_identifier = payload.get("identifier")
        if not isinstance(raw_identifier, str) or not raw_identifier.strip():
            raise InvalidEventError(f"identifier is missing or not a string: {raw_identifier!r}")
        identifier = raw_identifier.strip()
        if identifier.endswith(".img"):
            raise InvalidEventError(f"identifier is a draft image node: {identifier!r}")
        if not CONTENT_ID_RE.match(identifier):
            raise InvalidEventError(f"identifier must look like do_<id>, got {identifier!r}")

        if not is_course(payload):
            raise InvalidEventError(
                "not a Course: primaryCategory and courseCategory must be Course, got "
                f"{payload.get('primaryCategory')!r} / {payload.get('courseCategory')!r}"
            )

        properties = (payload.get("transactionData") or {}).get("properties") or {}
        status = _new_value(properties, "status")
        version = _new_value(properties, "versionKey")
        # Live = publish / republish. None = metadata update without a status change.
        if status not in (None, LIVE):
            raise InvalidEventError(f"status changed to {status!r}; only Live is indexed")

        return CourseIngestionEvent(
            identifier=identifier,
            operation_type=operation_type,
            status=status,
            version=str(version) if version else None,
        )

    async def handle(self, event: CourseIngestionEvent) -> HandleResult:
        content = await get_kb_course(self.ctx.content, event.identifier)  # content read API
        if content is None:
            raise NonRetryableError("not found in KB (content read API returned 404)")

        if not is_course(content):
            raise NonRetryableError(
                "KB content is not a Course: primaryCategory / courseCategory are "
                f"{content.get('primaryCategory')!r} / {content.get('courseCategory')!r}"
            )

        status = content.get("status")
        if status == RETIRED:
            # Retired after this event was emitted; the newer state wins.
            removed = await self.ctx.repository.delete([event.identifier])
            if removed:
                return HandleResult(Outcome.DELETED, f"retired in KB; {removed} row(s) removed")
            return HandleResult(Outcome.UNCHANGED, "retired in KB; not in the table")
        if status != LIVE:
            raise NonRetryableError(f"KB status is {status!r}, expected Live")

        if is_older(content.get("versionKey"), event.version):
            # KB's read cache can briefly serve the previous version; retry with backoff.
            raise StaleReadError(f"KB has versionKey {content.get('versionKey')}, event has {event.version}")

        outcome = await self.kb_indexer.index(content)
        return HandleResult(outcome)
