"""Remove a course everywhere in the CBP database when it is retired.

Event: {"identifier": "do_12345", "operationType": "RETIRED"}.

Nothing on the retirement topic is silently ignored: a message that fails validation (not a JSON
object, operationType other than RETIRED, missing or non do_ identifier, empty message) raises
InvalidEventError and goes straight to the DLQ, so it can be inspected and fixed. The topic must
therefore carry retirement events only.

For each retired course (same order as cbp-ai-service's scripts/remove_retired_courses.py):

    1. course_metadata_weightage_v2: the embeddings row is deleted, so it can't be recommended again.
    2. recommended_courses, suggested_courses, cbp_plans, approval_request_items (all statuses):
       the course's entries are taken out of their JSON arrays, 500 rows per transaction
       (references.py).

Every step is idempotent. Retries, DLQ and offset commits are handled by the common worker.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from ..common.base_pipeline import BasePipeline
from ..common.course_repository import COURSE_TABLE
from ..common.errors import InvalidEventError
from ..common.schema import ensure_schema
from ..common.types import HandleResult, Outcome
from .references import remove_course_references

RETIRED_OPERATION = "RETIRED"
CONTENT_ID_RE = re.compile(r"^do_\w+$")  # KB course id, e.g. do_12345


@dataclass(frozen=True)
class RetirementEvent:
    """A parsed retirement message: {"identifier": "do_12345", "operationType": "RETIRED"}."""

    identifier: str  # content id of the retired course, e.g. "do_12345"
    operation_type: str = RETIRED_OPERATION


class CourseRetirementPipeline(BasePipeline[RetirementEvent]):
    name = "course_retirement"
    needs_embeddings = False

    def topics(self) -> list[str]:
        return [self.settings.require("KAFKA_COURSE_RETIREMENT_TOPIC")]

    async def on_start(self) -> None:
        # Needed when this is the first pipeline started against an empty database.
        await ensure_schema(self.ctx.pool, COURSE_TABLE, self.settings.EMBEDDING_OUTPUT_DIMENSIONALITY)

    def parse(self, payload: Any) -> RetirementEvent:
        """Returns the retirement event. Never returns None: an invalid message raises
        InvalidEventError, which the worker sends straight to the DLQ."""
        if not isinstance(payload, dict):
            raise InvalidEventError(f"expected a JSON object, got {type(payload).__name__}")
        raw_operation = payload.get("operationType")
        operation_type = str(raw_operation or "").strip().upper()
        if operation_type != RETIRED_OPERATION:
            raise InvalidEventError(f"operationType must be {RETIRED_OPERATION}, got {raw_operation!r}")
        raw_identifier = payload.get("identifier")
        if not isinstance(raw_identifier, str) or not raw_identifier.strip():
            raise InvalidEventError(f"identifier is missing or not a string: {raw_identifier!r}")
        identifier = raw_identifier.strip()
        if not CONTENT_ID_RE.match(identifier):
            raise InvalidEventError(f"identifier must look like do_<id>, got {identifier!r}")
        return RetirementEvent(identifier=identifier, operation_type=operation_type)

    # dedupe_key() and describe() come from BasePipeline, which uses event.identifier.

    async def handle(self, event: RetirementEvent) -> HandleResult:
        # 1. Embeddings row first (as in the batch retire script), so a recommendation
        #    run during the cleanup cannot bring the course back.
        removed = await self.ctx.repository.delete([event.identifier])
        # 2. Recommendations, suggestions, plans, approval snapshots (all statuses).
        cleanup = await remove_course_references(self.ctx.pool, event.identifier)
        outcome = Outcome.DELETED if removed or cleanup.rows else Outcome.SKIPPED
        return HandleResult(outcome, f"{COURSE_TABLE} {removed} row(s); references: {cleanup.summary()}")
