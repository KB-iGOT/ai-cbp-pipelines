"""Remove a retired course from the tables that reference it inside JSON arrays.

Same logic, SQL and batching as cbp-ai-service's ``scripts/remove_retired_courses.py``, for one content_id per
Kafka event:

    1. recommended_courses      filtered_courses[] entries (actual_courses is left as is)
    2. suggested_courses        course_identifiers[] (array of bare id strings)
    3. cbp_plans                selected_courses[] entries
    4. approval_request_items   cbp_plan_data[].selected_courses[] entries -- ALL statuses

(``course_metadata_weightage_v2`` is deleted by the pipeline before this runs, as in the script.)

Only the matching entries are taken out of each JSON array; everything else and the order stay.

Keeping the load on Postgres low:
  - One cleanup at a time per process (_CLEANUP_LOCK): events handled in parallel by the worker
    queue here, without holding a DB connection, instead of scanning the same tables at once.
  - Each table is scanned ONCE per course, returning only row ids, using JSONB containment
    (``col @> '[{"identifier": "do_x"}]'``). Containment is cheaper than expanding every array
    element, and a GIN (jsonb_path_ops) index on the column turns the scan into an index lookup
    (see docs/course_retirement_pipeline_design.md). The id is a normal bind parameter.
  - Scans run with SET LOCAL statement_timeout, so a slow scan is cut off and retried instead of
    running on.
  - Rows are re-read, rewritten and updated in batches of BATCH_SIZE, each batch in its own short
    transaction locking only its own rows (SELECT ... FOR UPDATE), with BATCH_PAUSE_SECONDS
    between batches to spread WAL / replication / autovacuum load. Unlike the script, the lock is
    taken in id order: several events are handled in parallel, and a fixed order means two of
    them touching the same plan wait for each other instead of deadlocking.
  - Every write transaction starts with SET LOCAL lock_timeout: if the app holds a lock on a row,
    the batch fails fast instead of queueing. Transaction-scoped, so safe behind PgBouncer.
  - If a batch fails the error goes to the worker, which retries the event (then DLQ); committed
    batches stay committed and a re-run is safe (removed entries are simply no longer found).

JSON values are exchanged as text (asyncpg's default for json/jsonb), so the shared pool's type
codecs are left untouched.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Set, Tuple

import asyncpg

from ..common.log import get_logger

logger = get_logger("course_retirement.references")

APPROVAL_TABLE = "approval_request_items"

BATCH_SIZE = 500  # rows locked + rewritten per transaction
BATCH_PAUSE_SECONDS = 0.1  # between batches
LOCK_TIMEOUT = "5s"  # per write transaction: fail fast if the app holds a row
SCAN_TIMEOUT = "120s"  # per scan statement

# Only one cleanup runs at a time in this process (see remove_course_references).
_CLEANUP_LOCK = asyncio.Lock()


async def _begin_write(conn) -> None:
    """First statement of every write transaction (see module docstring)."""
    await conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")


@dataclass
class TableResult:
    rows: int = 0  # DB rows containing the course (rewritten)
    removed: int = 0  # entries removed


@dataclass
class CleanupResult:
    tables: Dict[str, TableResult] = field(default_factory=dict)

    @property
    def rows(self) -> int:
        return sum(r.rows for r in self.tables.values())

    def summary(self) -> str:
        parts = [f"{t} {r.rows}/{r.removed}" for t, r in self.tables.items() if r.rows]
        return ", ".join(parts) + " (rows/entries)" if parts else "none"


# {content_id: entries removed} for the DB row currently being rewritten.
RowHits = Dict[str, int]


# --- Removing entries from JSON arrays -----------------------------------------------------

def remove_from_list(values, targets: Set[str], hits: RowHits,
                     bare_ids: bool = False) -> Tuple[Any, bool]:
    """Drop the entries whose identifier is in `targets`, order kept. `values` holds course
    objects, or bare id strings when `bare_ids`. A non-list value is returned untouched."""
    if not isinstance(values, list):
        return values, False
    result = []
    for value in values:
        if bare_ids:
            content_id = str(value).strip() if value is not None else None
        else:
            content_id = (str(value.get("identifier")).strip()
                          if isinstance(value, dict) and value.get("identifier") is not None
                          else None)
        if content_id in targets:
            hits[content_id] += 1
        else:
            result.append(value)
    return result, len(result) != len(values)


def remove_from_plan_data(data, targets: Set[str], hits: RowHits) -> Tuple[Any, bool]:
    """cbp_plan_data is a list of plan snapshots (a single object is tolerated), each with its
    own selected_courses list."""
    plans = data if isinstance(data, list) else [data]
    new_plans, changed = [], False
    for plan in plans:
        if isinstance(plan, dict):
            courses, plan_changed = remove_from_list(plan.get("selected_courses"), targets, hits)
            if plan_changed:
                plan = {**plan, "selected_courses": courses}
                changed = True
        new_plans.append(plan)
    return (new_plans if isinstance(data, list) else new_plans[0]), changed


# --- JSON-array tables ---------------------------------------------------------------------
#
# scan_sql   one pass over the table for the content_id(s) -> matching row ids
# fetch_sql  full rows for one batch of ids ($1 uuid[]); " ORDER BY id FOR UPDATE" is appended
# rewrite    row -> new column values in update_sql's $2.. order, or None if nothing to remove
# update_sql one row's UPDATE, sent with executemany for the whole batch

def _json(value: Any) -> Any:
    """asyncpg returns json/jsonb as text by default."""
    return json.loads(value) if isinstance(value, str) else value


def _dump(value: Any) -> str:
    # Keep non-ASCII (e.g. Hindi course names) as-is rather than \u-escaping it.
    return json.dumps(value, ensure_ascii=False)


@dataclass
class TableSpec:
    table: str
    column: str
    scan_sql: str
    scan_args: Callable[[str], tuple]  # content_id -> scan_sql bind values (jsonb text)
    fetch_sql: str
    update_sql: str
    rewrite: Callable[[Any, Set[str], RowHits], Optional[tuple]]


def _course_list(content_id: str) -> tuple:
    """[{"identifier": id}] - matches an array holding a course object with that identifier."""
    return (_dump([{"identifier": content_id}]),)


def _rewrite_column(column: str, bare_ids: bool = False):
    def rewrite(row, targets, hits):
        values, changed = remove_from_list(_json(row[column]), targets, hits, bare_ids)
        return (_dump(values),) if changed else None
    return rewrite


def _rewrite_approval_item(row, targets, hits):
    data, changed = remove_from_plan_data(_json(row["data"]), targets, hits)
    return (_dump(data),) if changed else None


TABLE_SPECS = [
    TableSpec(
        table="recommended_courses",
        column="filtered_courses",
        scan_sql="SELECT id FROM recommended_courses WHERE filtered_courses @> $1::jsonb",
        scan_args=_course_list,
        fetch_sql="SELECT id, filtered_courses FROM recommended_courses WHERE id = ANY($1::uuid[])",
        update_sql="""UPDATE recommended_courses
                         SET filtered_courses = $2::jsonb, updated_at = now() WHERE id = $1""",
        rewrite=_rewrite_column("filtered_courses"),
    ),
    TableSpec(
        table="suggested_courses",
        column="course_identifiers",
        scan_sql="SELECT id FROM suggested_courses WHERE course_identifiers @> $1::jsonb",
        scan_args=lambda content_id: (_dump([content_id]),),
        fetch_sql="SELECT id, course_identifiers FROM suggested_courses WHERE id = ANY($1::uuid[])",
        update_sql="""UPDATE suggested_courses
                         SET course_identifiers = $2::jsonb, updated_at = now() WHERE id = $1""",
        rewrite=_rewrite_column("course_identifiers", bare_ids=True),
    ),
    TableSpec(
        table="cbp_plans",
        column="selected_courses",
        scan_sql="SELECT id FROM cbp_plans WHERE selected_courses @> $1::jsonb",
        scan_args=_course_list,
        fetch_sql="SELECT id, selected_courses FROM cbp_plans WHERE id = ANY($1::uuid[])",
        update_sql="""UPDATE cbp_plans
                         SET selected_courses = $2::jsonb, updated_at = now() WHERE id = $1""",
        rewrite=_rewrite_column("selected_courses"),
    ),
    TableSpec(
        table=APPROVAL_TABLE,
        column="cbp_plan_data",
        # All statuses. cbp_plan_data is JSON (not JSONB); a single snapshot object counts as a
        # one-item list.
        # $1 matches the list-of-snapshots shape, $2 a single snapshot object.
        scan_sql="""SELECT id FROM approval_request_items
                     WHERE cbp_plan_data::jsonb @> $1::jsonb OR cbp_plan_data::jsonb @> $2::jsonb""",
        scan_args=lambda content_id: (
            _dump([{"selected_courses": [{"identifier": content_id}]}]),
            _dump({"selected_courses": [{"identifier": content_id}]}),
        ),
        fetch_sql="""SELECT a.id, a.cbp_plan_data::jsonb AS data
                       FROM approval_request_items a
                      WHERE a.id = ANY($1::uuid[])""",
        # No updated_at column on this table; the value is written back as json.
        update_sql="UPDATE approval_request_items SET cbp_plan_data = $2::json WHERE id = $1",
        rewrite=_rewrite_approval_item,
    ),
]


async def process_table(conn, spec: TableSpec, targets: Set[str], total: TableResult) -> None:
    """Scan the table once, then rewrite and update the matching rows batch by batch."""
    (content_id,) = targets
    started = time.monotonic()
    async with conn.transaction():
        # Cap a runaway scan; the worker retries the event if it is hit.
        await conn.execute(f"SET LOCAL statement_timeout = '{SCAN_TIMEOUT}'")
        ids = [r["id"] for r in await conn.fetch(spec.scan_sql, *spec.scan_args(content_id))]
    if not ids:
        logger.debug(f"{spec.table}: no matching rows (scan {time.monotonic() - started:.2f}s)")
        return
    logger.info(f"{spec.table}: {len(ids)} matching rows (scan {time.monotonic() - started:.1f}s)")

    fetch_sql = spec.fetch_sql + " ORDER BY id FOR UPDATE"

    async def apply_batch(batch):
        updates = []
        for row in await conn.fetch(fetch_sql, batch):
            hits: RowHits = defaultdict(int)
            values = spec.rewrite(row, targets, hits)
            if values is None:
                continue  # changed since the scan; nothing left to remove
            updates.append((row["id"], *values))
            logger.debug(f"{spec.table} id={row['id']} {spec.column}: removed "
                         + ", ".join(f"{cid} x{n}" if n > 1 else cid for cid, n in hits.items()))
            total.removed += sum(hits.values())
        if updates:
            await conn.executemany(spec.update_sql, updates)
        total.rows += len(updates)

    for start in range(0, len(ids), BATCH_SIZE):
        batch = ids[start:start + BATCH_SIZE]
        batch_started = time.monotonic()
        try:
            async with conn.transaction():
                await _begin_write(conn)
                await apply_batch(batch)
        except Exception:
            logger.error(f"{spec.table}: batch {start + 1}-{start + len(batch)} failed and was "
                         f"rolled back; earlier batches are committed, re-running is safe.")
            raise
        logger.info(f"{spec.table}: {start + len(batch)}/{len(ids)} rows processed "
                    f"({time.monotonic() - batch_started:.1f}s)")
        if BATCH_PAUSE_SECONDS and start + BATCH_SIZE < len(ids):
            await asyncio.sleep(BATCH_PAUSE_SECONDS)


async def remove_course_references(pool: asyncpg.Pool, identifier: str) -> CleanupResult:
    """Remove `identifier` from the four JSON-array tables. The id is validated in
    CourseRetirementPipeline.parse(), before this is called."""
    result = CleanupResult()
    targets = {identifier}
    # One cleanup at a time per process: parallel events queue here (without holding a DB
    # connection) instead of scanning the same tables concurrently.
    async with _CLEANUP_LOCK:
        async with pool.acquire() as conn:
            for spec in TABLE_SPECS:
                result.tables[spec.table] = TableResult()
                await process_table(conn, spec, targets, result.tables[spec.table])
    return result
