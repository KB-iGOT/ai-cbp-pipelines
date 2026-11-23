"""Deletes from the course embeddings table.

The table has no unique constraint on ``identifier``, so writes take a transaction-scoped
advisory lock per identifier (the same lock the ingestion indexer takes when it replaces a row).
"""
from __future__ import annotations

import asyncpg

# The course embeddings table the app's recommendation search reads.
# Fixed in code on purpose (not configurable).
COURSE_TABLE = "course_metadata_weightage_v2"


def _affected(status: str) -> int:
    # asyncpg returns e.g. "DELETE 3"
    try:
        return int(status.split()[-1])
    except (ValueError, IndexError):
        return 0


class CourseRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def delete(self, identifiers: list[str]) -> int:
        identifiers = sorted({i for i in identifiers if i})
        if not identifiers:
            return 0
        async with self._pool.acquire() as conn, conn.transaction():
            # Locks taken in sorted order so concurrent deletes can't deadlock.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(i)) FROM unnest($1::text[]) AS t(i) ORDER BY i", identifiers
            )
            status = await conn.execute(f"DELETE FROM {COURSE_TABLE} WHERE identifier = ANY($1::text[])", identifiers)
        return _affected(status)
