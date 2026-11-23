"""KB course lookup and indexing for the course_ingestion pipeline.

Same rules as cbp-ai-service's scripts/course_embedding_pipeline.py: the course metadata is read
from the KB content read API, embedded, and written as one row (missing values become NULL).
"""
from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import uuid4

import asyncpg

from ..common.content_client import ContentClient
from ..common.embedding_client import EmbeddingClient
from ..common.errors import NonRetryableError
from ..common.types import Outcome
from .utils import (
    build_combined_text,
    build_description_text,
    build_keywords_text,
    normalise_level,
)

# Fields requested from the KB content read API for one course: what gets embedded and stored,
# plus status and categories so the pipeline can tell retired or non-course content apart.
LOOKUP_FIELDS = [
    "name", "identifier", "description", "keywords", "organisation", "competencies_v6",
    "language", "duration", "instructions", "difficultyLevel", "source", "purpose", "versionKey",
    "courseLevel", "status", "primaryCategory", "courseCategory",
]


async def get_kb_course(content: ContentClient, identifier: str) -> dict[str, Any] | None:
    """The course as stored in KB now (content read API, LOOKUP_FIELDS only), or None if KB has
    no content with this identifier."""
    return await content.read_content(identifier, LOOKUP_FIELDS)


def _version(course: dict[str, Any]) -> str | None:
    value = course.get("versionKey")
    return str(value) if value else None


def _list(value: Any) -> list[str]:
    return [str(v) for v in value] if isinstance(value, list) else []


def _str_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


class KbCourseIndexer:
    """Embeds one KB course and replaces its metadata row."""

    def __init__(self, pool: asyncpg.Pool, table: str, embedder: EmbeddingClient) -> None:
        self._pool = pool
        self._table = table
        self._embedder = embedder
        # description_tsv is GENERATED ALWAYS, so it must not be named in the INSERT.
        self._insert_sql = f"""
            INSERT INTO {table} (
                id, identifier, name, keywords, competencies_v6,
                instructions, description, language, difficulty_level,
                duration, organisation, token_count,
                description_embedding, keywords_embedding, combined_embedding,
                version_key, course_type, course_level
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11, $12,
                      $13::vector, $14::vector, $15::vector, $16, $17, $18)
        """

    async def index(self, course: dict[str, Any]) -> Outcome:
        identifier = course.get("identifier")
        if not identifier:
            raise NonRetryableError("course has no identifier")

        version = _version(course)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT version_key FROM {self._table} WHERE identifier = $1 LIMIT 1", identifier
            )
        if row is not None and version is not None and row["version_key"] == version:
            return Outcome.UNCHANGED

        description, keywords, combined = await asyncio.gather(
            self._embedder.embed(build_description_text(course)),
            self._embedder.embed(build_keywords_text(course)),
            self._embedder.embed(build_combined_text(course)),
        )

        competencies = course.get("competencies_v6")
        async with self._pool.acquire() as conn, conn.transaction():
            # Same advisory lock as CourseRepository, so workers and jobs never insert duplicates.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", identifier)
            deleted = await conn.execute(f"DELETE FROM {self._table} WHERE identifier = $1", identifier)
            await conn.execute(
                self._insert_sql,
                str(uuid4()), identifier, (course.get("name") or "").strip(), _list(course.get("keywords")),
                json.dumps(competencies) if competencies else None,
                course.get("instructions"), course.get("description"), _list(course.get("language")),
                _str_or_none(course.get("difficultyLevel")), _str_or_none(course.get("duration")),
                _list(course.get("organisation")), description.billable_character_count,
                str(description.values), str(keywords.values), str(combined.values),
                version, course.get("courseType"), normalise_level(course.get("courseLevel")),
            )
        return Outcome.UPDATED if deleted.split()[-1] != "0" else Outcome.INSERTED
