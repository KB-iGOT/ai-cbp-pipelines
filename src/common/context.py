"""Everything a pipeline or job needs, created once per process."""
from __future__ import annotations

from dataclasses import dataclass

import asyncpg

from .content_client import ContentClient
from .course_repository import CourseRepository
from .embedding_client import EmbeddingClient
from .settings import PipelineSettings


@dataclass
class PipelineContext:
    settings: PipelineSettings
    pool: asyncpg.Pool
    content: ContentClient
    repository: CourseRepository
    embedder: EmbeddingClient | None

    @classmethod
    async def create(cls, settings: PipelineSettings, *, with_embeddings: bool = True) -> "PipelineContext":
        embedder = (
            EmbeddingClient(
                api_key=settings.GOOGLE_API_KEY,
                model=settings.GOOGLE_EMBEDDING_MODEL,
                dimensions=settings.EMBEDDING_OUTPUT_DIMENSIONALITY,
                concurrency=settings.EMBEDDING_CONCURRENCY,
            )
            if with_embeddings
            else None
        )
        pool = await asyncpg.create_pool(
            settings.asyncpg_dsn, min_size=settings.DB_POOL_MIN_SIZE, max_size=settings.DB_POOL_MAX_SIZE
        )
        content = ContentClient(
            settings.KB_BASE_URL,
            settings.KB_AUTH_TOKEN,
            timeout=settings.KB_HTTP_TIMEOUT_SECONDS,
            retries=settings.KB_HTTP_RETRIES,
        )
        return cls(
            settings=settings,
            pool=pool,
            content=content,
            repository=CourseRepository(pool),
            embedder=embedder,
        )

    async def close(self) -> None:
        await self.content.aclose()
        await self.pool.close()
