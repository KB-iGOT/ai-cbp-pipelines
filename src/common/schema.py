"""Table, extension and index DDL, moved here from cbp-ai-service's course_embedding_pipeline.py.

Everything is IF NOT EXISTS, so running it at every worker start is cheap once the
table exists. The first run on a large table will lock it while indexes build.
"""
from __future__ import annotations

import asyncpg

from .log import get_logger

log = get_logger("schema")


def _index_name(table: str, suffix: str) -> str:
    return f"{table.split('.')[-1]}_{suffix}"


async def ensure_schema(pool: asyncpg.Pool, table: str, dimensions: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id uuid,
                identifier text,
                name text,
                keywords text[],
                competencies_v6 jsonb,
                instructions text,
                description text,
                language text[],
                difficulty_level text,
                organisation text[],
                token_count double precision,
                description_embedding vector({dimensions}),
                keywords_embedding vector({dimensions}),
                combined_embedding vector({dimensions}),
                duration text,
                version_key text,
                description_tsv tsvector GENERATED ALWAYS AS (
                    to_tsvector('english'::regconfig, COALESCE(description, ''::text))
                ) STORED,
                course_type text,
                course_level text
            );
        """)
        # Columns added after the table was first created.
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS course_type text;")
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS course_level text;")

        indexes = {
            "keywords_gin_idx": "USING gin (keywords)",
            "organisation_gin_idx": "USING gin (organisation)",
            "language_gin_idx": "USING gin (language)",
            "competencies_gin_idx": "USING gin (competencies_v6)",
            "description_tsv_idx": "USING gin (description_tsv)",
            "identifier_idx": "USING btree (identifier)",
            "course_level_idx": "USING btree (course_level)",
            "name_trgm_idx": "USING gin (name gin_trgm_ops)",
            "desc_emb_hnsw_idx": "USING hnsw (description_embedding vector_cosine_ops)",
            "kw_emb_hnsw_idx": "USING hnsw (keywords_embedding vector_cosine_ops)",
            "comb_emb_hnsw_idx": "USING hnsw (combined_embedding vector_cosine_ops)",
        }
        for suffix, definition in indexes.items():
            await conn.execute(f"CREATE INDEX IF NOT EXISTS {_index_name(table, suffix)} ON {table} {definition};")
    log.info("Schema for %s is ready", table)
