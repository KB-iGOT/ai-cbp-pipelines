from __future__ import annotations

import asyncio
from dataclasses import dataclass

from google import genai
from google.genai import types

from .errors import EmbeddingError


@dataclass(frozen=True)
class Embedding:
    values: list[float]
    billable_character_count: int | None = None


class EmbeddingClient:
    """Gemini embeddings with a process-wide concurrency cap and SDK-level retries on 429/5xx."""

    def __init__(self, api_key: str, model: str, dimensions: int, concurrency: int = 20) -> None:
        if not api_key:
            raise RuntimeError("GOOGLE_API_KEY must be set for pipelines that generate embeddings")
        self._client = genai.Client(
            api_key=api_key,
            vertexai=False,
            http_options=types.HttpOptions(
                retry_options=types.HttpRetryOptions(
                    attempts=3, initial_delay=1.0, exp_base=2.0, http_status_codes=[429, 500, 502, 503, 504]
                )
            ),
        )
        self._model = model
        self._dimensions = dimensions
        self._semaphore = asyncio.Semaphore(concurrency)

    async def embed(self, text: str) -> Embedding:
        async with self._semaphore:
            response = await self._client.aio.models.embed_content(
                model=self._model,
                contents=text,
                config=types.EmbedContentConfig(output_dimensionality=self._dimensions),
            )
        if not response or not response.embeddings or not response.embeddings[0].values:
            raise EmbeddingError(f"No embedding returned for text: {text[:60]}...")
        metadata = response.metadata
        return Embedding(
            values=list(response.embeddings[0].values),
            billable_character_count=getattr(metadata, "billable_character_count", None) if metadata else None,
        )
