"""HTTP client for the KB content APIs: composite search and content read."""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

import httpx

from .errors import FatalPipelineError, NonRetryableError
from .log import get_logger

COMPOSITE_SEARCH_PATH = "/api/composite/v4/search"
CONTENT_READ_PATH = "/api/content/v4/read"

log = get_logger("content_client")


class ContentClient:
    def __init__(
        self,
        base_url: str,
        auth_token: str,
        *,
        timeout: float = 60.0,
        retries: int = 2,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers={"Content-Type": "application/json", "Authorization": auth_token},
            transport=transport,
        )
        self._retries = max(1, retries)
        self._sleep = sleep

    async def aclose(self) -> None:
        await self._client.aclose()

    async def search_content(self, request_body: dict[str, Any]) -> dict[str, Any]:
        """POST ``request_body`` to KB composite search and return its ``result``
        (``{"content": [...], "count": N}``), or ``{}`` when the response has none.

        Example body: ``{"request": {"filters": {...}, "fields": [...], "offset": 0, "limit": 1}}``
        """
        body = await self._request("POST", COMPOSITE_SEARCH_PATH, json=request_body)
        return body.get("result") or {}

    async def read_content(self, identifier: str, fields: list[str] | None = None) -> dict[str, Any] | None:
        """GET one content item from the KB content read API, or None if it doesn't exist (404).

        ``fields`` limits the response to those fields (the full item has ~90). Unlike search,
        read is served from the content store, so it sees a publish immediately.
        """
        params = {"fields": ",".join(fields)} if fields else None
        body = await self._request("GET", f"{CONTENT_READ_PATH}/{identifier}", params=params, not_found_ok=True)
        if body is None:
            return None
        return (body.get("result") or {}).get("content") or None

    async def _request(
        self, method: str, path: str, *, not_found_ok: bool = False, **kwargs: Any
    ) -> dict[str, Any] | None:
        """Send with retries. 401/403 → FatalPipelineError; 404 → None when ``not_found_ok``;
        other 4xx → NonRetryableError; network errors, 429 and 5xx are retried."""
        last_error: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                response = await self._client.request(method, path, **kwargs)
            except httpx.TransportError as exc:
                last_error = exc
            else:
                code = response.status_code
                if code in (401, 403):
                    raise FatalPipelineError(f"KB API rejected credentials ({code}) on {path}; check KB_AUTH_TOKEN")
                if code == 404 and not_found_ok:
                    return None
                if code == 429 or code >= 500:
                    last_error = RuntimeError(f"{path} returned {code}")
                elif code >= 400:
                    raise NonRetryableError(f"{path} returned {code}: {response.text[:300]}")
                else:
                    return response.json()
            if attempt < self._retries:
                log.warning("%s %s failed (attempt %d/%d): %s", method, path, attempt, self._retries, last_error)
                await self._sleep(2 ** attempt)
        assert last_error is not None
        raise last_error
