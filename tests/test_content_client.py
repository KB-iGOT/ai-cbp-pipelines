import asyncio

import httpx
import pytest

from src.common.content_client import ContentClient
from src.common.errors import FatalPipelineError, NonRetryableError

BODY = {"request": {"filters": {"identifier": ["do_1"]}, "limit": 1}}


async def no_sleep(_):
    return None


def client_with(responses):
    calls = []

    def handler(request):
        calls.append(request)
        status, body = responses.pop(0)
        return httpx.Response(status, json=body)

    client = ContentClient("https://kb.example/", "Bearer t", retries=3,
                           transport=httpx.MockTransport(handler), sleep=no_sleep)
    return client, calls


def test_search_content_retries_5xx_then_returns_result():
    client, calls = client_with([
        (503, {}),
        (200, {"result": {"content": [{"identifier": "do_1", "status": "Live"}], "count": 1}}),
    ])
    result = asyncio.run(client.search_content(BODY))
    assert result["content"][0]["status"] == "Live"
    assert len(calls) == 2
    assert calls[0].headers["Authorization"] == "Bearer t"
    assert calls[0].url.path == "/api/composite/v4/search"


def test_search_content_without_result_returns_empty_dict():
    client, _ = client_with([(200, {"id": "api.search"})])
    assert asyncio.run(client.search_content(BODY)) == {}


def test_auth_failure_is_fatal():
    client, _ = client_with([(401, {})])
    with pytest.raises(FatalPipelineError):
        asyncio.run(client.search_content(BODY))


def test_bad_request_is_not_retried():
    client, calls = client_with([(400, {"err": "bad"})])
    with pytest.raises(NonRetryableError):
        asyncio.run(client.search_content(BODY))
    assert len(calls) == 1


def test_read_content_returns_content_with_fields():
    client, calls = client_with([
        (200, {"responseCode": "OK", "result": {"content": {"identifier": "do_1", "status": "Live"}}}),
    ])
    item = asyncio.run(client.read_content("do_1", ["name", "status"]))
    assert item == {"identifier": "do_1", "status": "Live"}
    assert calls[0].method == "GET"
    assert calls[0].url.path == "/api/content/v4/read/do_1"
    assert calls[0].url.params["fields"] == "name,status"
    assert calls[0].headers["Authorization"] == "Bearer t"


def test_read_content_not_found_returns_none_without_retry():
    client, calls = client_with([(404, {"responseCode": "RESOURCE_NOT_FOUND"})])
    assert asyncio.run(client.read_content("do_x")) is None
    assert len(calls) == 1


def test_read_content_retries_5xx():
    client, calls = client_with([(502, {}), (200, {"result": {"content": {"identifier": "do_1"}}})])
    assert asyncio.run(client.read_content("do_1"))["identifier"] == "do_1"
    assert len(calls) == 2


def test_read_content_auth_failure_is_fatal():
    client, _ = client_with([(403, {})])
    with pytest.raises(FatalPipelineError):
        asyncio.run(client.read_content("do_1"))
