"""Tests for RedditClient (request shape, retries, pacing)."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from reddit_firehose_tool.client import RedditAPIError, RedditClient
from tests.conftest import FakeClock, FakeSleeper, make_handler, make_listing

LISTING = make_listing([{"kind": "t3", "data": {"name": "t3_x", "created_utc": 1.0}}])


async def fake_token_provider(force: bool = False) -> str:
    return "tok"


def make_client(responses, *, token_provider=fake_token_provider, **kwargs):
    client = httpx.AsyncClient(transport=httpx.MockTransport(make_handler(list(responses))))
    return RedditClient(
        client,
        token_provider,
        "test-agent",
        sleep=kwargs.pop("sleep", FakeSleeper()),
        clock=kwargs.pop("clock", FakeClock()),
        max_retries=kwargs.pop("max_retries", 3),
        backoff_base=kwargs.pop("backoff_base", 1.0),
        backoff_cap=kwargs.pop("backoff_cap", 30.0),
        **kwargs,
    )


def run(coro):
    return asyncio.run(coro)


def test_get_listing_sends_params_headers_and_parses():
    seen = {}

    def handler(request):
        seen["url"] = request.url
        seen["authorization"] = request.headers.get("authorization")
        seen["user_agent"] = request.headers.get("user-agent")
        return httpx.Response(200, json=LISTING)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    reddit = RedditClient(client, fake_token_provider, "test-agent")
    page = run(reddit.get_listing("Quebec", "new", limit=100, after="t3_cursor"))

    assert str(seen["url"]).startswith("https://api.reddit.com/r/Quebec/new")
    assert seen["url"].params["limit"] == "100"
    assert seen["url"].params["raw_json"] == "1"
    assert seen["url"].params["after"] == "t3_cursor"
    assert seen["authorization"] == "Bearer tok"
    assert seen["user_agent"] == "test-agent"
    assert page.after is None
    assert page.children[0]["data"]["name"] == "t3_x"


def test_get_listing_rejects_malformed_payload():
    client = make_client([(200, {"kind": "NotAListing", "data": {}})])
    with pytest.raises(RedditAPIError, match="not a Listing"):
        run(client.get_listing("Quebec", "new"))


def test_401_refreshes_once_then_retries():
    forces = []

    async def provider(force: bool = False):
        forces.append(force)
        return "tok"

    client = make_client(
        [(401, {"error": "unauthorized"}), (200, LISTING)],
        token_provider=provider,
    )
    page = run(client.get_listing("Quebec", "new"))
    assert page.children[0]["data"]["name"] == "t3_x"
    assert forces == [False, True]  # exactly one forced refresh


def test_401_without_provider_raises():
    client = make_client([(401, {"error": "unauthorized"})], token_provider=None)
    with pytest.raises(RedditAPIError, match="401"):
        run(client.get_listing("Quebec", "new"))


def test_429_honors_retry_after_and_retries():
    sleeper = FakeSleeper()
    # a 429 with a Retry-After header is retried after that many seconds
    responses = iter(
        [
            httpx.Response(429, json={"error": "ratelimit"}, headers={"retry-after": "7"}),
            httpx.Response(200, json=LISTING),
        ]
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda req: next(responses)))
    reddit = RedditClient(http, fake_token_provider, "test-agent", sleep=sleeper)
    page = run(reddit.get_listing("Quebec", "new"))
    assert page.children[0]["data"]["name"] == "t3_x"
    assert any(7.0 <= delay <= 7.3 for delay in sleeper.calls)


def test_5xx_retries_with_backoff_then_raises():
    sleeper = FakeSleeper()
    client = make_client(
        [(500, {"error": "boom"})] * 4,
        sleep=sleeper,
    )
    with pytest.raises(RedditAPIError, match="retries"):
        run(client.get_listing("Quebec", "new"))
    # exponential backoff with cap: 1, 2, 4 (+ jitter each)
    assert len(sleeper.calls) == 3
    assert sleeper.calls[0] >= 1.0
    assert sleeper.calls[1] >= sleeper.calls[0]
    assert sleeper.calls[2] >= sleeper.calls[1]


def test_connect_error_retries_then_raises():
    client = make_client([httpx.ConnectError("boom")] * 4)
    with pytest.raises(httpx.ConnectError):
        run(client.get_listing("Quebec", "new"))


def test_qpm_budget_paces_requests():
    sleeper = FakeSleeper()
    clock = FakeClock()
    client = make_client(
        [(200, LISTING)] * 3,
        sleep=sleeper,
        clock=clock,
        qpm_budget=2,
    )
    for _ in range(3):
        run(client.get_listing("Quebec", "new"))
    # the 3rd request within the same fake second must wait for the window
    assert any(delay >= 60.0 for delay in sleeper.calls)


def test_rate_limit_zero_preemptively_paces():
    sleeper = FakeSleeper()
    clock = FakeClock()
    responses = iter(
        [
            httpx.Response(
                200,
                json=LISTING,
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "12"},
            ),
            httpx.Response(200, json=LISTING),
        ]
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda req: next(responses)))
    reddit = RedditClient(http, fake_token_provider, "test-agent", sleep=sleeper, clock=clock)
    run(reddit.get_listing("Quebec", "new"))
    run(reddit.get_listing("Quebec", "new"))
    assert any(delay == 12.0 for delay in sleeper.calls)


# ---------------------------------------------------------------- get_info


def test_get_info_sends_info_url_and_params():
    seen = {}

    def handler(request):
        seen["url"] = request.url
        seen["authorization"] = request.headers.get("authorization")
        return httpx.Response(200, json=LISTING)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    reddit = RedditClient(http, fake_token_provider, "test-agent")
    children = run(reddit.get_info(["t3_x", "t1_y"]))

    assert str(seen["url"]).startswith("https://api.reddit.com/api/info")
    assert seen["url"].params["id"] == "t3_x,t1_y"
    assert seen["url"].params["raw_json"] == "1"
    assert seen["authorization"] == "Bearer tok"
    assert children[0]["data"]["name"] == "t3_x"


def test_get_info_empty_ids_makes_no_request():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(500, json={"error": "boom"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    reddit = RedditClient(http, fake_token_provider, "test-agent")
    assert run(reddit.get_info([])) == []
    assert calls == []


def test_get_info_rejects_more_than_100_ids():
    client = make_client([(200, LISTING)])
    with pytest.raises(ValueError, match="100"):
        run(client.get_info([f"t3_{i}" for i in range(101)]))


def test_get_info_rejects_malformed_payload():
    client = make_client([(200, {"kind": "NotAListing", "data": {}})])
    with pytest.raises(RedditAPIError, match="not a Listing"):
        run(client.get_info(["t3_x"]))


def test_get_info_401_refreshes_once_then_retries():
    forces = []

    async def provider(force: bool = False):
        forces.append(force)
        return "tok"

    client = make_client(
        [(401, {"error": "unauthorized"}), (200, LISTING)],
        token_provider=provider,
    )
    children = run(client.get_info(["t3_x"]))
    assert children[0]["data"]["name"] == "t3_x"
    assert forces == [False, True]  # exactly one forced refresh


def test_get_info_5xx_retries_then_raises():
    client = make_client([(500, {"error": "boom"})] * 4)
    with pytest.raises(RedditAPIError, match="retries"):
        run(client.get_info(["t3_x"]))


def test_get_info_omitted_ids_are_not_errors():
    # /api/info omits deleted/removed items: a shorter response is not an error
    subset = make_listing([{"kind": "t3", "data": {"name": "t3_x", "created_utc": 1.0}}])
    client = make_client([(200, subset)])
    children = run(client.get_info(["t3_x", "t3_gone"]))
    assert [c["data"]["name"] for c in children] == ["t3_x"]


# ------------------------------------------------------------ get_user_about

ABOUT = {"kind": "t2", "data": {"name": "user1", "id": "4gare", "created_utc": 1.0}}


def test_get_user_about_sends_url_and_returns_data():
    seen = {}

    def handler(request):
        seen["url"] = request.url
        seen["authorization"] = request.headers.get("authorization")
        return httpx.Response(200, json=ABOUT)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    reddit = RedditClient(http, fake_token_provider, "test-agent")
    data = run(reddit.get_user_about("user1"))

    assert str(seen["url"]).startswith("https://api.reddit.com/user/user1/about")
    assert seen["url"].params["raw_json"] == "1"
    assert seen["authorization"] == "Bearer tok"
    assert data["name"] == "user1"


def test_get_user_about_404_returns_none():
    client = make_client([(404, {"error": "not found"})])
    assert run(client.get_user_about("suspended_user")) is None


def test_get_user_about_403_raises():
    client = make_client([(403, {"error": "forbidden"})])
    with pytest.raises(RedditAPIError, match="403"):
        run(client.get_user_about("user1"))


def test_get_user_about_rejects_malformed_payload():
    client = make_client([(200, {"kind": "t2", "data": "nope"})])
    with pytest.raises(RedditAPIError, match="not a t2 account"):
        run(client.get_user_about("user1"))


def test_get_user_about_401_refreshes_once_then_retries():
    forces = []

    async def provider(force: bool = False):
        forces.append(force)
        return "tok"

    client = make_client(
        [(401, {"error": "unauthorized"}), (200, ABOUT)],
        token_provider=provider,
    )
    data = run(client.get_user_about("user1"))
    assert data["name"] == "user1"
    assert forces == [False, True]  # exactly one forced refresh


def test_get_user_about_429_honors_retry_after_and_retries():
    sleeper = FakeSleeper()
    responses = iter(
        [
            httpx.Response(429, json={"error": "ratelimit"}, headers={"retry-after": "7"}),
            httpx.Response(200, json=ABOUT),
        ]
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda req: next(responses)))
    reddit = RedditClient(http, fake_token_provider, "test-agent", sleep=sleeper)
    data = run(reddit.get_user_about("user1"))
    assert data["name"] == "user1"
    assert any(7.0 <= delay <= 7.3 for delay in sleeper.calls)


def test_get_user_about_5xx_retries_then_raises():
    client = make_client([(500, {"error": "boom"})] * 4)
    with pytest.raises(RedditAPIError, match="retries"):
        run(client.get_user_about("user1"))
