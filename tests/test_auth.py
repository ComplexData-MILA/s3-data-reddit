"""Tests for RedditAuth (paste-back flow, refresh, caching)."""

from __future__ import annotations

import asyncio
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from reddit_firehose_tool.auth import AuthError, RedditAuth, TokenRevokedError
from reddit_firehose_tool.state import FirehoseState
from tests.conftest import FakeClock, FakeStdin, FakeStdout, make_handler

TOKEN_RESPONSE = {
    "access_token": "at-123",
    "token_type": "bearer",
    "expires_in": 3600,
    "scope": "read",
    "refresh_token": "rt-456",
}


def make_auth(state, responses=(), *, stdin=None, stdout=None, clock=None):
    client = httpx.AsyncClient(transport=httpx.MockTransport(make_handler(list(responses))))
    return RedditAuth(
        client_id="cid",
        client_secret="csec",
        redirect_uri="http://localhost:8080",
        user_agent="test-agent",
        state=state,
        client=client,
        stdin=stdin if stdin is not None else FakeStdin(tty=False),
        stdout=stdout if stdout is not None else FakeStdout(),
        clock=clock if clock is not None else FakeClock(),
    )


def test_build_authorize_url():
    url = RedditAuth.build_authorize_url("cid", "http://localhost:8080", "st")
    parsed = urlparse(url)
    assert parsed.scheme == "https" and parsed.netloc == "www.reddit.com"
    query = parse_qs(parsed.query)
    assert query["client_id"] == ["cid"]
    assert query["response_type"] == ["code"]
    assert query["state"] == ["st"]
    assert query["redirect_uri"] == ["http://localhost:8080"]
    assert query["duration"] == ["permanent"]
    assert query["scope"] == ["read"]


def test_parse_callback_url():
    ok = "http://localhost:8080/?state=st&code=abc123"
    assert RedditAuth.parse_callback_url(ok, "st") == "abc123"
    assert RedditAuth.parse_callback_url("garbage", "st") is None
    assert RedditAuth.parse_callback_url("http://localhost:8080/?code=abc", "st") is None
    with pytest.raises(AuthError, match="denied"):
        RedditAuth.parse_callback_url("http://localhost:8080/?state=st&error=access_denied", "st")
    with pytest.raises(AuthError, match="state"):
        RedditAuth.parse_callback_url("http://localhost:8080/?state=other&code=abc", "st")


def test_client_credentials_grant_caches_and_persists_nothing(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"access_token": "at-app", "expires_in": 86400})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    auth = RedditAuth(
        "cid", "csec", "http://localhost:8080", "test-agent", state, client,
        stdin=FakeStdin(tty=False), stdout=FakeStdout(), clock=FakeClock(),
    )
    assert asyncio.run(auth.get_access_token()) == "at-app"
    # cached: a second call makes no new request
    assert asyncio.run(auth.get_access_token()) == "at-app"
    assert len(requests) == 1
    assert "grant_type=client_credentials" in requests[0].content.decode()
    assert requests[0].headers["authorization"].startswith("Basic ")
    # nothing persisted: no refresh token involved
    assert state.refresh_token is None
    assert not state.path.exists()


def test_client_credentials_unsupported_falls_back_to_interactive(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    auth = make_auth(
        state, responses=[(400, {"error": "unsupported_grant_type"})]
    )
    # non-TTY stdin: the paste-back flow cannot run, so a clear error is raised
    with pytest.raises(AuthError, match="interactive"):
        asyncio.run(auth.get_access_token())


def test_client_credentials_grant_error_raises(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    auth = make_auth(state, responses=[(401, {"error": "invalid_grant"})])
    with pytest.raises(AuthError, match="client_credentials"):
        asyncio.run(auth.get_access_token())


def test_refresh_flow_and_caching(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    state.set_refresh_token("rt-old")
    clock = FakeClock()
    auth = make_auth(
        state,
        responses=[(200, TOKEN_RESPONSE)],
        clock=clock,
    )
    access = asyncio.run(auth.get_access_token())
    assert access == "at-123"
    # second call hits the cache (within the safety margin): no new request needed
    assert asyncio.run(auth.get_access_token()) == "at-123"
    # forced call bypasses the cache and refreshes again
    auth = make_auth(
        state,
        responses=[(200, TOKEN_RESPONSE)],
        clock=clock,
    )
    assert asyncio.run(auth.get_access_token(force=True)) == "at-123"


def test_refresh_sends_basic_auth_and_form(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    state.set_refresh_token("rt")
    seen = {}

    def handler(request):
        seen["authorization"] = request.headers.get("authorization")
        seen["content"] = request.content.decode()
        return httpx.Response(200, json=TOKEN_RESPONSE)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    auth = RedditAuth(
        "cid", "csec", "http://localhost:8080", "test-agent", state, client,
        stdin=FakeStdin(tty=False), stdout=FakeStdout(), clock=FakeClock(),
    )
    asyncio.run(auth.get_access_token())
    assert seen["authorization"].startswith("Basic ")
    assert "grant_type=refresh_token" in seen["content"]
    assert "refresh_token=rt" in seen["content"]


def test_invalid_grant_clears_token_and_falls_back_to_app_token(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    state.set_refresh_token("rt")
    auth = make_auth(
        state,
        responses=[
            (400, {"error": "invalid_grant"}),  # refresh token revoked
            (200, {"access_token": "at-app", "expires_in": 86400}),  # client_credentials
        ],
    )
    assert asyncio.run(auth.get_access_token()) == "at-app"
    assert state.refresh_token is None  # revoked token was cleared


def test_refresh_network_error_propagates(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    state.set_refresh_token("rt")
    auth = make_auth(state, responses=[httpx.ConnectError("boom")])
    with pytest.raises(httpx.ConnectError):
        asyncio.run(auth.get_access_token())
    assert state.refresh_token == "rt"  # untouched: not a revocation


def test_paste_back_flow_exchanges_code_and_persists(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    stdout = FakeStdout()

    class EchoStdin(FakeStdin):
        """Pastes back a redirect URL carrying the state token that was just printed."""

        def __init__(self, out):
            super().__init__(tty=True)
            self._out = out

        def readline(self):
            printed = self._out.lines[-1]
            url = printed.split(":", 1)[1].strip()
            state_token = parse_qs(urlparse(url).query)["state"][0]
            return f"http://localhost:8080/?state={state_token}&code=the-code\n"

    auth = make_auth(
        state,
        responses=[
            (400, {"error": "unsupported_grant_type"}),  # no client_credentials here
            (200, TOKEN_RESPONSE),  # the code exchange succeeds
        ],
        stdin=EchoStdin(stdout),
        stdout=stdout,
    )
    access = asyncio.run(auth.get_access_token())
    assert access == "at-123"
    assert state.refresh_token == "rt-456"
    # the refresh token was persisted to the state file
    assert FirehoseState.load(state.path).refresh_token == "rt-456"


def test_non_tty_without_token_raises_clear_error(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    auth = make_auth(
        state,
        responses=[(400, {"error": "unsupported_grant_type"})],
        stdin=FakeStdin(tty=False),
    )
    with pytest.raises(AuthError, match="interactive"):
        asyncio.run(auth.get_access_token())
