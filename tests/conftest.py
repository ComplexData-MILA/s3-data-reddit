"""Shared fixtures and helpers for the reddit_firehose_tool tests."""

from __future__ import annotations

import json
import re
from pathlib import Path

import httpx
import pytest

EXAMPLES = Path(__file__).resolve().parent.parent / "example_data"


# ---------------------------------------------------------------- example data


def load_example(name: str):
    path = EXAMPLES / name
    if not path.exists():
        pytest.skip(f"example_data/{name} not available")
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def example_comments():
    return load_example("comments.json")


@pytest.fixture(scope="session")
def example_posts():
    return load_example("new.json")


# ---------------------------------------------------------------- record builders


def make_child(kind: str, name: str, created_utc: float, subreddit: str = "Quebec", **extra):
    data = {
        "name": name,
        "id": name[3:],
        "created_utc": created_utc,
        "subreddit": subreddit,
        "author": "user1",
        "permalink": f"/r/{subreddit}/comments/{name[3:]}",
        "score": 1,
    }
    data.update(extra)
    return {"kind": kind, "data": data}


def make_post(name: str, created_utc: float, **extra):
    return make_child(
        "t3",
        name,
        created_utc,
        title=f"title {name}",
        selftext="",
        url="https://example.com/x",
        num_comments=0,
        upvote_ratio=1.0,
        **extra,
    )


def make_comment(name: str, created_utc: float, **extra):
    return make_child(
        "t1",
        name,
        created_utc,
        body=f"body {name}",
        link_id="t3_link",
        parent_id="t3_link",
        **extra,
    )


def make_listing(children, after=None):
    return {
        "kind": "Listing",
        "data": {"after": after, "dist": len(children), "children": children},
    }


# ---------------------------------------------------------------- scripted API


class ScriptedAPI:
    """MockTransport handler serving scripted listing pages per (subreddit, endpoint).

    Pages are popped in order. When a stream runs out of pages, an empty
    listing (``after`` null) is served. A per-stream ``failures`` entry makes
    the next request raise (simulating a network error).
    """

    def __init__(self):
        self.pages = {}  # (subreddit, endpoint) -> list[list[child]]
        self.afters = {}  # (subreddit, endpoint) -> list[str | None]
        self.status = {}  # (subreddit, endpoint) -> int
        self.failures = {}  # (subreddit, endpoint) -> Exception
        self.requests = []  # httpx.Request, in order

    def add_page(self, subreddit, endpoint, children, after=None):
        self.pages.setdefault((subreddit, endpoint), []).append(children)
        self.afters.setdefault((subreddit, endpoint), []).append(after)

    def handler(self, request):
        self.requests.append(request)
        match = re.match(r"/r/([^/]+)/(new|comments)", request.url.path)
        if not match:
            return httpx.Response(404, json={"error": "not found"})
        key = (match.group(1), match.group(2))
        failure = self.failures.get(key)
        if failure is not None:
            raise failure
        pages = self.pages.get(key)
        if not pages:
            return httpx.Response(
                200, json={"kind": "Listing", "data": {"after": None, "dist": 0, "children": []}}
            )
        children, after = pages.pop(0), self.afters[key].pop(0)
        status = self.status.get(key, 200)
        return httpx.Response(status, json=make_listing(children, after=after))


def make_handler(responses):
    """One-shot handler popping scripted (status, payload) pairs or Exceptions."""

    def handler(request):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        status, payload = item
        return httpx.Response(status, json=payload)

    return handler


# ---------------------------------------------------------------- fakes


class FakeSleeper:
    def __init__(self):
        self.calls = []

    async def __call__(self, delay: float) -> None:
        self.calls.append(delay)


class FakeClock:
    def __init__(self, start: float = 1_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeStdin:
    def __init__(self, lines=(), tty: bool = True):
        self._lines = list(lines)
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty

    def readline(self) -> str:
        return self._lines.pop(0) if self._lines else ""


class FakeStdout:
    def __init__(self):
        self.lines = []

    def write(self, text: str) -> None:
        self.lines.append(text)

    def flush(self) -> None:
        pass


@pytest.fixture
def sleeper():
    return FakeSleeper()


@pytest.fixture
def clock():
    return FakeClock()
