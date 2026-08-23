"""Long-running poller over Reddit listings, yielding record dicts.

One ``RedditFirehose`` instance polls one set of endpoints (``("new",)`` for
posts or ``("comments",)`` for comments) across all configured subreddits,
every ``interval`` seconds. It never terminates on its own — callers bound it
with :func:`iter_until_timeout` (hourly S3 batches) or SIGINT.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import time
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Sequence
from typing import Any, Callable, TypeVar

import httpx

from .auth import AuthError
from .client import RedditAPIError, RedditClient
from .state import FirehoseState, StreamCursor

logger = logging.getLogger(__name__)

POST_ENDPOINT = "new"
COMMENT_ENDPOINT = "comments"
VALID_ENDPOINTS = {POST_ENDPOINT, COMMENT_ENDPOINT}

T = TypeVar("T")

#: Exceptions that mean "this stream failed; keep its old cursor and retry
#: next cycle". One failing stream must not block the others.
POLL_ERRORS = (
    RedditAPIError,
    AuthError,
    httpx.HTTPError,
    TimeoutError,
    json.JSONDecodeError,
)


def build_record(child: dict[str, Any], endpoint: str, fetched_at: str) -> dict[str, Any]:
    """Flatten a Reddit child ({"kind": ..., "data": {...}}) into a record.

    The fullname (``t3_...``/``t1_...``) is used both as the row ``id`` (so S3
    rows keep stable ids) and as the ``name`` deduplication key. Posts and
    comments produce homogeneous, endpoint-specific schemas; the eventual join
    is ``comments.link_id == posts.name``.
    """
    data = child.get("data") if isinstance(child, dict) else {}
    name = data.get("name")
    record: dict[str, Any] = {
        "id": name,
        "name": name,
        "subreddit": data.get("subreddit"),
        "created_utc": data.get("created_utc"),
        "author": data.get("author"),
        "permalink": data.get("permalink"),
        "score": data.get("score"),
        "fetched_at": fetched_at,
        "raw": data,
    }
    if endpoint == POST_ENDPOINT:
        record.update(
            {
                "title": data.get("title"),
                "selftext": data.get("selftext"),
                "url": data.get("url"),
                "num_comments": data.get("num_comments"),
                "upvote_ratio": data.get("upvote_ratio"),
            }
        )
    else:
        record.update(
            {
                "body": data.get("body"),
                "link_id": data.get("link_id"),
                "parent_id": data.get("parent_id"),
            }
        )
    return record


class RedditFirehose(AsyncIterator[dict[str, Any]]):
    """Poll ``subreddits × endpoints`` every ``interval`` seconds and yield records.

    Progress is kept in the shared :class:`FirehoseState`: per stream a
    ``last_seen_utc`` cursor plus the fullnames seen at the cursor's boundary
    second (Reddit timestamps are second-quantized, so ties are common). The
    cursor is snapshotted at cycle start and only committed after the stream
    completes successfully, so a failure never loses items and a cancelled
    cycle is simply redone.
    """

    def __init__(
        self,
        client: RedditClient,
        state: FirehoseState,
        subreddits: Sequence[str],
        *,
        endpoints: Iterable[str] = (POST_ENDPOINT, COMMENT_ENDPOINT),
        interval: float = 60.0,
        max_pages: int = 25,
        limit: int = 100,
        backfill: bool = True,
        max_cycles: int | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._endpoints = tuple(endpoints)
        unknown = set(self._endpoints) - VALID_ENDPOINTS
        if unknown:
            raise ValueError(f"unknown endpoints: {sorted(unknown)}")
        if not self._endpoints:
            raise ValueError("at least one endpoint is required")

        self._client = client
        self._state = state
        self._subreddits = tuple(subreddits)
        self._interval = max(interval, 0.0)
        self._max_pages = max(max_pages, 1)
        self._limit = max(limit, 1)
        self._backfill = backfill
        self._max_cycles = max_cycles
        self._sleep = sleep
        self._clock = clock if clock is not None else time.time

        self._buffer: deque[dict[str, Any]] = deque()
        self._next_poll: float | None = None
        self._cycles = 0
        self._closed = False

    def __aiter__(self) -> "RedditFirehose":
        return self

    async def aclose(self) -> None:
        self._closed = True

    async def __anext__(self) -> dict[str, Any]:
        while not self._closed:
            if self._buffer:
                return self._buffer.popleft()
            if self._max_cycles is not None and self._cycles >= self._max_cycles:
                break

            now = self._clock()
            if self._next_poll is None:
                self._next_poll = now  # first cycle runs immediately
            if now < self._next_poll:
                await self._sleep(self._next_poll - now)
                now = self._clock()
            # No catch-up spiral: if a cycle overran the interval, the next
            # poll is scheduled interval-seconds after the last one finished.
            self._next_poll = max(self._next_poll + self._interval, now + self._interval)
            self._cycles += 1

            self._buffer.extend(await self._poll_cycle())

        raise StopAsyncIteration

    async def _poll_cycle(self) -> list[dict[str, Any]]:
        fetched_at = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        records: list[dict[str, Any]] = []
        for subreddit in self._subreddits:
            for endpoint in self._endpoints:
                key = f"{subreddit}/{endpoint}"
                try:
                    records.extend(
                        await self._poll_stream(key, subreddit, endpoint, fetched_at)
                    )
                except POLL_ERRORS as exc:
                    logger.warning(
                        "poll failed for %s (%r) — cursor not advanced; retrying next cycle",
                        key,
                        exc,
                    )
        self._state.save()
        return records

    async def _poll_stream(
        self, key: str, subreddit: str, endpoint: str, fetched_at: str
    ) -> list[dict[str, Any]]:
        cur = self._state.cursor(key)
        old_cursor = cur.last_seen_utc if cur is not None else None
        old_names = cur.names() if cur is not None else set()
        first_run = cur is None

        records: list[dict[str, Any]] = []
        seen_children: list[tuple[str, float]] = []  # (fullname, created_utc)
        max_cut: float | None = old_cursor
        after: str | None = None
        pages = 0

        while True:
            page = await self._client.get_listing(
                subreddit, endpoint, limit=self._limit, after=after
            )
            children = [
                c for c in page.children if isinstance(c, dict) and isinstance(c.get("data"), dict)
            ]
            if not children:
                break  # nothing at all: leave the cursor untouched

            valid: list[tuple[str, float]] = []
            for c in children:
                d = c.get("data", {})
                name, cut = d.get("name"), d.get("created_utc")
                if isinstance(name, str) and isinstance(cut, (int, float)):
                    valid.append((name, cut))
            if valid:
                page_max = max(t for _, t in valid)
                max_cut = page_max if max_cut is None else max(max_cut, page_max)
                seen_children.extend(valid)

            if first_run:
                # Backfill exactly one page (the most recent items), then commit.
                if self._backfill:
                    records.extend(
                        build_record(c, endpoint, fetched_at)
                        for c in children
                        if isinstance(c.get("data", {}).get("name"), str)
                        and isinstance(c.get("data", {}).get("created_utc"), (int, float))
                    )
                break

            stop = False
            for child in children:
                data = child["data"]
                name = data.get("name")
                cut = data.get("created_utc")
                if name is None or not isinstance(cut, (int, float)):
                    continue  # malformed child: skip it, keep going
                if cut > old_cursor:
                    records.append(build_record(child, endpoint, fetched_at))
                elif cut == old_cursor and name not in old_names:
                    records.append(build_record(child, endpoint, fetched_at))
                else:
                    stop = True  # reached the already-ingested overlap window
                    break
            pages += 1
            if stop or pages >= self._max_pages:
                break
            after = page.after
            if after is None:
                break

        if max_cut is not None and seen_children:
            recent_names = (
                [(n, t) for n, t in cur.recent_names if t >= max_cut]
                if cur is not None
                else []
            )
            known = {n for n, _ in recent_names}
            for name, cut in seen_children:
                if cut >= max_cut and name not in known:
                    recent_names.append((name, cut))
                    known.add(name)
            new_cursor = StreamCursor(last_seen_utc=max_cut, recent_names=recent_names)
            new_cursor.prune()
            self._state.set_cursor(key, new_cursor)
        return records


async def iter_until_timeout(
    source: AsyncIterable[T], timeout: float
) -> AsyncIterator[T]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    it = aiter(source)

    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return

        try:
            yield await asyncio.wait_for(anext(it), timeout=remaining)
        except (StopAsyncIteration, asyncio.TimeoutError):
            return
