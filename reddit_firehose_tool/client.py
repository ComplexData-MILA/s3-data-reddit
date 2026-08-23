"""httpx-based Reddit API client with pacing, retries, and token refresh."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import httpx

logger = logging.getLogger(__name__)

#: Callable[(force: bool) -> str] — returns a bearer token; used to refresh
#: exactly once when a listing request comes back 401.
TokenProvider = Callable[[bool], Awaitable[str]]


class RedditAPIError(RuntimeError):
    """A listing request ultimately failed (after retries) or was malformed."""

    def __init__(
        self, status_code: int | None, message: str, headers: httpx.Headers | None = None
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.headers = headers


@dataclass
class Page:
    children: list[dict[str, Any]]
    after: str | None
    headers: httpx.Headers


class RedditClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        token_provider: TokenProvider | None,
        user_agent: str,
        *,
        base_url: str = "https://api.reddit.com",
        qpm_budget: int = 90,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        backoff_cap: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._token_provider = token_provider
        self._user_agent = user_agent
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._sleep = sleep
        self._clock = clock

        # Client-side QPM budget: request timestamps in the trailing window.
        self._request_times: deque[float] = deque(maxlen=max(qpm_budget, 1))
        self._next_request_at: float | None = None  # set pre-emptively on rate-limit warnings

    async def get_listing(
        self,
        subreddit: str,
        endpoint: str,
        *,
        limit: int = 100,
        after: str | None = None,
        raw_json: int = 1,
    ) -> Page:
        url = f"{self._base_url}/r/{subreddit}/{endpoint}"
        params: dict[str, Any] = {"limit": limit, "raw_json": raw_json}
        if after:
            params["after"] = after

        response = await self._request(url, params=params)
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise RedditAPIError(response.status_code, "listing body was not valid JSON", response.headers) from exc

        if (
            not isinstance(payload, dict)
            or payload.get("kind") != "Listing"
            or not isinstance(payload.get("data", {}).get("children"), list)
        ):
            raise RedditAPIError(
                response.status_code, "unexpected payload shape (not a Listing)", response.headers
            )
        return Page(
            children=payload["data"]["children"],
            after=payload["data"].get("after"),
            headers=response.headers,
        )

    async def _request(
        self, url: str, *, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        allow_refresh = self._token_provider is not None
        response: httpx.Response | None = None

        for attempt in range(self._max_retries + 1):
            await self._pace()

            headers = {"User-Agent": self._user_agent}
            if self._token_provider is not None:
                # force=True on the post-401 retry so the cached token is bypassed.
                token = await self._token_provider(force=not allow_refresh)
                headers["Authorization"] = f"Bearer {token}"

            try:
                response = await self._client.get(url, params=params, headers=headers)
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError):
                if attempt >= self._max_retries:
                    raise
                await self._backoff_sleep(attempt)
                continue

            status = response.status_code
            if status == 401:
                if allow_refresh:
                    allow_refresh = False  # retry once with a freshly refreshed token
                    continue
                break  # no (more) refresh available: raise below
            if status == 429:
                if attempt >= self._max_retries:
                    break
                await self._sleep(self._rate_limit_delay(response) + self._jitter())
                continue
            if status >= 500:
                if attempt >= self._max_retries:
                    break
                await self._backoff_sleep(attempt)
                continue

            self._note_rate_limit(response)
            return response

        assert response is not None
        raise RedditAPIError(
            response.status_code,
            f"request failed after {self._max_retries} retries (HTTP {response.status_code})",
            response.headers,
        )

    async def _pace(self) -> None:
        """Enforce the client-side QPM budget before each request."""
        now = self._clock()
        if self._next_request_at is not None and now < self._next_request_at:
            await self._sleep(self._next_request_at - now)
            now = self._clock()

        if len(self._request_times) == self._request_times.maxlen:
            wait = 60.0 - (now - self._request_times[0])
            if wait > 0:
                await self._sleep(wait + 0.1)
        self._request_times.append(self._clock())

    def _note_rate_limit(self, response: httpx.Response) -> None:
        """Sleep pre-emptively when the server says the budget is exhausted."""
        remaining = response.headers.get("x-ratelimit-remaining")
        if remaining is not None and remaining.isdigit() and int(remaining) == 0:
            reset = response.headers.get("x-ratelimit-reset")
            if reset is not None and reset.isdigit() and int(reset) > 0:
                self._next_request_at = self._clock() + int(reset)
                logger.info("rate limit exhausted; pacing until reset in %ss", reset)

    @staticmethod
    def _rate_limit_delay(response: httpx.Response) -> float:
        retry_after = response.headers.get("retry-after")
        if retry_after is not None and retry_after.isdigit():
            return float(retry_after)
        reset = response.headers.get("x-ratelimit-reset")
        if reset is not None and reset.isdigit():
            return float(reset)
        return 60.0

    async def _backoff_sleep(self, attempt: int) -> None:
        delay = min(self._backoff_base * (2**attempt), self._backoff_cap)
        await self._sleep(delay + self._jitter())

    @staticmethod
    def _jitter() -> float:
        return random.random() * 0.2
