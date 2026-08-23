"""Reddit OAuth2 authentication.

Two flows are supported, chosen automatically:

* ``client_credentials`` (application-only OAuth) — used by "personal use
  script" apps; needs only the client id/secret, no login and no redirect URI.
* Authorization-code with a paste-back callback — for "web" apps: the script
  prints an authorize URL, the user approves it in a browser, and pastes the
  resulting redirect URL (whose localhost page never has to load) back into
  the terminal. The refresh token is persisted in the shared state file so
  subsequent runs are fully non-interactive.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import sys
import time
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from .state import FirehoseState

logger = logging.getLogger(__name__)

AUTHORIZE_PATH = "/api/v1/authorize"
TOKEN_PATH = "/api/v1/access_token"
DEFAULT_REDIRECT_URI = "http://localhost:8080"
DEFAULT_TOKEN_TTL = 3600  # seconds; used if expires_in is missing


class AuthError(RuntimeError):
    """Authentication could not be completed."""


class TokenRevokedError(AuthError):
    """The stored refresh token was rejected by Reddit (revoked/expired)."""


class UnsupportedGrantError(AuthError):
    """The app type does not support the client_credentials grant."""


class RedditAuth:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        user_agent: str,
        state: FirehoseState,
        client: httpx.AsyncClient,
        *,
        auth_base: str = "https://www.reddit.com",
        stdin: Any = None,
        stdout: Any = None,
        token_safety_margin: float = 60.0,
        clock: Any = time.time,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._redirect_uri = redirect_uri
        self._user_agent = user_agent
        self._state = state
        self._client = client
        self._auth_base = auth_base.rstrip("/")
        self._stdin = stdin if stdin is not None else sys.stdin
        self._stdout = stdout if stdout is not None else sys.stdout
        self._safety_margin = token_safety_margin
        self._clock = clock

        self._access_token: str | None = None
        self._expires_at: float = 0.0

    async def get_access_token(self, *, force: bool = False) -> str:
        """Return a valid access token, refreshing or prompting as needed.

        Token sources, in order: the in-memory cache, a stored refresh token
        (web-app flow), the ``client_credentials`` grant (script/personal-use
        apps — no login needed), and finally the interactive paste-back flow.

        With ``force=True`` the cached token is ignored — used by the client
        to retry a request that got a 401.
        """
        if not force and self._access_token is not None:
            if self._clock() < self._expires_at - self._safety_margin:
                return self._access_token

        refresh_token = self._state.refresh_token or os.environ.get(
            "REDDIT_REFRESH_TOKEN"
        )
        if refresh_token:
            try:
                return await self._refresh(refresh_token)
            except TokenRevokedError as exc:
                logger.warning("%s — clearing stored token and re-authenticating", exc)
                self._state.set_refresh_token(None)

        try:
            return await self._client_credentials()
        except UnsupportedGrantError:
            logger.warning(
                "this app does not support the client_credentials grant — "
                "falling back to interactive login"
            )
        return await self._authorization_code_flow()

    async def _client_credentials(self) -> str:
        """Application-only OAuth: used by 'personal use script' apps.

        The token is anonymous (no user context) and long-lived (24 h); it is
        cached in memory only and re-requested on the next process start, so
        nothing sensitive needs to be persisted.
        """
        response = await self._client.post(
            f"{self._auth_base}{TOKEN_PATH}",
            data={"grant_type": "client_credentials"},
            auth=httpx.BasicAuth(self._client_id, self._client_secret),
            headers={"User-Agent": self._user_agent},
        )
        data = self._parse_token_response(response)
        if response.status_code != 200:
            error = data.get("error")
            if error == "unsupported_grant_type":
                raise UnsupportedGrantError(
                    "the app type does not support the client_credentials grant"
                )
            raise AuthError(
                f"client_credentials grant failed: HTTP {response.status_code} "
                f"(error={error})"
            )
        access_token = data.get("access_token")
        if not isinstance(access_token, str):
            raise AuthError("token response missing access_token")
        self._cache_access_token(access_token, data.get("expires_in"))
        return access_token

    async def _refresh(self, refresh_token: str) -> str:
        response = await self._client.post(
            f"{self._auth_base}{TOKEN_PATH}",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            auth=httpx.BasicAuth(self._client_id, self._client_secret),
            headers={"User-Agent": self._user_agent},
        )
        data = self._parse_token_response(response)
        if response.status_code != 200:
            if data.get("error") == "invalid_grant":
                raise TokenRevokedError("Reddit rejected the refresh token (revoked or expired)")
            raise AuthError(
                f"token refresh failed: HTTP {response.status_code} "
                f"(error={data.get('error')})"
            )

        access_token = data.get("access_token")
        if not isinstance(access_token, str):
            raise AuthError("token response missing access_token")
        self._cache_access_token(access_token, data.get("expires_in"))
        rotated = data.get("refresh_token")
        if isinstance(rotated, str) and rotated != refresh_token:
            self._state.set_refresh_token(rotated)
        return access_token

    async def _authorization_code_flow(self) -> str:
        if not self._stdin.isatty():
            raise AuthError(
                "no refresh token available and stdin is not a terminal.\n"
                "Run the script once in an interactive terminal to log in, or set "
                "REDDIT_REFRESH_TOKEN in the environment."
            )

        state_token = secrets.token_urlsafe(16)
        url = self.build_authorize_url(self._client_id, self._redirect_uri, state_token)
        self._stdout.write(
            "\nOne-time Reddit login required:\n"
            "  1. Open this URL in a browser: %s\n"
            "  2. Log in and click 'Allow'.\n"
            "  3. The browser will redirect to %s — the page will FAIL to load, "
            "that is expected.\n"
            "  4. Copy the full URL from the browser's address bar.\n" % (url, self._redirect_uri)
        )
        self._stdout.flush()

        for _ in range(3):
            line = await asyncio.to_thread(self._stdin.readline)
            code = self.parse_callback_url(line.strip(), state_token)
            if code is None:
                self._stdout.write(
                    "No authorization code found in that URL — make sure you copied "
                    "the full redirect URL. Try again:\n"
                )
                self._stdout.flush()
                continue
            return await self._exchange_code(code)
        raise AuthError("too many failed attempts — restart the script to try again")

    async def _exchange_code(self, code: str) -> str:
        response = await self._client.post(
            f"{self._auth_base}{TOKEN_PATH}",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._redirect_uri,
            },
            auth=httpx.BasicAuth(self._client_id, self._client_secret),
            headers={"User-Agent": self._user_agent},
        )
        data = self._parse_token_response(response)
        if response.status_code != 200:
            if data.get("error") == "invalid_grant":
                raise AuthError(
                    "Reddit rejected the authorization code (codes are single-use — "
                    "restart the script and use a freshly opened authorize URL)"
                )
            raise AuthError(
                f"code exchange failed: HTTP {response.status_code} "
                f"(error={data.get('error')})"
            )

        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")
        if not isinstance(access_token, str) or not isinstance(refresh_token, str):
            raise AuthError("token response missing access_token or refresh_token")
        self._cache_access_token(access_token, data.get("expires_in"))
        self._state.set_refresh_token(refresh_token)
        logger.info("logged in; refresh token saved to %s", self._state.path)
        return access_token

    @staticmethod
    def _parse_token_response(response: httpx.Response) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError:
            return {}
        return data if isinstance(data, dict) else {}

    def _cache_access_token(self, access_token: str, expires_in: Any) -> None:
        try:
            ttl = float(expires_in)
        except (TypeError, ValueError):
            ttl = DEFAULT_TOKEN_TTL
        self._access_token = access_token
        self._expires_at = self._clock() + ttl

    @staticmethod
    def build_authorize_url(
        client_id: str,
        redirect_uri: str,
        state_token: str,
        *,
        duration: str = "permanent",
        scope: str = "read",
    ) -> str:
        params = urlencode(
            {
                "client_id": client_id,
                "response_type": "code",
                "state": state_token,
                "redirect_uri": redirect_uri,
                "duration": duration,
                "scope": scope,
            }
        )
        return f"https://www.reddit.com{AUTHORIZE_PATH}?{params}"

    @staticmethod
    def parse_callback_url(url: str, expected_state: str) -> str | None:
        """Extract the authorization code from a pasted redirect URL.

        Returns ``None`` if no code is present (caller re-prompts). Raises
        ``AuthError`` on denial or a state mismatch.
        """
        try:
            query = parse_qs(urlparse(url).query)
        except ValueError:
            return None

        error = query.get("error", [None])[0]
        if error is not None:
            raise AuthError(f"authorization was denied by the user (error={error})")

        state = query.get("state", [None])[0]
        if state is None:
            return None  # not a redirect URL at all: let the caller re-prompt
        if state != expected_state:
            raise AuthError(
                "state parameter mismatch — the pasted URL does not belong to this "
                "authorization attempt; restart the script and use the URL it prints"
            )

        code = query.get("code", [None])[0]
        return code if isinstance(code, str) and code else None
