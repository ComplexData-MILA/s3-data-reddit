"""Local JSON state for restart-safe polling progress and OAuth tokens."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: Number of recently seen fullnames kept per stream. The list only needs to
#: cover the boundary second (Reddit timestamps are second-quantized, so many
#: items can share the cursor timestamp); 10k is far beyond plausible volume.
MAX_RECENT_NAMES = 10_000


@dataclass
class StreamCursor:
    """Polling progress for one (subreddit, endpoint) stream.

    ``last_seen_utc`` is the newest ``created_utc`` that has been ingested.
    ``recent_names`` holds ``(fullname, created_utc)`` pairs at the cursor
    boundary second, so ties can be resolved without duplicates.
    """

    last_seen_utc: float
    recent_names: list[tuple[str, float]] = field(default_factory=list)
    maxlen: int = MAX_RECENT_NAMES

    def add(self, name: str, created_utc: float) -> None:
        """Remember a fullname (newest last), keeping the list bounded."""
        self.recent_names.append((name, created_utc))
        if len(self.recent_names) > self.maxlen:
            del self.recent_names[: len(self.recent_names) - self.maxlen]

    def prune(self) -> None:
        """Drop entries strictly older than the cursor; they can never tie again."""
        self.recent_names = [
            (name, cut) for name, cut in self.recent_names if cut >= self.last_seen_utc
        ]

    def names(self) -> set[str]:
        return {name for name, _ in self.recent_names}


class FirehoseState:
    """Versioned JSON state file with atomic whole-file writes (mode 0600).

    Shared by the posts and comments firehoses: their cursor keys are
    disjoint (``"{subreddit}/new"`` vs ``"{subreddit}/comments"``) and saves
    are full snapshots of this object, so last-writer-wins is safe.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.refresh_token: str | None = None
        self.cursors: dict[str, StreamCursor] = {}

    @classmethod
    def load(cls, path: Path) -> "FirehoseState":
        state = cls(path)
        if not path.exists():
            return state

        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            corrupt = path.with_name(path.name + ".corrupt")
            try:
                os.replace(path, corrupt)
            except OSError:
                pass
            logger.warning(
                "state file %s was unreadable (%s); moved aside to %s and starting fresh",
                path,
                exc,
                corrupt,
            )
            return state

        oauth = raw.get("oauth")
        if isinstance(oauth, dict) and isinstance(oauth.get("refresh_token"), str):
            state.refresh_token = oauth["refresh_token"]

        cursors = raw.get("cursors")
        if isinstance(cursors, dict):
            for key, cur in cursors.items():
                try:
                    last_seen = float(cur["last_seen_utc"])
                    recent = [
                        (str(name), float(cut))
                        for name, cut in cur.get("recent_names", [])
                        if float(cut) >= last_seen
                    ]
                except (KeyError, TypeError, ValueError):
                    logger.warning("dropping malformed cursor %r from state file", key)
                    continue
                state.cursors[str(key)] = StreamCursor(
                    last_seen_utc=last_seen, recent_names=recent
                )
        return state

    def save(self) -> None:
        payload = json.dumps(self._to_dict(), indent=2, ensure_ascii=False)
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(
                dir=self.path.parent, prefix=self.path.name + ".", suffix=".tmp"
            )
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.path)
        finally:
            if tmp is not None:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    def _to_dict(self) -> dict[str, Any]:
        return {
            "version": 1,
            "oauth": {"refresh_token": self.refresh_token}
            if self.refresh_token
            else {},
            "cursors": {
                key: {
                    "last_seen_utc": cur.last_seen_utc,
                    "recent_names": [
                        [name, cut] for name, cut in cur.recent_names
                    ],
                }
                for key, cur in self.cursors.items()
            },
        }

    def set_refresh_token(self, token: str | None) -> None:
        """Persist (or clear) the OAuth refresh token immediately."""
        self.refresh_token = token
        self.save()

    def cursor(self, key: str) -> StreamCursor | None:
        return self.cursors.get(key)

    def set_cursor(self, key: str, cursor: StreamCursor) -> None:
        self.cursors[key] = cursor
