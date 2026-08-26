"""Author profile enrichment daemon: build the ``{prefix}-authors`` entity dataset.

The firehose (:mod:`reddit_firehose_tool.main`) stores each post/comment's
author username as a top-level column and, inside the ``raw`` blob, the
author's ``author_fullname`` (t2 id) — but no profile details. This process
runs continuously (long-running, supervised by systemd, mirroring the
backfill's style): it scans the collected posts and comments for distinct
authors, fetches each author's profile once via Reddit ``/user/{name}/about``,
uploads one record per author into the ``{prefix}-authors`` dataset (batch
``reddit-firehose-authors-YYYYMMDD``), merges the batch, and repeats.

Design notes:

* The S3 store is the single source of truth. A small author index
  (``{prefix}/_authors/authors-index.parquet``) records every author already
  fetched — including ``not_found`` tombstones for suspended/deleted accounts —
  so an author is never queried twice. The only re-query path is the
  staleness window (``--refetch-days``, default 7): profiles older than the
  window whose author is still present in the datasets are refreshed, at most
  once per window.
* A local JSONL cache (``author_profiles_cache.jsonl``) absorbs API calls
  across crashes and index rebuilds; it is an optimization only — losing it
  never loses data, only re-fetches.
* Concurrency safety uses the WSS MUTEX feature through the pinned
  ``s3_data_tool``'s ``S3Lock`` (a TTL lock file in S3, check-and-set guarded
  by the mutex websocket); the lock is held per iteration, never across the
  sleep, and renewed while long fetches run.
* Each iteration re-scans all merged parquet via DuckDB over httpfs (cheap at
  the current scale — the index, not the scan, is what makes the API budget
  incremental). The scan therefore needs no manifest: a batch is implicitly
  "done" once every author in it has an index row, and the anti-join makes
  rescanning idempotent.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aioboto3
import duckdb
import httpx

from s3_data_tool.s3_lock import S3Lock
from s3_data_tool.s3_utils import enumerate_parquet_paths, s3_object_exists

from .auth import RedditAuth
from .backfill import (
    SCAN_RETRIES,
    _merge_own_batch,
    _renew_lock_task,
    _upload_records,
    unquote_fullname,
    valid_fullname,
)
from .client import RedditAPIError, RedditClient
from .main import DEFAULT_API_BASE, _require_env, load_dotenv
from .state import FirehoseState

logger = logging.getLogger(__name__)

#: Global authors-worker lock: S3 lock file ``locks/reddit-firehose-authors.lock``
#: guarded by the WSS mutex name ``s3lock-reddit-firehose-authors``.
LOCK_PATH = "reddit-firehose-authors"
DEFAULT_LOCK_TTL_MS = 3_600_000
#: The authors worker's own daily batches are ``reddit-firehose-authors-YYYYMMDD``.
AUTHORS_BATCH_PREFIX = "reddit-firehose-authors-"
DEFAULT_POLL_SECONDS = 300.0
DEFAULT_REFETCH_DAYS = 7.0
DEFAULT_MAX_FETCH = 500
DEFAULT_CACHE_FILE = "author_profiles_cache.jsonl"

#: t2 fullname as stored in each listing payload's ``author_fullname`` field.
AUTHOR_FULLNAME_PREFIX = "t2_"


# --------------------------------------------------------------------- pure
# helpers


def authors_batch_name(date: datetime.datetime, suffix: str | None = None) -> str:
    """Daily authors batch name (``reddit-firehose-authors-YYYYMMDD``)."""
    name = f"{AUTHORS_BATCH_PREFIX}{date:%Y%m%d}"
    if suffix:
        name += f"-{suffix}"
    return name


def fetchable_author(author: Any) -> bool:
    """True for a usable ``/about`` username (deleted/removed markers excluded)."""
    return isinstance(author, str) and author not in ("[deleted]", "[removed]")


def build_author_record(
    profile: dict[str, Any],
    *,
    name: str,
    author: str,
    fetched_at: str,
    first_seen_at: float | None,
    last_seen_at: float | None,
) -> dict[str, Any]:
    """Build one ``reddit-authors`` record from a ``/user/{name}/about`` payload.

    ``name`` is the t2 fullname from the base rows (stable across renames);
    ``author`` is the username the profile was fetched under. Curated columns
    are typed top-level; the full payload rides along in
    ``author_profile_raw`` (mirroring the firehose's ``raw`` convention).
    """
    subreddit = profile.get("subreddit")
    return {
        "id": name,
        "name": name,
        "author": author,
        "author_created_utc": profile.get("created_utc"),
        "author_link_karma": profile.get("link_karma"),
        "author_comment_karma": profile.get("comment_karma"),
        "author_total_karma": profile.get("total_karma"),
        "author_awardee_karma": profile.get("awardee_karma"),
        "author_awarder_karma": profile.get("awarder_karma"),
        "author_is_mod": profile.get("is_mod", False),
        "author_is_employee": profile.get("is_employee", False),
        "author_is_gold": profile.get("is_gold", False),
        "author_verified": profile.get("verified", False),
        "author_has_verified_email": profile.get("has_verified_email", False),
        "author_icon_img": profile.get("icon_img"),
        "author_subreddit": (
            subreddit.get("display_name") if isinstance(subreddit, dict) else subreddit
        ),
        "author_profile_raw": profile,
        "fetched_at": fetched_at,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
    }


def cache_entry_for(
    name: str, author: str, status: str, profile: dict[str, Any] | None, fetched_at: str
) -> dict[str, Any]:
    """One local-cache line (the shape appended to ``author_profiles_cache.jsonl``)."""
    return {
        "name": name,
        "author": author,
        "status": status,
        "profile": profile,
        "fetched_at": fetched_at,
    }


def entry_is_fresh(
    entry: dict[str, Any] | None, refetch_days: float, now: datetime.datetime
) -> bool:
    """True when an index/cache entry is within the staleness window.

    ``refetch_days <= 0`` means entries never expire (fetch-once-forever);
    unparseable ``fetched_at`` values count as stale.
    """
    if entry is None or entry.get("status") != "ok":
        return False
    if refetch_days <= 0:
        return True
    fetched = entry.get("fetched_at")
    if not isinstance(fetched, str):
        return False
    try:
        fetched_dt = datetime.datetime.fromisoformat(fetched)
    except ValueError:
        return False
    if fetched_dt.tzinfo is not None:
        fetched_dt = fetched_dt.replace(tzinfo=None)  # naive UTC, like the worker clock
    return (now - fetched_dt) < datetime.timedelta(days=refetch_days)


def cache_lookup(
    entry: dict[str, Any] | None, refetch_days: float, now: datetime.datetime
) -> str | None:
    """How a local-cache entry satisfies a fetch candidate.

    Returns ``"ok"`` (profile usable), ``"tombstone"`` (permanent
    ``not_found``), or None (entry missing or stale — an API call is needed).
    """
    if entry is None:
        return None
    if entry.get("status") == "not_found":
        return "tombstone"
    if entry_is_fresh(entry, refetch_days, now):
        return "ok"
    return None


def plan_fetches(
    index_rows: dict[str, dict[str, Any]],
    scanned: dict[str, dict[str, Any]],
    *,
    refetch_days: float,
    now: datetime.datetime,
    max_fetch: int,
) -> list[dict[str, Any]]:
    """Candidates to fetch this iteration, new-first, capped at ``max_fetch``.

    New authors are those in ``scanned`` but absent from the index. Stale
    authors are index rows with ``status == "ok"`` whose ``fetched_at`` is
    older than the staleness window and whose fullname is still present in the
    scan. ``not_found`` tombstones are permanent (never re-fetched), and
    authors without a usable username are skipped. Each candidate carries
    ``{name, author, first_seen_at, last_seen_at, stale}``.
    """
    new = [
        {**row, "stale": False}
        for name, row in sorted(scanned.items())
        if name not in index_rows and fetchable_author(row.get("author"))
    ]
    stale: list[dict[str, Any]] = []
    if refetch_days > 0:
        stale = [
            {**scanned[name], "stale": True}
            for name, row in sorted(index_rows.items())
            if row.get("status") == "ok"
            and name in scanned
            and fetchable_author(scanned[name].get("author"))
            and not entry_is_fresh(row, refetch_days, now)
        ]
    return (new + stale)[:max_fetch]


def index_row_for(
    name: str,
    author: str,
    status: str,
    fetched_at: str,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    """One index row (``ok`` or ``not_found``) for a fetched candidate."""
    return {
        "name": name,
        "author": author,
        "status": status,
        "fetched_at": fetched_at,
        "first_seen_at": candidate["first_seen_at"],
        "last_seen_at": candidate["last_seen_at"],
    }


def build_authors_scan_query(
    paths_by_dataset: dict[str, list[str]]
) -> str | None:
    """DuckDB query over merged parquet: distinct authors with seen ranges.

    Extracts ``author_fullname`` from the JSON ``raw`` column (the lake stores
    the raw Reddit payload JSON-stringified). Rows without a fullname
    (``[deleted]`` authors) drop out. Returns None when there is nothing to
    scan. Paths are absolute ``s3://`` URLs (as returned by
    ``s3_data_tool.s3_utils.enumerate_parquet_paths``).
    """
    subqueries: list[str] = []
    for dataset, paths in paths_by_dataset.items():
        if not paths:
            continue
        path_list = ", ".join(
            f"'{p.replace(chr(39), chr(39) * 2)}'" for p in paths
        )
        subqueries.append(
            "SELECT author, "
            "json_extract_string(raw, '$.author_fullname') AS author_fullname, "
            f"created_utc FROM read_parquet([{path_list}])"
        )
    if not subqueries:
        return None
    return (
        "SELECT author, author_fullname, "
        "MIN(created_utc) AS first_seen_utc, MAX(created_utc) AS last_seen_utc "
        f"FROM ({' UNION ALL '.join(subqueries)}) "
        "WHERE author_fullname IS NOT NULL AND author_fullname <> '' "
        "GROUP BY author, author_fullname"
    )


# --------------------------------------------------------------------- S3 key
# helpers


def index_key(prefix: str) -> str:
    """Author index parquet: one row per fetched author (or tombstone)."""
    return f"{prefix}/_authors/authors-index.parquet"


# --------------------------------------------------------------------- async
# S3 / DuckDB helpers


async def _run_row_query(
    query: str, *, endpoint_url: str, access_key: str, secret_key: str
) -> list[tuple[Any, ...]]:
    """Execute a DuckDB query over s3:// paths; return the raw rows.

    Mirrors ``backfill.run_duckdb_query`` (worker thread, httpfs SET config)
    but returns all columns instead of the first.
    """

    def _run() -> list[tuple[Any, ...]]:
        endpoint_host = endpoint_url.removeprefix("https://").rstrip("/")
        endpoint_host = endpoint_host.removeprefix("http://").rstrip("/")
        use_ssl = endpoint_url.startswith("https://")
        conn = duckdb.connect()
        try:
            conn.execute(
                f"""
                SET s3_access_key_id='{access_key}';
                SET s3_secret_access_key='{secret_key}';
                SET s3_endpoint='{endpoint_host}';
                SET s3_use_ssl={str(use_ssl).lower()};
                SET s3_url_style='path';
                """
            )
            result = conn.execute(query)
            rows: list[tuple[Any, ...]] = []
            while True:
                batch = result.fetchmany(1000)
                if not batch:
                    break
                rows.extend(batch)
            return rows
        finally:
            conn.close()

    return await asyncio.to_thread(_run)


async def _run_row_query_retry(
    query: str, *, endpoint_url: str, access_key: str, secret_key: str
) -> list[tuple[Any, ...]]:
    """Like :func:`_run_row_query` but retries transient failures."""
    last: BaseException | None = None
    for attempt in range(SCAN_RETRIES):
        try:
            return await _run_row_query(
                query,
                endpoint_url=endpoint_url,
                access_key=access_key,
                secret_key=secret_key,
            )
        except Exception as exc:  # noqa: BLE001 — any blip; retry then re-raise
            last = exc
            logger.warning(
                "DuckDB query failed (attempt %d/%d): %r", attempt + 1, SCAN_RETRIES, exc
            )
            if attempt < SCAN_RETRIES - 1:
                await asyncio.sleep(2.0**attempt)
    assert last is not None
    raise last


async def load_index_rows(
    s3_client: Any,
    bucket: str,
    prefix: str,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> dict[str, dict[str, Any]]:
    """Load the author index parquet into ``{name: row}`` (empty if absent).

    String columns are unquoted from the lake's JSON encoding. ``name`` is the
    t2 fullname.
    """
    key = index_key(prefix)
    if not await s3_object_exists(s3_client, bucket, key):
        return {}
    rows = await _run_row_query(
        "SELECT name, author, status, fetched_at, first_seen_at, last_seen_at "
        f"FROM read_parquet(['s3://{bucket}/{key}'])",
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = unquote_fullname(row[0])
        if name is None:
            continue
        index[name] = {
            "name": name,
            "author": unquote_fullname(row[1]),
            "status": unquote_fullname(row[2]) or "ok",
            "fetched_at": unquote_fullname(row[3]),
            "first_seen_at": row[4],
            "last_seen_at": row[5],
        }
    return index


async def write_authors_index(
    s3_client: Any,
    bucket: str,
    prefix: str,
    rows: dict[str, dict[str, Any]],
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Rewrite the author index parquet from the in-memory row dict.

    Strings are JSON-quoted to match the lake's string encoding (like
    ``backfill.write_index``). The parquet is written to a ``.temp`` key then
    copied into place (the library's own merge pattern).
    """
    key = index_key(prefix)
    temp_key = f"{key}.temp"

    def _write() -> None:
        endpoint_host = endpoint_url.removeprefix("https://").rstrip("/")
        endpoint_host = endpoint_host.removeprefix("http://").rstrip("/")
        use_ssl = endpoint_url.startswith("https://")
        conn = duckdb.connect()
        try:
            conn.execute(
                f"""
                SET s3_access_key_id='{access_key}';
                SET s3_secret_access_key='{secret_key}';
                SET s3_endpoint='{endpoint_host}';
                SET s3_use_ssl={str(use_ssl).lower()};
                SET s3_url_style='path';
                """
            )
            conn.execute(
                "CREATE TABLE idx(name VARCHAR, author VARCHAR, status VARCHAR, "
                "fetched_at VARCHAR, first_seen_at DOUBLE, last_seen_at DOUBLE)"
            )
            conn.executemany(
                "INSERT INTO idx VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        json.dumps(row["name"]),
                        json.dumps(row.get("author") or ""),
                        json.dumps(row.get("status") or "ok"),
                        json.dumps(row.get("fetched_at") or ""),
                        row.get("first_seen_at"),
                        row.get("last_seen_at"),
                    )
                    for row in sorted(rows.values(), key=lambda r: r["name"])
                ],
            )
            conn.execute(f"COPY idx TO 's3://{bucket}/{temp_key}' (FORMAT PARQUET)")
        finally:
            conn.close()

    await asyncio.to_thread(_write)
    await s3_client.copy_object(
        Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": temp_key}
    )
    await s3_client.delete_object(Bucket=bucket, Key=temp_key)


async def scan_authors(
    paths_by_dataset: dict[str, list[str]],
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> dict[str, dict[str, Any]]:
    """Distinct authors in the given merged parquet files, keyed by t2 fullname.

    Rows for the same fullname seen under different usernames (renames) are
    merged, preferring a concrete username over a NULL one.
    """
    query = build_authors_scan_query(paths_by_dataset)
    if query is None:
        return {}
    rows = await _run_row_query_retry(
        query, endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key
    )
    authors: dict[str, dict[str, Any]] = {}
    for row in rows:
        author = unquote_fullname(row[0])
        name = unquote_fullname(row[1])
        if not valid_fullname(name, AUTHOR_FULLNAME_PREFIX):
            continue
        first_seen = row[2] if row[2] is not None else 0.0
        last_seen = row[3] if row[3] is not None else 0.0
        prev = authors.get(name)
        if prev is None:
            authors[name] = {
                "name": name,
                "author": author,
                "first_seen_at": first_seen,
                "last_seen_at": last_seen,
            }
            continue
        prev["first_seen_at"] = min(prev["first_seen_at"], first_seen)
        prev["last_seen_at"] = max(prev["last_seen_at"], last_seen)
        if author is not None:
            prev["author"] = author
    return authors


# --------------------------------------------------------------------- local
# cache


def load_cache(path: Path) -> dict[str, dict[str, Any]]:
    """Load the local profile cache (one JSON object per line); {} if absent.

    Corrupt lines are skipped — the cache is an optimization; the S3 index is
    authoritative.
    """
    cache: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return cache
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("ignoring corrupt cache line in %s", path)
                continue
            name = entry.get("name")
            if isinstance(name, str):
                cache[name] = entry
    return cache


def append_cache_line(path: Path, entry: dict[str, Any]) -> None:
    """Append one cache entry (single writer per host; the S3 lock serializes)."""
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


# --------------------------------------------------------------------- fetch


@dataclass
class FetchOutcome:
    records: list[dict[str, Any]] = field(default_factory=list)
    tombstones: list[dict[str, Any]] = field(default_factory=list)
    index_rows: list[dict[str, Any]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)


async def fetch_profiles(
    client: RedditClient,
    cache: dict[str, dict[str, Any]],
    cache_path: Path,
    candidates: list[dict[str, Any]],
    *,
    fetched_at: str,
    refetch_days: float,
    now: datetime.datetime,
) -> FetchOutcome:
    """Fetch ``/user/{name}/about`` for each candidate (serial, QPM-paced).

    Fresh local-cache entries satisfy a candidate without an API call; cached
    ``not_found`` entries are permanent tombstones. 404s become index
    tombstones; other failures are logged and skipped (retried next iteration).
    """
    outcome = FetchOutcome()
    for candidate in candidates:
        name = candidate["name"]
        author = candidate["author"]
        lookup = cache_lookup(cache.get(name), refetch_days, now)
        if lookup == "tombstone":
            outcome.tombstones.append(
                index_row_for(name, author, "not_found", fetched_at, candidate)
            )
            continue
        if lookup == "ok":
            profile = cache[name]["profile"]
            outcome.records.append(
                build_author_record(
                    profile,
                    name=name,
                    author=author,
                    fetched_at=fetched_at,
                    first_seen_at=candidate["first_seen_at"],
                    last_seen_at=candidate["last_seen_at"],
                )
            )
            outcome.index_rows.append(
                index_row_for(name, author, "ok", fetched_at, candidate)
            )
            continue
        try:
            profile = await client.get_user_about(author)
        except RedditAPIError:
            logger.warning(
                "profile fetch failed for %s (%s); retrying next iteration",
                name,
                author,
            )
            outcome.failed.append(name)
            continue
        except Exception:  # noqa: BLE001 — any blip must not abort the iteration
            logger.exception(
                "profile fetch failed for %s (%s); retrying next iteration",
                name,
                author,
            )
            outcome.failed.append(name)
            continue
        if profile is None:
            append_cache_line(
                cache_path,
                cache_entry_for(name, author, "not_found", None, fetched_at),
            )
            outcome.tombstones.append(
                index_row_for(name, author, "not_found", fetched_at, candidate)
            )
            continue
        append_cache_line(
            cache_path, cache_entry_for(name, author, "ok", profile, fetched_at)
        )
        outcome.records.append(
            build_author_record(
                profile,
                name=name,
                author=author,
                fetched_at=fetched_at,
                first_seen_at=candidate["first_seen_at"],
                last_seen_at=candidate["last_seen_at"],
            )
        )
        outcome.index_rows.append(
            index_row_for(name, author, "ok", fetched_at, candidate)
        )
    return outcome


# --------------------------------------------------------------------- the
# iteration


def _require_authors_env(name: str, why: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"environment variable {name} must be set for authors.py: {why}")
    return value


async def _authors_work(
    args: argparse.Namespace,
    s3_client: Any,
    bucket: str,
    prefix: str,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> None:
    posts_dataset = f"{args.dataset_prefix}-posts"
    comments_dataset = f"{args.dataset_prefix}-comments"
    authors_dataset = f"{args.dataset_prefix}-authors"
    now = datetime.datetime.now()
    batch = authors_batch_name(now, args.batch_suffix)

    # ---- lake state: one pass per dataset, then the author index ------------
    paths_by_dataset = {
        posts_dataset: await enumerate_parquet_paths(
            s3_client, bucket, prefix, posts_dataset
        ),
        comments_dataset: await enumerate_parquet_paths(
            s3_client, bucket, prefix, comments_dataset
        ),
    }
    index_rows = await load_index_rows(
        s3_client,
        bucket,
        prefix,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )

    # ---- scan: distinct authors over all merged parquet ----------------------
    scanned = await scan_authors(
        paths_by_dataset,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )
    candidates = plan_fetches(
        index_rows,
        scanned,
        refetch_days=args.refetch_days,
        now=now,
        max_fetch=args.max_fetch,
    )
    logger.info(
        "scanned %d distinct authors (%d known in index); %d candidates",
        len(scanned),
        len(index_rows),
        len(candidates),
    )
    if args.dry_run:
        for candidate in candidates:
            print(json.dumps(candidate, sort_keys=True))
        return

    if not candidates:
        logger.info("nothing to fetch")
    else:
        client_id = _require_authors_env(
            "REDDIT_CLIENT_ID", "fetching author profiles requires app credentials"
        )
        client_secret = _require_authors_env(
            "REDDIT_CLIENT_SECRET", "fetching author profiles requires app credentials"
        )
        redirect_uri = os.environ.get("REDDIT_REDIRECT_URI", "http://localhost:8080")
        user_agent = os.environ.get(
            "REDDIT_USER_AGENT", "linux:reddit-firehose-authors:v0.1.0 (by /u/unknown)"
        )

        fetched_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        cache_path = Path(args.cache_file)
        cache = load_cache(cache_path)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=30, write=10, pool=10)
        ) as http:
            auth = None
            if not args.skip_auth:
                auth = RedditAuth(
                    client_id,
                    client_secret,
                    redirect_uri,
                    user_agent,
                    # A separate state file: the live firehose's cursors and
                    # refresh token are never touched by the authors worker.
                    FirehoseState.load(Path("reddit_firehose_authors_state.json")),
                    http,
                    auth_base=args.auth_base,
                )
            client = RedditClient(
                http,
                token_provider=auth.get_access_token if auth is not None else None,
                user_agent=user_agent,
                base_url=args.api_base,
                qpm_budget=args.qpm_budget,
            )
            outcome = await fetch_profiles(
                client,
                cache,
                cache_path,
                candidates,
                fetched_at=fetched_at,
                refetch_days=args.refetch_days,
                now=now,
            )
        logger.info(
            "fetched %d profiles; %d tombstones (suspended/deleted); %d failed",
            len(outcome.records),
            len(outcome.tombstones),
            len(outcome.failed),
        )

        await _upload_records(outcome.records, authors_dataset, batch)

        for row in outcome.index_rows:
            index_rows[row["name"]] = row
        for row in outcome.tombstones:
            index_rows[row["name"]] = row
        await write_authors_index(
            s3_client,
            bucket,
            prefix,
            index_rows,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
        )

    if not args.skip_merge:
        # Merging also runs when nothing was fetched: a previous iteration's
        # upload may still be unmerged (a crash between upload and merge).
        await _merge_own_batch(
            s3_client, bucket, prefix, authors_dataset, batch, args.lock_ttl_ms
        )


async def _run_iteration(
    args: argparse.Namespace,
    s3_client: Any,
    bucket: str,
    prefix: str,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> None:
    if args.dry_run:
        await _authors_work(
            args,
            s3_client,
            bucket,
            prefix,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
        )
        return

    lock = S3Lock(LOCK_PATH, args.lock_ttl_ms, s3_client, bucket)
    try:
        acquired = await lock.acquire()
    except Exception:
        # The mutex server is unreachable — crash so systemd restarts us later.
        logger.exception("WSS mutex unreachable; aborting this iteration")
        raise
    if not acquired:
        logger.info("authors worker already running elsewhere; skipping this iteration")
        return

    try:
        work = asyncio.create_task(
            _authors_work(
                args,
                s3_client,
                bucket,
                prefix,
                endpoint_url=endpoint_url,
                access_key=access_key,
                secret_key=secret_key,
            )
        )
        renewal = asyncio.create_task(_renew_lock_task(lock, work))
        try:
            await work
        finally:
            renewal.cancel()
    finally:
        await lock.release()


# --------------------------------------------------------------------- CLI


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Reddit author profiles for authors appearing in "
        "collected posts/comments and maintain the {prefix}-authors dataset."
    )
    parser.add_argument(
        "--dataset-prefix",
        default="reddit",
        help="S3 dataset prefix; posts live in {prefix}-posts, comments in "
        "{prefix}-comments, authors in {prefix}-authors (default: reddit)",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="seconds between iterations (default: 300)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="run exactly one iteration, then exit (smoke tests)",
    )
    parser.add_argument(
        "--max-fetch",
        type=int,
        default=DEFAULT_MAX_FETCH,
        help="cap on author profiles fetched per iteration; leftovers are "
        "picked up by the next iteration (default: 500)",
    )
    parser.add_argument(
        "--refetch-days",
        type=float,
        default=DEFAULT_REFETCH_DAYS,
        help="re-fetch profiles older than this many days whose author is "
        "still present in the datasets; 0 disables (default: 7)",
    )
    parser.add_argument(
        "--batch-suffix",
        default=None,
        help="append -{suffix} to the daily authors batch name (testing)",
    )
    parser.add_argument(
        "--cache-file",
        default=DEFAULT_CACHE_FILE,
        help="local JSONL cache of fetched profiles, one entry per line "
        "(default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print candidate authors (one JSON object per line) without "
        "lock, fetch, upload, merge, or index writes",
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="leave the authors batch as JSONL for the daily merge cron",
    )
    parser.add_argument(
        "--skip-auth",
        action="store_true",
        help="make unauthenticated API requests (dev/testing only)",
    )
    parser.add_argument(
        "--api-base",
        default=os.environ.get("REDDIT_API_BASE", DEFAULT_API_BASE),
        help=f"Reddit API base URL (default: {DEFAULT_API_BASE})",
    )
    parser.add_argument(
        "--auth-base",
        default="https://www.reddit.com",
        help="Reddit OAuth base URL (default: https://www.reddit.com)",
    )
    parser.add_argument(
        "--qpm-budget",
        type=int,
        default=90,
        help="max requests per 60 s, client-side pacing (default: 90, under "
        "Reddit's 100 QPM)",
    )
    parser.add_argument(
        "--lock-ttl-ms",
        type=int,
        default=DEFAULT_LOCK_TTL_MS,
        help="S3Lock TTL for the authors lock (default: 3600000)",
    )
    parser.add_argument("--log-level", default="INFO", help="logging level (default: INFO)")
    args = parser.parse_args(argv)

    if args.poll_interval_seconds < 0:
        parser.error("--poll-interval-seconds must be non-negative")
    if args.max_fetch < 1:
        parser.error("--max-fetch must be at least 1")
    if args.refetch_days < 0:
        parser.error("--refetch-days must be non-negative")
    if args.qpm_budget < 1:
        parser.error("--qpm-budget must be at least 1")
    return args


async def main(argv: list[str] | None = None) -> None:
    load_dotenv()  # must run before parse_args (which reads env defaults)
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    bucket = _require_env("S3_BUCKET")
    prefix = os.environ.get("S3_PREFIX", "datasets")
    # Explicit credentials are required even in --dry-run: the DuckDB scans
    # read parquet over httpfs, which cannot use instance credentials.
    endpoint_url = _require_authors_env(
        "S3_ENDPOINT_URL",
        "the DuckDB scan reads parquet over httpfs and needs the endpoint",
    )
    access_key = _require_authors_env(
        "S3_ACCESS_KEY", "the DuckDB scan reads parquet over httpfs and needs explicit keys"
    )
    secret_key = _require_authors_env(
        "S3_SECRET_KEY", "the DuckDB scan reads parquet over httpfs and needs explicit keys"
    )
    if not args.dry_run:
        _require_env("WSS_MUTEX_BASE_URL")

    session = aioboto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    async with session.client("s3", **kwargs) as s3_client:  # type: ignore
        while True:
            await _run_iteration(
                args,
                s3_client,
                bucket,
                prefix,
                endpoint_url=endpoint_url,
                access_key=access_key,
                secret_key=secret_key,
            )
            if args.once:
                break
            await asyncio.sleep(args.poll_interval_seconds)

    logger.info("authors worker stopped")
