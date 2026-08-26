"""Daily backfill daemon: fetch posts and parent comments referenced by
collected comments but missing from the S3 data lake.

The firehose (:mod:`reddit_firehose_tool.main`) only ever ingests items newer
than its stream start, so comments referencing posts (``link_id``, ``t3_...``)
or parent comments (``parent_id``, ``t1_...``) created before collection began
reference records that are missing from ``{prefix}-posts`` /
``{prefix}-comments`` forever.

This process runs once a day (long-running, supervised by systemd, mirroring
the firehose's style): it scans recently collected comments, finds referenced
fullnames missing from the datasets, fetches them via Reddit ``/api/info``,
uploads them into the same datasets (batch ``reddit-firehose-backfill-YYYYMMDD``),
merges them, and marks the day complete.

Design notes:

* The S3 store is the single source of truth for what has been collected.
  Membership checks use a small *prefill index* (``{prefix}/_backfill/names-*``,
  one parquet + manifest per dataset, refreshed incrementally) plus the names
  found in unmerged JSONL chunks, so the full datasets are never re-scanned.
* Concurrency safety uses the WSS MUTEX feature through the pinned
  ``s3_data_tool``'s ``S3Lock`` (a TTL lock file in S3, check-and-set guarded
  by the mutex websocket); the lock is held per daily iteration, never across
  the sleep, and renewed while long work (e.g. ``--full``) runs.
* The daily window overlaps days (default 48 h), so a crashed or skipped day
  is re-covered by the next run; deleted/removed targets are simply omitted by
  ``/api/info`` and age out of the window (no permanent skip list).
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import os
import random
import sys
from collections import deque
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aioboto3
import duckdb
import httpx

from s3_data_tool import S3DataTool
from s3_data_tool.clean_up import merge_dataset_batch
from s3_data_tool.s3_lock import LockRenewalError, S3Lock
from s3_data_tool.s3_utils import (
    enumerate_batches,
    enumerate_parquet_paths,
    iter_jsonl_rows,
    s3_object_exists,
)

from .auth import RedditAuth
from .client import RedditAPIError, RedditClient
from .main import DEFAULT_API_BASE, _require_env, load_dotenv
from .state import FirehoseState
from .stream import COMMENT_ENDPOINT, POST_ENDPOINT, build_record

logger = logging.getLogger(__name__)

#: Reddit /api/info accepts at most 100 fullnames per request.
INFO_CHUNK_SIZE = 100
#: Global backfill lock: S3 lock file ``locks/reddit-firehose-backfill.lock``
#: guarded by the WSS mutex name ``s3lock-reddit-firehose-backfill``.
LOCK_PATH = "reddit-firehose-backfill"
DEFAULT_LOCK_TTL_MS = 3_600_000
#: Firehose hourly batches are ``reddit-firehose-YYYYMMDD-HH``; the backfill's
#: own daily batches are ``reddit-firehose-backfill-YYYYMMDD``.
FIREHOSE_BATCH_PREFIX = "reddit-firehose-"
BACKFILL_BATCH_PREFIX = "reddit-firehose-backfill-"
#: Retry budget for transient failures of the DuckDB scans (one blip should
#: not abort an entire day; the process as a whole is restarted by systemd).
SCAN_RETRIES = 3
#: Sleep in chunks of at most 30 min, recomputed from the wall clock, so NTP
#: jumps or suspend/resume cannot oversleep a day.
SLEEP_CHUNK_SECONDS = 1800.0


# --------------------------------------------------------------------- pure
# helpers


def batch_name_for(date: datetime.datetime, suffix: str | None = None) -> str:
    """Daily backfill batch name (``reddit-firehose-backfill-YYYYMMDD``)."""
    name = f"{BACKFILL_BATCH_PREFIX}{date:%Y%m%d}"
    if suffix:
        name += f"-{suffix}"
    return name


def cutoff_batch_name(now: datetime.datetime, window_hours: float) -> str:
    """Hourly batch name at the window's lower edge.

    Batch names are zero-padded and share a prefix, so lexicographic
    comparison is chronological.
    """
    return (now - datetime.timedelta(hours=window_hours)).strftime("%Y%m%d-%H")


def backfill_batch_date(name: str) -> str | None:
    """Extract the ``%Y%m%d`` date from a backfill batch name, if it has one."""
    if not name.startswith(BACKFILL_BATCH_PREFIX):
        return None
    candidate = name[len(BACKFILL_BATCH_PREFIX):][:8]
    if len(candidate) == 8 and candidate.isdigit():
        return candidate
    return None


def select_recent_batches(batches: Sequence[str], cutoff: str | None) -> list[str]:
    """Keep batches inside the scan window.

    Firehose hourly batches are kept when ``>= cutoff``. Backfill batches are
    kept when their date is ``>= cutoff``'s date (a day of slack is fine: they
    are small, and re-scanning them keeps the fixed-point closure alive across
    days). ``cutoff=None`` keeps everything (``--full``). Other names are
    ignored.
    """
    result: list[str] = []
    cutoff_date = cutoff[:8] if cutoff is not None else None
    for name in batches:
        if name.startswith(BACKFILL_BATCH_PREFIX):
            bdate = backfill_batch_date(name)
            if bdate is not None and (cutoff is None or bdate >= cutoff_date):
                result.append(name)
        elif name.startswith(FIREHOSE_BATCH_PREFIX):
            # ``cutoff`` is the unprefixed timestamp suffix (see
            # cutoff_batch_name), so compare the name's suffix.
            if cutoff is None or name[len(FIREHOSE_BATCH_PREFIX):] >= cutoff:
                result.append(name)
    return result


def filter_parquet_paths(paths: Sequence[str], batches: Sequence[str]) -> list[str]:
    """Keep parquet paths belonging to one of ``batches`` (path contains ``/{batch}/``)."""
    return [p for p in paths if any(f"/{b}/" in p for b in batches)]


def unquote_fullname(value: Any) -> str | None:
    """Undo the lake's JSON-encoding of string values.

    S3 rows are written through ``transform_row_for_jsonl``, which JSON-encodes
    every string, so parquet/JSONL carry ``"t3_abc"`` with literal quote
    characters. Returns the plain fullname, or None for non-strings.
    """
    if not isinstance(value, str):
        return None
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def valid_fullname(value: Any, prefix: str) -> bool:
    """True for a plain fullname like ``t3_abc`` / ``t1_abc`` (never quoted)."""
    return (
        isinstance(value, str)
        and value.startswith(prefix)
        and len(value) > len(prefix)
    )


def is_jsonl_chunk_key(key: str) -> bool:
    """True for ``{run_id}_chunk_%05d.jsonl`` keys (manifests/parquet excluded)."""
    filename = key.rsplit("/", 1)[-1]
    return filename.endswith(".jsonl") and "_chunk_" in filename


def new_refs(
    data: dict[str, Any], known_posts: set[str], known_comments: set[str]
) -> list[str]:
    """Refs revealed by a fetched comment that are still missing.

    ``link_id`` references posts, ``parent_id`` references posts (top-level
    comments) or comments (replies). Revealed names are added to the
    kind-appropriate known set so each name is enqueued at most once.
    """
    refs: list[str] = []
    for field in ("link_id", "parent_id"):
        value = data.get(field)
        if valid_fullname(value, "t3_"):
            if value not in known_posts:
                known_posts.add(value)
                refs.append(value)
        elif valid_fullname(value, "t1_"):
            if value not in known_comments:
                known_comments.add(value)
                refs.append(value)
    return refs


def build_refs_query(
    source_column: str, prefix_literal: str, comment_paths: Sequence[str]
) -> str | None:
    """DuckDB query returning DISTINCT quoted fullname references.

    ``prefix_literal`` includes the opening JSON quote, e.g. ``'"t3_`` (the
    lake stores string values JSON-quoted). Returns None when there is nothing
    to scan.
    """
    if not comment_paths:
        return None
    paths = ", ".join(f"'{p.replace(chr(39), chr(39) * 2)}'" for p in comment_paths)
    return (
        f"SELECT DISTINCT {source_column} AS name "
        f"FROM read_parquet([{paths}]) "
        f"WHERE {source_column} IS NOT NULL AND {source_column} <> '' "
        f"AND starts_with({source_column}, '{prefix_literal}') ORDER BY 1"
    )


def plan_index_refresh(
    manifest: dict[str, Any] | None,
    batch_mtimes: dict[str, str],
    *,
    rebuild: bool = False,
) -> list[str]:
    """Batches whose merged parquet must be (re)scanned into the prefill index.

    A batch needs scanning when it is missing from the manifest or its parquet
    ``LastModified`` changed (a re-merge appended rows).
    """
    if rebuild:
        return sorted(batch_mtimes)
    covered = (manifest or {}).get("batches", {})
    return [
        batch
        for batch, mtime in sorted(batch_mtimes.items())
        if covered.get(batch, {}).get("last_modified") != mtime
    ]


def _parse_run_at(value: str) -> datetime.time:
    try:
        hour_s, minute_s = value.split(":", 1)
        hour, minute = int(hour_s), int(minute_s)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        return datetime.time(hour, minute)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid HH:MM time: {value!r}") from None


def next_run_time(
    now: datetime.datetime, run_at: datetime.time, jitter_offset: float
) -> datetime.datetime:
    """Next occurrence of ``run_at`` strictly after ``now``, plus fixed jitter.

    Naive datetimes, interpreted in UTC (the firehose's batch naming uses local
    time, which is UTC on MILA hosts).
    """
    candidate = now.replace(hour=run_at.hour, minute=run_at.minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += datetime.timedelta(days=1)
    return candidate + datetime.timedelta(seconds=jitter_offset)


# --------------------------------------------------------------------- S3 key
# helpers


def index_key(prefix: str, dataset: str) -> str:
    return f"{prefix}/_backfill/names-{dataset}.parquet"


def index_manifest_key(prefix: str, dataset: str) -> str:
    return f"{prefix}/_backfill/names-{dataset}.manifest.json"


def marker_key(prefix: str, date: datetime.datetime) -> str:
    """Completion marker key for a daily run."""
    return f"{prefix}/_backfill/reddit-firehose-backfill-{date:%Y-%m-%d}.done"


# --------------------------------------------------------------------- async
# S3 helpers


async def list_dataset_state(
    s3_client: Any, bucket: str, prefix: str, dataset: str
) -> tuple[dict[str, str], list[str]]:
    """One paginated pass over a dataset.

    Returns ``(batch -> merged.parquet LastModified ISO string, jsonl chunk
    keys)``. Chunk keys are bounded by the unmerged volume (normally ~a day);
    merged batches have none.
    """
    mtimes: dict[str, str] = {}
    chunk_keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/{dataset}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/merged.parquet") and "/annotations/" not in key:
                # {prefix}/{dataset}/{batch}/merged.parquet
                mtimes[key.rstrip("/").split("/")[-2]] = obj["LastModified"].isoformat()
            elif is_jsonl_chunk_key(key):
                chunk_keys.append(key)
    return mtimes, chunk_keys


async def read_index_manifest(
    s3_client: Any, bucket: str, key: str
) -> dict[str, Any] | None:
    try:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        return json.loads(body)
    except s3_client.exceptions.NoSuchKey:
        return None
    except Exception:
        logger.warning("failed to read index manifest %s; treating as empty", key)
        return None


async def jsonl_scan(
    s3_client: Any, bucket: str, keys: list[str]
) -> tuple[set[str], set[str], set[str]]:
    """Read unmerged JSONL chunks: (names, post refs, comment refs).

    ``iter_jsonl_rows`` reads one whole file at a time, so memory stays
    bounded; row values are JSON-encoded (quoted) and unquoted here.
    """
    names: set[str] = set()
    ref_posts: set[str] = set()
    ref_comments: set[str] = set()
    async for row in iter_jsonl_rows(s3_client, bucket, keys):
        name = unquote_fullname(row.get("name"))
        if name is not None:
            names.add(name)
        link = unquote_fullname(row.get("link_id"))
        if valid_fullname(link, "t3_"):
            ref_posts.add(link)
        parent = unquote_fullname(row.get("parent_id"))
        if valid_fullname(parent, "t1_"):
            ref_comments.add(parent)
    return names, ref_posts, ref_comments


# --------------------------------------------------------------------- DuckDB


async def run_duckdb_query(
    query: str, *, endpoint_url: str, access_key: str, secret_key: str
) -> list[str]:
    """Execute a single-column DuckDB query over s3:// paths; return the rows.

    Runs in a worker thread (DuckDB is synchronous) so the event loop — and
    the lock-renewal task — stay live during long scans. SET configuration
    mirrors ``s3_data_tool.data_filtering._execute_and_stream``.
    """

    def _run() -> list[str]:
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
            rows: list[str] = []
            while True:
                batch = result.fetchmany(1000)
                if not batch:
                    break
                rows.extend(row[0] for row in batch)
            return rows
        finally:
            conn.close()

    return await asyncio.to_thread(_run)


async def run_duckdb_query_retry(
    query: str, *, endpoint_url: str, access_key: str, secret_key: str
) -> list[str]:
    """Like :func:`run_duckdb_query` but retries transient failures."""
    last: BaseException | None = None
    for attempt in range(SCAN_RETRIES):
        try:
            return await run_duckdb_query(
                query, endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key
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


# --------------------------------------------------------------------- prefill
# index


async def load_index(
    s3_client: Any,
    bucket: str,
    key: str,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> set[str]:
    """Load the prefill index parquet into a plain-name set (empty if absent)."""
    if not await s3_object_exists(s3_client, bucket, key):
        return set()
    rows = await run_duckdb_query(
        f"SELECT name FROM read_parquet(['s3://{bucket}/{key}'])",
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )
    return {name for row in rows if (name := unquote_fullname(row)) is not None}


async def maintain_index(
    s3_client: Any,
    bucket: str,
    prefix: str,
    dataset: str,
    names: set[str],
    batch_mtimes: dict[str, str],
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    rebuild: bool = False,
) -> bool:
    """Union names from newly merged batches into ``names`` (in memory).

    Returns True when the index needs rewriting (batches were scanned). The
    caller decides when to persist via :func:`write_index` so a single write
    per iteration can also absorb the names fetched this run.
    """
    manifest = await read_index_manifest(s3_client, bucket, index_manifest_key(prefix, dataset))
    to_scan = plan_index_refresh(manifest, batch_mtimes, rebuild=rebuild)
    if not to_scan:
        return False
    logger.info("index %s: scanning %d new/changed batches", dataset, len(to_scan))
    for batch in to_scan:
        query = (
            f"SELECT DISTINCT name FROM read_parquet("
            f"['s3://{bucket}/{prefix}/{dataset}/{batch}/merged.parquet'])"
        )
        rows = await run_duckdb_query_retry(
            query, endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key
        )
        names.update(name for row in rows if (name := unquote_fullname(row)) is not None)
    return True


async def write_index(
    s3_client: Any,
    bucket: str,
    prefix: str,
    dataset: str,
    names: set[str],
    batch_mtimes: dict[str, str],
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Rewrite the prefill index parquet + manifest from a plain-name set.

    Names are re-quoted to match the lake's string encoding. The parquet is
    written to a ``.temp`` key then copied into place (the library's own merge
    pattern); the manifest is written last, so a crash in between leaves the
    manifest stale and the affected batches are simply re-scanned next run.
    """
    key = index_key(prefix, dataset)
    quoted = [json.dumps(name) for name in sorted(names)]
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
                f"COPY (SELECT DISTINCT name FROM (SELECT unnest(?) AS name)) "
                f"TO 's3://{bucket}/{temp_key}' (FORMAT PARQUET)",
                [quoted],
            )
        finally:
            conn.close()

    await asyncio.to_thread(_write)
    await s3_client.copy_object(
        Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": temp_key}
    )
    await s3_client.delete_object(Bucket=bucket, Key=temp_key)

    manifest = {
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "batches": {
            batch: {
                "parquet_key": f"{prefix}/{dataset}/{batch}/merged.parquet",
                "last_modified": mtime,
            }
            for batch, mtime in sorted(batch_mtimes.items())
        },
    }
    await s3_client.put_object(
        Bucket=bucket,
        Key=index_manifest_key(prefix, dataset),
        Body=json.dumps(manifest).encode(),
    )


# --------------------------------------------------------------------- closure


@dataclass
class FetchResult:
    posts: list[dict[str, Any]] = field(default_factory=list)
    comments: list[dict[str, Any]] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    remaining: list[str] = field(default_factory=list)
    failed_chunks: int = 0

    @property
    def fetched(self) -> int:
        return len(self.posts) + len(self.comments)


async def closure_fetch(
    client: RedditClient,
    post_candidates: set[str],
    comment_candidates: set[str],
    known_posts: set[str],
    known_comments: set[str],
    fetched_at: str,
    *,
    max_fetch: int,
) -> FetchResult:
    """Fetch missing fullnames via /api/info, expanding revealed refs to a fixed point.

    A fetched comment's own ``link_id``/``parent_id`` may reference records
    that are still missing; they are enqueued (in-memory set lookups only) and
    the loop runs until the queue drains or ``max_fetch`` is exhausted. Names
    requested but omitted by the API (deleted/removed) land in ``missing`` and
    age out of the scan window — there is no permanent skip list.
    """
    queue: deque[str] = deque(sorted(post_candidates | comment_candidates))
    known_posts.update(post_candidates)
    known_comments.update(comment_candidates)

    result = FetchResult()
    returned: set[str] = set()
    requested_ok: list[str] = []

    while queue and result.fetched < max_fetch:
        # Cap the chunk so every returned child can be stored (no silent loss).
        take = min(INFO_CHUNK_SIZE, len(queue), max_fetch - result.fetched)
        chunk = [queue.popleft() for _ in range(take)]
        try:
            children = await client.get_info(chunk)
        except (RedditAPIError, httpx.HTTPError, json.JSONDecodeError) as exc:
            result.failed_chunks += 1
            logger.warning(
                "get_info chunk failed (%r); %d names retried next window", exc, len(chunk)
            )
            continue
        requested_ok.extend(chunk)

        for child in children:
            if not isinstance(child, dict) or not isinstance(child.get("data"), dict):
                continue
            data = child["data"]
            name = data.get("name")
            if not isinstance(name, str):
                continue
            kind = child.get("kind")
            if kind == "t3":
                result.posts.append(build_record(child, POST_ENDPOINT, fetched_at))
            elif kind == "t1":
                result.comments.append(build_record(child, COMMENT_ENDPOINT, fetched_at))
                queue.extend(new_refs(data, known_posts, known_comments))
            else:
                logger.warning("unexpected kind %r for %s; skipped", kind, name)
                continue
            returned.add(name)

    result.missing = sorted(name for name in requested_ok if name not in returned)
    result.remaining = list(queue)
    return result


# --------------------------------------------------------------------- lock /
# loop plumbing


async def _renew_lock_task(lock: S3Lock, work_task: asyncio.Task) -> None:
    """Renew the S3Lock periodically; cancel the work task if renewal fails.

    Mirrors the renewal loop documented in ``s3_data_tool.s3_lock``'s module
    docstring (``gather_subject_to_lock_renewal`` renews only once, which
    would kill long ``--full`` runs).
    """
    while True:
        await asyncio.sleep((lock._ttl_ms / 1000) - 60)
        try:
            await lock.renew()
        except LockRenewalError:
            work_task.cancel()
            return


async def _sleep_until_run(
    run_at: datetime.time,
    jitter_offset: float,
    *,
    sleep: Any = asyncio.sleep,
) -> None:
    target = next_run_time(datetime.datetime.now(), run_at, jitter_offset)
    logger.info("next backfill run at %s (UTC)", target.strftime("%Y-%m-%d %H:%M:%S"))
    while True:
        remaining = (target - datetime.datetime.now()).total_seconds()
        if remaining <= 0:
            return
        await sleep(min(remaining, SLEEP_CHUNK_SECONDS))


async def _upload_records(records: list[dict[str, Any]], dataset: str, batch: str) -> None:
    """Upload built records into a dataset (same pattern as the firehose)."""
    if not records:
        return

    async def records_iter() -> AsyncIterator[dict[str, Any]]:
        for record in records:
            yield record

    async with S3DataTool().dataset_generator() as dataset_generator:
        await dataset_generator.from_async_iterator(
            records_iter(),
            name=dataset,
            batch=batch,
            streaming_configs=S3DataTool.StreamingConfigs(chunk_size=100),
            deduplicate_on=["name"],
        )


async def _merge_own_batch(
    s3_client: Any,
    bucket: str,
    prefix: str,
    dataset: str,
    batch: str,
    lock_ttl_ms: int,
) -> bool:
    """Merge the backfill's own batch (per-batch lock, same path as run_clean_up).

    Returns False when the batch was locked by another process (the merge cron
    will finish it). The firehose never writes to ``reddit-firehose-backfill-*``
    batches, so there is no producer race.
    """
    batch_prefix = f"{prefix}/{dataset}/{batch}"
    lock = S3Lock(batch_prefix, lock_ttl_ms, s3_client, bucket)
    async with lock:
        if not lock:
            logger.info("batch %s locked by another process; skipping merge", batch_prefix)
            return False
        await merge_dataset_batch(s3_client, bucket, batch_prefix)
        return True


# --------------------------------------------------------------------- the
# daily iteration


def _require_backfill_env(name: str, why: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"environment variable {name} must be set for backfill.py: {why}")
    return value


async def _backfill_work(
    args: argparse.Namespace,
    s3_client: Any,
    bucket: str,
    prefix: str,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    date: datetime.datetime,
    write: bool,
) -> None:
    posts_dataset = f"{args.dataset_prefix}-posts"
    comments_dataset = f"{args.dataset_prefix}-comments"
    batch = batch_name_for(date, args.batch_suffix)
    now = datetime.datetime.now()

    # ---- lake state: one paginated pass per dataset --------------------------
    posts_mtimes, posts_chunk_keys = await list_dataset_state(
        s3_client, bucket, prefix, posts_dataset
    )
    comments_mtimes, comments_chunk_keys = await list_dataset_state(
        s3_client, bucket, prefix, comments_dataset
    )

    collected_posts = await load_index(
        s3_client,
        bucket,
        index_key(prefix, posts_dataset),
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )
    collected_comments = await load_index(
        s3_client,
        bucket,
        index_key(prefix, comments_dataset),
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )

    # The index refresh scan is skipped in dry runs (read-only, repeated
    # dry-runs would otherwise rescan everything the manifest has not seen).
    index_dirty = False
    if write:
        index_dirty |= await maintain_index(
            s3_client,
            bucket,
            prefix,
            posts_dataset,
            collected_posts,
            posts_mtimes,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            rebuild=args.rebuild_index,
        )
        index_dirty |= await maintain_index(
            s3_client,
            bucket,
            prefix,
            comments_dataset,
            collected_comments,
            comments_mtimes,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            rebuild=args.rebuild_index,
        )

    # ---- candidates: merged comment parquet inside the window ----------------
    all_comment_batches = await enumerate_batches(s3_client, bucket, prefix, comments_dataset)
    cutoff = None if args.full else cutoff_batch_name(now, args.window_hours)
    comment_batches = select_recent_batches(all_comment_batches, cutoff)
    comment_paths = filter_parquet_paths(
        await enumerate_parquet_paths(s3_client, bucket, prefix, comments_dataset),
        comment_batches,
    )
    logger.info(
        "scanning %d comment batches (window=%s)",
        len(comment_batches),
        "full" if args.full else f"{args.window_hours}h",
    )

    post_candidates: set[str] = set()
    comment_candidates: set[str] = set()
    for source_column, prefix_literal, valid_prefix in (
        ("link_id", '"t3_', "t3_"),
        ("parent_id", '"t1_', "t1_"),
    ):
        query = build_refs_query(source_column, prefix_literal, comment_paths)
        if query is None:
            continue
        rows = await run_duckdb_query_retry(
            query, endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key
        )
        for row in rows:
            name = unquote_fullname(row)
            if valid_fullname(name, valid_prefix):
                if valid_prefix == "t3_":
                    post_candidates.add(name)
                else:
                    comment_candidates.add(name)

    # ---- unmerged JSONL chunks: known names + extra candidates ---------------
    # All unmerged chunks (any age) count as collected — the S3 store is the
    # single source of truth; the volume is bounded by the merge cron's lag.
    jsonl_posts, _, _ = await jsonl_scan(s3_client, bucket, posts_chunk_keys)
    jsonl_comments, ref_posts, ref_comments = await jsonl_scan(
        s3_client, bucket, comments_chunk_keys
    )
    post_candidates |= ref_posts
    comment_candidates |= ref_comments

    known_posts = collected_posts | jsonl_posts
    known_comments = collected_comments | jsonl_comments
    post_candidates -= known_posts
    comment_candidates -= known_comments

    logger.info(
        "candidates: %d posts, %d comments (known: %d posts, %d comments)",
        len(post_candidates),
        len(comment_candidates),
        len(known_posts),
        len(known_comments),
    )
    if args.dry_run:
        for name in sorted(post_candidates):
            print(name)
        for name in sorted(comment_candidates):
            print(name)
        return

    # ---- fetch → upload → merge → index → marker -----------------------------
    merged_posts = merged_comments = True
    fetched = 0
    missing: list[str] = []
    remaining: list[str] = []
    failed_chunks = 0

    async def merge_own_batches() -> None:
        nonlocal merged_posts, merged_comments
        merged_posts = await _merge_own_batch(
            s3_client, bucket, prefix, posts_dataset, batch, args.lock_ttl_ms
        )
        merged_comments = await _merge_own_batch(
            s3_client, bucket, prefix, comments_dataset, batch, args.lock_ttl_ms
        )

    if post_candidates or comment_candidates:
        client_id = _require_backfill_env(
            "REDDIT_CLIENT_ID", "fetching missing records requires app credentials"
        )
        client_secret = _require_backfill_env(
            "REDDIT_CLIENT_SECRET", "fetching missing records requires app credentials"
        )
        redirect_uri = os.environ.get("REDDIT_REDIRECT_URI", "http://localhost:8080")
        user_agent = os.environ.get(
            "REDDIT_USER_AGENT", "linux:reddit-firehose-backfill:v0.1.0 (by /u/unknown)"
        )

        fetched_at = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
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
                    # refresh token are never touched by the backfill.
                    FirehoseState.load(Path("reddit_firehose_backfill_state.json")),
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

            result = await closure_fetch(
                client,
                post_candidates,
                comment_candidates,
                known_posts,
                known_comments,
                fetched_at,
                max_fetch=args.max_fetch,
            )
            fetched = result.fetched
            missing = result.missing
            remaining = result.remaining
            failed_chunks = result.failed_chunks
            logger.info(
                "fetched %d records (%d posts, %d comments); %d omitted (deleted?), "
                "%d queued beyond budget, %d failed chunks",
                fetched,
                len(result.posts),
                len(result.comments),
                len(missing),
                len(remaining),
                failed_chunks,
            )

            await _upload_records(result.posts, posts_dataset, batch)
            await _upload_records(result.comments, comments_dataset, batch)

            if fetched:
                collected_posts.update(record["name"] for record in result.posts)
                collected_comments.update(record["name"] for record in result.comments)
                index_dirty = True

            if not args.skip_merge:
                await merge_own_batches()
    else:
        logger.info("nothing to fetch; marking the day complete")
        if not args.skip_merge:
            # No-op merges (nothing written), but keeps the marker honest.
            await merge_own_batches()

    if index_dirty:
        # NOTE: batch_mtimes predate this run's merge, so the manifest does not
        # yet claim the backfill batch — next run re-scans it (idempotent union).
        await write_index(
            s3_client,
            bucket,
            prefix,
            posts_dataset,
            collected_posts,
            posts_mtimes,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
        )
        await write_index(
            s3_client,
            bucket,
            prefix,
            comments_dataset,
            collected_comments,
            comments_mtimes,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
        )

    await s3_client.put_object(
        Bucket=bucket,
        Key=marker_key(prefix, date),
        Body=json.dumps(
            {
                "completed_at": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "dataset_prefix": args.dataset_prefix,
                "batch": batch,
                "window_hours": args.window_hours,
                "full": args.full,
                "candidates": {
                    "posts": len(post_candidates),
                    "comments": len(comment_candidates),
                },
                "fetched": fetched,
                "missing": len(missing),
                "remaining": len(remaining),
                "failed_chunks": failed_chunks,
                "merged": bool(merged_posts and merged_comments and not args.skip_merge),
            }
        ).encode(),
    )


async def _run_daily_iteration(
    args: argparse.Namespace,
    s3_client: Any,
    bucket: str,
    prefix: str,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
) -> None:
    date = datetime.datetime.now()
    marker = marker_key(prefix, date)

    if args.dry_run:
        await _backfill_work(
            args,
            s3_client,
            bucket,
            prefix,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            date=date,
            write=False,
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
        logger.info("backfill already running elsewhere; skipping this iteration")
        return

    try:
        if not args.force and await s3_object_exists(s3_client, bucket, marker):
            logger.info("backfill for %s already complete; skipping", date.strftime("%Y-%m-%d"))
            return

        work = asyncio.create_task(
            _backfill_work(
                args,
                s3_client,
                bucket,
                prefix,
                endpoint_url=endpoint_url,
                access_key=access_key,
                secret_key=secret_key,
                date=date,
                write=True,
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
        description="Daily backfill of posts and parent comments referenced by "
        "collected comments but missing from the S3 data lake."
    )
    parser.add_argument(
        "--dataset-prefix",
        default="reddit",
        help="S3 dataset prefix; posts live in {prefix}-posts, comments in "
        "{prefix}-comments (default: reddit)",
    )
    parser.add_argument(
        "--window-hours",
        type=float,
        default=48.0,
        help="scan comment batches newer than this many hours (default: 48; "
        "overlapping days make missed runs self-healing)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="scan ALL collected comment batches, not just the window "
        "(one-time historical backfill)",
    )
    parser.add_argument(
        "--run-at",
        type=_parse_run_at,
        default=datetime.time(3, 0),
        help="daily run time HH:MM, interpreted in UTC (default: 03:00)",
    )
    parser.add_argument(
        "--jitter-seconds",
        type=float,
        default=900.0,
        help="per-process random delay added to each run time (default: 900)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="run exactly one daily iteration, then exit (smoke tests)",
    )
    parser.add_argument(
        "--batch-suffix",
        default=None,
        help="append -{suffix} to the daily backfill batch name (testing)",
    )
    parser.add_argument(
        "--max-fetch",
        type=int,
        default=10_000,
        help="cap on records fetched per iteration; leftovers are recovered "
        "via the overlapping window (default: 10000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print candidate fullnames (one per line) without lock, fetch, "
        "upload, merge, marker, or index writes",
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="leave the backfill batch as JSONL for the daily merge cron",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="ignore the day's completion marker and run again",
    )
    parser.add_argument(
        "--rebuild-index",
        action="store_true",
        help="rebuild the prefill index from scratch this iteration "
        "(corruption recovery; one-time full scan cost)",
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
        help="S3Lock TTL for the backfill lock (default: 3600000)",
    )
    parser.add_argument("--log-level", default="INFO", help="logging level (default: INFO)")
    args = parser.parse_args(argv)

    if args.window_hours <= 0:
        parser.error("--window-hours must be positive")
    if args.max_fetch < 1:
        parser.error("--max-fetch must be at least 1")
    if args.jitter_seconds < 0:
        parser.error("--jitter-seconds must be non-negative")
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
    endpoint_url = _require_backfill_env(
        "S3_ENDPOINT_URL", "the DuckDB scan reads parquet over httpfs and needs the endpoint"
    )
    access_key = _require_backfill_env(
        "S3_ACCESS_KEY", "the DuckDB scan reads parquet over httpfs and needs explicit keys"
    )
    secret_key = _require_backfill_env(
        "S3_SECRET_KEY", "the DuckDB scan reads parquet over httpfs and needs explicit keys"
    )
    if not args.dry_run:
        _require_env("WSS_MUTEX_BASE_URL")

    jitter_offset = random.uniform(0, args.jitter_seconds)

    session = aioboto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    async with session.client("s3", **kwargs) as s3_client:  # type: ignore
        while True:
            await _run_daily_iteration(
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
            await _sleep_until_run(args.run_at, jitter_offset)

    logger.info("backfill stopped")
