"""Entrypoint: run the Reddit firehose and stream records into the S3 data lake.

Mirrors the long-running batch pattern of bsky_live.py: the process runs
forever, splitting the stream into hourly batches (``reddit-firehose-YYYYMMDD-HH``)
uploaded to two datasets, ``{prefix}-posts`` and ``{prefix}-comments``, via the
s3-data-tool library.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import httpx
from s3_data_tool import S3DataTool

from .auth import RedditAuth
from .client import RedditClient
from .state import FirehoseState
from .stream import COMMENT_ENDPOINT, POST_ENDPOINT, RedditFirehose, iter_until_timeout

logger = logging.getLogger(__name__)

#: Authenticated Reddit API host. api.reddit.com rejects application-only
#: OAuth tokens; oauth.reddit.com is the canonical host for bearer requests.
DEFAULT_API_BASE = "https://oauth.reddit.com"


def load_dotenv(path: Path | None = None) -> None:
    """Load a plain ``KEY=VALUE`` .env file into os.environ.

    The file needs no ``export`` keywords (values may contain spaces,
    parentheses, etc.); existing environment variables always win.
    """
    path = path if path is not None else Path(os.environ.get("ENV_FILE", ".env"))
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll Reddit subreddits for new posts and comments and "
        "stream them into the S3 data lake."
    )
    parser.add_argument(
        "--subreddits",
        required=True,
        help="comma-separated list of subreddits to monitor (e.g. Quebec,Montreal)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=60.0,
        help="seconds between poll cycles (default: 60, aligned with the 100 QPM OAuth limit)",
    )
    parser.add_argument(
        "--dataset-prefix",
        default="reddit",
        help="S3 dataset prefix; posts go to {prefix}-posts, comments to {prefix}-comments (default: reddit)",
    )
    parser.add_argument(
        "--state-file",
        default="reddit_firehose_state.json",
        help="local JSON state file for cursors and the OAuth refresh token (default: %(default)s)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=25,
        help="max listing pages per (subreddit, endpoint) per cycle (default: 25)",
    )
    parser.add_argument(
        "--limit", type=int, default=100, help="items per page (default: 100, Reddit max)"
    )
    parser.add_argument(
        "--batch-seconds",
        type=float,
        default=3600.0,
        help="length of each S3 batch (default: 3600)",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="run at most this many poll cycles, then exit (useful for smoke tests)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print one JSON record per line to stdout instead of uploading to S3",
    )
    parser.add_argument(
        "--skip-auth",
        action="store_true",
        help="make unauthenticated requests (dev/testing only; much lower rate limits)",
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
        help="max requests per 60 s, client-side pacing (default: 90, under Reddit's 100 QPM)",
    )
    parser.add_argument(
        "--no-backfill",
        action="store_true",
        help="on first run, do not capture the current first page; only start from now",
    )
    parser.add_argument("--log-level", default="INFO", help="logging level (default: INFO)")
    args = parser.parse_args(argv)

    args.subreddits = [s.strip() for s in args.subreddits.split(",") if s.strip()]
    if not args.subreddits:
        parser.error("--subreddits must contain at least one subreddit")
    return args


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"environment variable {name} must be set (see .env.example)")
    return value


async def _run_batch_pair(
    posts_firehose: RedditFirehose,
    comments_firehose: RedditFirehose,
    dataset_prefix: str,
    batch_name: str,
    args: argparse.Namespace,
) -> None:
    """Consume both firehoses concurrently for one batch."""
    if args.dry_run:
        async def drain(firehose: RedditFirehose) -> None:
            async for record in iter_until_timeout(firehose, args.batch_seconds):
                print(json.dumps(record, ensure_ascii=False), flush=True)
            await firehose.aclose()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(drain(posts_firehose))
            tg.create_task(drain(comments_firehose))
        return

    async def upload(name: str, firehose: RedditFirehose) -> None:
        async with S3DataTool().dataset_generator() as dataset_generator:
            await dataset_generator.from_async_iterator(
                iter_until_timeout(firehose, args.batch_seconds),
                name=name,
                batch=batch_name,
                streaming_configs=S3DataTool.StreamingConfigs(chunk_size=100),
                deduplicate_on=["name"],  # list of columns
            )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(upload(f"{dataset_prefix}-posts", posts_firehose))
        tg.create_task(upload(f"{dataset_prefix}-comments", comments_firehose))


async def main(argv: list[str] | None = None) -> None:
    load_dotenv()  # must run before parse_args (which reads env defaults)
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    redirect_uri = os.environ.get("REDDIT_REDIRECT_URI", "http://localhost:8080")
    user_agent = os.environ.get(
        "REDDIT_USER_AGENT", "linux:reddit-firehose:v0.1.0 (by /u/unknown)"
    )

    client_id = client_secret = None
    if not args.skip_auth:
        client_id = _require_env("REDDIT_CLIENT_ID")
        client_secret = _require_env("REDDIT_CLIENT_SECRET")
    if not args.dry_run:
        _require_env("S3_BUCKET")

    peak_requests = 2 * len(args.subreddits) * args.max_pages
    if peak_requests > args.qpm_budget:
        logger.warning(
            "worst-case load %d requests/cycle exceeds the %d QPM budget — "
            "consider fewer subreddits or a smaller --max-pages",
            peak_requests,
            args.qpm_budget,
        )

    state = FirehoseState.load(Path(args.state_file))

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
                state,
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

        while True:
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H")
            batch_name = f"reddit-firehose-{timestamp}"
            logger.info(
                "starting batch %s (subreddits=%s, interval=%ss)",
                batch_name,
                ",".join(args.subreddits),
                args.interval,
            )

            posts_firehose = RedditFirehose(
                client,
                state,
                args.subreddits,
                endpoints=(POST_ENDPOINT,),
                interval=args.interval,
                max_pages=args.max_pages,
                limit=args.limit,
                backfill=not args.no_backfill,
                max_cycles=args.max_cycles,
            )
            comments_firehose = RedditFirehose(
                client,
                state,
                args.subreddits,
                endpoints=(COMMENT_ENDPOINT,),
                interval=args.interval,
                max_pages=args.max_pages,
                limit=args.limit,
                backfill=not args.no_backfill,
                max_cycles=args.max_cycles,
            )

            await _run_batch_pair(
                posts_firehose, comments_firehose, args.dataset_prefix, batch_name, args
            )

            if args.max_cycles is not None:
                logger.info("max-cycles reached; exiting")
                break

    logger.info("firehose stopped")
