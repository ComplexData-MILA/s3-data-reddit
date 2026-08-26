# Reddit Firehose

A long-running poller that archives new posts and comments from specific
subreddits into the MILA S3 data lake. It queries the Reddit API with
[`httpx`](https://github.com/encode/httpx) on a cron-like 60-second cycle
(aligned with Reddit's 100 QPM OAuth rate limit), authenticates via OAuth2,
and keeps a small local state file so it can resume cleanly after restarts.

Data lands in **two separate S3 datasets**:

- `reddit-posts` — one row per post (`t3_...`), with `title`, `selftext`,
  `url`, `num_comments`, `upvote_ratio`, …
- `reddit-comments` — one row per comment (`t1_...`), with `body`, `link_id`,
  `parent_id`, …

The eventual join is **`comments.link_id == posts.name`** (both columns are
Reddit fullnames, and `name`/`id` are also the row dedup keys).

## Setup

### 1. Register a Reddit app

Create an app at <https://www.reddit.com/prefs/apps>. A **"personal use
script"** app is all you need:

- Fill `REDDIT_CLIENT_ID` and `REDDIT_CLIENT_SECRET` in `.env`.
- Done — the script authenticates itself automatically via the
  `client_credentials` grant (an anonymous, application-only token, valid
  24 h, re-requested on each process start). No login, no redirect URI, no
  username/password.

If you only have a **"web"** app (redirect URI required), see
[Web-app login (alternative)](#web-app-login-alternative) below.

### 2. Configure the environment

```bash
cp .env.example .env
# fill in REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT, and the S3_* variables
```

`.env` uses plain `KEY=VALUE` lines — no `export` keywords. The script loads
it automatically at startup (real environment variables take precedence), so
there is nothing to source. `REDDIT_USER_AGENT` must be descriptive, e.g.
`linux:reddit-firehose:v0.1.0 (by /u/your_username)`.

### 3. Install dependencies

```bash
uv sync --group dev
```

## Web-app login (alternative)

For a "web" app, set `REDDIT_REDIRECT_URI` in `.env` (register the same URI
in the app settings, e.g. `http://localhost:8080`). On first run the script
prints an authorization URL. Open it, log in, click **Allow**. The browser
then redirects to `http://localhost:8080/?state=...&code=...` — the page
**fails to load, which is expected**. Copy the full URL from the browser's
address bar and paste it back into the terminal. The refresh token is stored
in the state file (mode `0600`), so all subsequent runs are fully
non-interactive. (Headless alternative: run the flow once anywhere and copy
the state file, or set `REDDIT_REFRESH_TOKEN` in `.env`.)

## Usage

```
uv run python main.py --subreddits Quebec,Montreal [options]
```

| Option | Default | Meaning |
|---|---|---|
| `--subreddits` | *(required)* | comma-separated subreddits to monitor |
| `--interval` | `60` | seconds between poll cycles (aligns with 100 QPM) |
| `--dataset-prefix` | `reddit` | datasets are `{prefix}-posts` and `{prefix}-comments` |
| `--state-file` | `reddit_firehose_state.json` | local progress + OAuth token file |
| `--max-pages` | `25` | listing pages per subreddit × endpoint per cycle |
| `--limit` | `100` | items per page (Reddit max) |
| `--batch-seconds` | `3600` | length of one S3 batch (`reddit-firehose-YYYYMMDD-HH`) |
| `--max-cycles` | *(unlimited)* | exit after N cycles (smoke tests) |
| `--dry-run` | off | print JSON records to stdout instead of uploading to S3 |
| `--skip-auth` | off | unauthenticated requests (dev only, much lower limits) |
| `--api-base` | `https://oauth.reddit.com` | Reddit API base (authenticated calls must use oauth.reddit.com — api.reddit.com rejects app-only tokens) |
| `--no-backfill` | off | first run: start from "now" instead of capturing page 1 |

## How it works

- Every cycle, each subreddit's `/new` (posts) and `/comments` (comments)
  listings are polled (`limit=100`, paginated via `after` up to `--max-pages`).
- Progress per (subreddit, endpoint) is a `last_seen_utc` cursor plus the
  fullnames seen at the cursor's boundary second (Reddit timestamps are
  second-quantized, so ties are common). The cursor only advances after a
  stream polls successfully, so a failure never loses items.
- The process runs forever, splitting the stream into hourly batches uploaded
  by `s3-data-tool` as JSONL chunks + run manifests:

  ```
  {S3_PREFIX}/{dataset}/reddit-firehose-YYYYMMDD-HH/{run_id}_chunk_00000.jsonl
  {S3_PREFIX}/{dataset}/reddit-firehose-YYYYMMDD-HH/{run_id}.manifest.json
  ```

- Deduplication on the `name` column happens at the data-lake-pipeline
  merge/cleanup step, so the rare re-emitted boundary row (e.g. after a crash
  mid-cycle) is absorbed there.

### Rate limiting

The client paces itself to a configurable QPM budget (`--qpm-budget`, default
90 — under Reddit's 100 QPM OAuth limit), honors `x-ratelimit-*` headers and
`Retry-After`, and retries 429/5xx/network errors with exponential backoff.
Startup warns if `2 × subreddits × max_pages` could exceed the budget.

## Post / parent-comment backfill

The firehose only ever ingests items newer than its stream start, so comments
referencing **posts** (`link_id`, `t3_...`) or **parent comments**
(`parent_id`, `t1_...`) created before collection began reference records
missing from the datasets forever. `backfill.py` closes that gap: a
long-running daemon (systemd, same supervised-process style as `main.py`)
that runs one iteration per day:

1. Scans comments collected in the last `--window-hours` (default 48 h,
   batch-name windowed — overlapping days make missed runs self-healing).
2. Finds referenced fullnames missing from the datasets, where membership
   comes from a small **prefill index** maintained in S3
   (`{prefix}/_backfill/names-{dataset}.parquet` + manifest, refreshed
   incrementally from newly merged batches) plus the names found in unmerged
   JSONL chunks. The S3 store is the single source of truth; the full
   datasets are never re-scanned daily.
3. Fetches the missing records via Reddit `/api/info` (≤100 fullnames per
   request, under the same QPM budget as the firehose), following newly
   revealed references to a fixed point (bounded by `--max-fetch`).
4. Uploads posts to `{prefix}-posts` and comments to `{prefix}-comments`,
   both in the daily batch `reddit-firehose-backfill-YYYYMMDD`, merges them,
   updates the prefill index, and writes a completion marker
   (`{prefix}/_backfill/reddit-firehose-backfill-YYYY-MM-DD.done`).

Concurrency safety uses the WSS MUTEX service (cf-workers-mutex) through
`s3-data-tool`'s `S3Lock`: the per-day lock is a TTL lock file in S3 whose
check-and-set is guarded by the mutex websocket, held per iteration (renewed
while work runs, never held across the sleep). A second instance of the
backfill simply skips its iteration while the lock is held.

```
uv run python backfill.py [options]
```

| Option | Default | Meaning |
|---|---|---|
| `--dataset-prefix` | `reddit` | datasets are `{prefix}-posts` and `{prefix}-comments` |
| `--window-hours` | `48` | scan comment batches newer than this many hours |
| `--full` | off | scan ALL collected comment batches (one-time historical backfill) |
| `--run-at` | `03:00` | daily run time `HH:MM`, interpreted in UTC |
| `--jitter-seconds` | `900` | per-process random delay added to each run time |
| `--once` | off | run exactly one iteration, then exit (smoke tests) |
| `--max-fetch` | `10000` | cap on records fetched per iteration |
| `--dry-run` | off | print candidate fullnames without lock/fetch/uploads |
| `--skip-merge` | off | leave the batch as JSONL for the daily merge cron |
| `--force` | off | ignore the day's completion marker |
| `--rebuild-index` | off | rebuild the prefill index from scratch (one-time full scan) |
| `--skip-auth` | off | unauthenticated requests (dev only) |
| `--api-base` | `https://oauth.reddit.com` | Reddit API base (same requirement as main.py) |
| `--qpm-budget` | `90` | max requests per 60 s (under Reddit's 100 QPM) |
| `--lock-ttl-ms` | `3600000` | S3Lock TTL (crash-safety window) |

**Environment**: everything `main.py` needs, plus — unlike the firehose —
`S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` are **required** for
`backfill.py` (even in `--dry-run`): its DuckDB scans read parquet over
httpfs, which cannot use instance credentials.

**Deployment** (see `deploy/backfill.service` for a ready systemd unit):
run a one-time historical pass first, then enable the daily service:

```bash
uv run python backfill.py --once --full --max-fetch 1000   # historical catch-up
cp deploy/backfill.service ~/.config/systemd/user/         # set User= + paths
systemctl --user daemon-reload && systemctl --user enable --now backfill
```

Notes: SIGTERM does not release the S3 lock gracefully — the lock TTL and the
daily completion marker make restarts idempotent. Deleted/removed targets are
simply omitted by `/api/info` and age out of the window (no permanent skip
list). Backfilled batches are named `reddit-firehose-backfill-YYYYMMDD` and
merge/dedupe exactly like firehose batches.

## Author profile enrichment

The firehose stores each record's `author` username and, inside the `raw`
blob, the author's `author_fullname` (`t2_...` id) — but no profile details.
`authors.py` builds a third dataset: a long-running daemon (systemd, same
supervised-process style as `backfill.py`) that

1. scans all merged `{prefix}-posts` / `{prefix}-comments` parquet (DuckDB
   over httpfs — cheap at the current scale) for distinct authors;
2. fetches each author's profile once via Reddit `/user/{name}/about`, under
   the same QPM budget as the firehose;
3. uploads one record per author to `{prefix}-authors` (daily batch
   `reddit-firehose-authors-YYYYMMDD`), merges it, and repeats every
   `--poll-interval-seconds`.

**One query per author.** A small author index in S3
(`{prefix}/_authors/authors-index.parquet`) records every author already
fetched — including `not_found` tombstones for suspended/deleted accounts —
so an author is never queried twice, across restarts and worker instances.
The only re-query path is the staleness window: profiles older than
`--refetch-days` (default 7) whose author is still present in the datasets
are refreshed, at most once per window (`0` disables). A local JSONL cache
(`author_profiles_cache.jsonl`) absorbs API calls after crashes; it is an
optimization only — the S3 index is authoritative.

**Schema** (`{prefix}-authors`, one row per unique author): `id`/`name` (the
`t2_...` fullname — the join key to posts/comments via their
`raw -> author_fullname`), `author` (username), `author_created_utc`,
`author_link_karma`, `author_comment_karma`, `author_total_karma`,
`author_awardee_karma`, `author_awarder_karma`, `author_is_mod`,
`author_is_employee`, `author_is_gold`, `author_verified`,
`author_has_verified_email`, `author_icon_img`, `author_subreddit`,
`author_profile_raw` (the full `/about` payload), `fetched_at`,
`first_seen_at`, `last_seen_at`. Like the base datasets, string values are
JSON-encoded in the lake. Suspended/deleted accounts get an index tombstone
but no dataset row; their rows can be found via the index. Refetched
profiles are uploaded as new records in the new daily batch — consumers
dedupe with `GROUP BY name`.

Concurrency safety uses the same WSS MUTEX + `S3Lock` scheme as `backfill.py`
(lock path `reddit-firehose-authors`, held per iteration, never across the
sleep).

```
uv run python authors.py [options]
```

| Option | Default | Meaning |
|---|---|---|
| `--dataset-prefix` | `reddit` | datasets are `{prefix}-posts`, `{prefix}-comments`, `{prefix}-authors` |
| `--poll-interval-seconds` | `300` | seconds between iterations |
| `--once` | off | run exactly one iteration, then exit (smoke tests) |
| `--max-fetch` | `500` | cap on profiles fetched per iteration; leftovers are picked up next iteration |
| `--refetch-days` | `7` | staleness window for re-fetching; `0` disables |
| `--batch-suffix` | *(none)* | append `-{suffix}` to the daily batch name (testing) |
| `--cache-file` | `author_profiles_cache.jsonl` | local profile cache, one JSON entry per line |
| `--dry-run` | off | print candidate authors (one JSON object per line) without lock/fetch/uploads |
| `--skip-merge` | off | leave the batch as JSONL for the daily merge cron |
| `--skip-auth` | off | unauthenticated requests (dev only) |
| `--api-base` | `https://oauth.reddit.com` | Reddit API base (same requirement as main.py) |
| `--qpm-budget` | `90` | max requests per 60 s (under Reddit's 100 QPM) |
| `--lock-ttl-ms` | `3600000` | S3Lock TTL (crash-safety window) |

**Environment**: everything `backfill.py` needs — `S3_ENDPOINT_URL`,
`S3_ACCESS_KEY`, and `S3_SECRET_KEY` are **required** even in `--dry-run`
(the DuckDB scans read parquet over httpfs) — plus `WSS_MUTEX_BASE_URL`
(unless `--dry-run`). Reddit credentials are only required when there is
something to fetch.

**Deployment** (see `deploy/authors.service` for a ready systemd unit): run a
one-time catch-up pass first, then enable the service:

```bash
uv run python authors.py --once --max-fetch 10000   # one-time catch-up
cp deploy/authors.service ~/.config/systemd/user/    # set User= + paths
systemctl --user daemon-reload && systemctl --user enable --now authors
```

The worker is idempotent and incremental: restarts resume from the S3 index,
and new firehose batches are picked up automatically once merged.

## Development

```bash
uv run pytest                       # offline tests (mock transport + example_data)
uv run python tests/mock_server.py  # serve example_data on :9000 for CLI smoke tests
uv run python main.py --subreddits Quebec --api-base http://127.0.0.1:9000 \
    --skip-auth --dry-run --max-cycles 1 --state-file /tmp/smoke_state.json
```

## Known limitations

- Posts/comments created **before** collection started are not captured by the
  firehose itself; run [`backfill.py`](#post--parent-comment-backfill) daily
  to fetch records referenced by collected comments.
- If a subreddit emits more than `limit × max_pages` items per cycle, the
  overflow is a gap (documented sampling behavior); the cursor still advances
  so no duplicates are produced.
- Items that appear late with a `created_utc` at or below the cursor (e.g.
  delayed moderation approvals) are dropped.
- `[removed]`/`[deleted]` bodies are stored as-is.
