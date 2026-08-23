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

## Development

```bash
uv run pytest                       # offline tests (mock transport + example_data)
uv run python tests/mock_server.py  # serve example_data on :9000 for CLI smoke tests
uv run python main.py --subreddits Quebec --api-base http://127.0.0.1:9000 \
    --skip-auth --dry-run --max-cycles 1 --state-file /tmp/smoke_state.json
```

## Known limitations

- If a subreddit emits more than `limit × max_pages` items per cycle, the
  overflow is a gap (documented sampling behavior); the cursor still advances
  so no duplicates are produced.
- Items that appear late with a `created_utc` at or below the cursor (e.g.
  delayed moderation approvals) are dropped.
- `[removed]`/`[deleted]` bodies are stored as-is.
