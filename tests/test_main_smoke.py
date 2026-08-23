"""End-to-end smoke test: mock API -> firehose -> dry-run stdout."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import httpx
import pytest

from reddit_firehose_tool.main import load_dotenv, main, parse_args
from tests.conftest import ScriptedAPI, make_comment, make_post


@pytest.fixture(autouse=True)
def no_real_dotenv(monkeypatch):
    """Never pick up the developer's real .env during tests."""
    monkeypatch.setattr("reddit_firehose_tool.main.load_dotenv", lambda path=None: None)


def test_load_dotenv_plain_key_value(tmp_path, monkeypatch):
    monkeypatch.delenv("MY_TEST_VAR", raising=False)
    monkeypatch.delenv("MY_KEEP_VAR", raising=False)
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "# a comment\n"
        "MY_TEST_VAR=linux:test:v0.1.0 (by /u/someone)  # inline comments are kept verbatim\n"
        "MY_KEEP_VAR=from-file\n"
    )
    monkeypatch.setenv("MY_KEEP_VAR", "from-environment")
    load_dotenv(dotenv)
    assert os.environ["MY_TEST_VAR"] == (
        "linux:test:v0.1.0 (by /u/someone)  # inline comments are kept verbatim"
    )
    assert os.environ["MY_KEEP_VAR"] == "from-environment"  # existing env wins


def test_default_api_base_is_oauth_reddit(monkeypatch):
    monkeypatch.delenv("REDDIT_API_BASE", raising=False)
    args = parse_args(["--subreddits", "Quebec", "--dry-run"])
    assert args.api_base == "https://oauth.reddit.com"


def test_main_dry_run_streams_posts_and_comments(tmp_path, capsys, monkeypatch):
    scripted = ScriptedAPI()
    scripted.add_page("Quebec", "new", [make_post("t3_p1", 100.0)])
    scripted.add_page("Quebec", "comments", [make_comment("t1_c1", 100.0)])

    # main() builds its own httpx.AsyncClient; patch the class so it uses our
    # mock transport instead of the network (capture the real class first to
    # avoid recursing into the fake).
    real_async_client = httpx.AsyncClient

    def fake_async_client(*args, **kwargs):
        return real_async_client(transport=httpx.MockTransport(scripted.handler))

    monkeypatch.setattr(httpx, "AsyncClient", fake_async_client)

    state_file = tmp_path / "state.json"
    asyncio.run(
        main(
            [
                "--subreddits", "Quebec",
                "--interval", "0",
                "--state-file", str(state_file),
                "--dry-run",
                "--skip-auth",
                "--max-cycles", "1",
            ]
        )
    )

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    records = [json.loads(line) for line in lines]
    posts = [r for r in records if "title" in r]
    comments = [r for r in records if "body" in r]
    assert [r["name"] for r in posts] == ["t3_p1"]
    assert [r["name"] for r in comments] == ["t1_c1"]
    # homogeneous schemas: no cross-contamination between the two streams
    assert all("body" not in r for r in posts)
    assert all("title" not in r for r in comments)

    # the API was polled once per endpoint (one cycle)
    paths = sorted(r.url.path for r in scripted.requests)
    assert paths == ["/r/Quebec/comments", "/r/Quebec/new"]

    # state was written with both cursors
    assert state_file.exists()
    assert os.stat(state_file).st_mode & 0o777 == 0o600
    from reddit_firehose_tool.state import FirehoseState

    loaded = FirehoseState.load(state_file)
    assert loaded.cursor("Quebec/new").last_seen_utc == 100.0
    assert loaded.cursor("Quebec/comments").last_seen_utc == 100.0


def test_main_requires_subreddits_and_env(capsys, monkeypatch):
    for var in ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_REFRESH_TOKEN", "S3_BUCKET"):
        monkeypatch.delenv(var, raising=False)

    with pytest.raises(SystemExit) as exc:
        asyncio.run(main(["--dry-run", "--skip-auth"]))
    assert exc.value.code == 2  # argparse error: --subreddits required

    # sys.exit(message) only prints via the unhandled-exception path, so the
    # message is asserted from the SystemExit code itself.
    with pytest.raises(SystemExit) as exc:
        asyncio.run(main(["--subreddits", "Quebec"]))  # no REDDIT_CLIENT_ID
    assert "REDDIT_CLIENT_ID" in exc.value.code

    with pytest.raises(SystemExit) as exc:
        asyncio.run(main(["--subreddits", "Quebec", "--skip-auth"]))  # no S3_BUCKET
    assert "S3_BUCKET" in exc.value.code
