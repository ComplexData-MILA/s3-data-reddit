"""Tests for the backfill daemon (pure helpers + /api/info closure)."""

from __future__ import annotations

import asyncio
import datetime

import httpx
import pytest

from reddit_firehose_tool.backfill import (
    batch_name_for,
    backfill_batch_date,
    build_refs_query,
    closure_fetch,
    filter_parquet_paths,
    is_jsonl_chunk_key,
    marker_key,
    new_refs,
    next_run_time,
    plan_index_refresh,
    select_recent_batches,
    unquote_fullname,
    valid_fullname,
)
from reddit_firehose_tool.client import RedditClient
from tests.conftest import FakeSleeper, make_child, make_comment, make_listing, make_post

FETCHED_AT = "2026-08-24T00:00:00Z"


def run(coro):
    return asyncio.run(coro)


async def fake_token_provider(force: bool = False) -> str:
    return "tok"


def make_info_client(responses, **kwargs):
    """RedditClient whose /api/info responses are scripted child lists (or Exceptions).

    Note that ``RedditClient._request`` retries transport errors internally, so
    a scripted Exception is consumed per retry attempt (max_retries + 1).
    """

    def handler(request):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return httpx.Response(200, json=make_listing(item))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return RedditClient(http, fake_token_provider, "test-agent", **kwargs)


# ---------------------------------------------------------------- pure helpers


def test_unquote_fullname():
    assert unquote_fullname('"t3_abc"') == "t3_abc"
    assert unquote_fullname("t3_abc") == "t3_abc"
    assert unquote_fullname(None) is None
    assert unquote_fullname(42) is None
    assert unquote_fullname('"') == '"'  # too short to be the quoted form
    assert unquote_fullname('"t3_abc') == '"t3_abc'  # no closing quote: passthrough


def test_valid_fullname():
    assert valid_fullname("t3_abc", "t3_")
    assert valid_fullname("t1_abc", "t1_")
    assert not valid_fullname("t3_", "t3_")  # empty id
    assert not valid_fullname("t1abc", "t1_")  # missing underscore
    assert not valid_fullname('"t1_x"', "t1_")  # quoted input never counts
    assert not valid_fullname(None, "t3_")


def test_batch_name_for():
    assert batch_name_for(datetime.datetime(2026, 8, 24)) == "reddit-firehose-backfill-20260824"
    assert batch_name_for(datetime.datetime(2026, 8, 24), "test") == (
        "reddit-firehose-backfill-20260824-test"
    )


def test_backfill_batch_date():
    assert backfill_batch_date("reddit-firehose-backfill-20260824") == "20260824"
    assert backfill_batch_date("reddit-firehose-backfill-20260824-test") == "20260824"
    assert backfill_batch_date("reddit-firehose-20260824-12") is None
    assert backfill_batch_date("reddit-firehose-backfill-junk") is None


def test_marker_key():
    assert marker_key("datasets", datetime.datetime(2026, 8, 24)) == (
        "datasets/_backfill/reddit-firehose-backfill-2026-08-24.done"
    )


def test_select_recent_batches_window():
    batches = [
        "reddit-firehose-20260822-13",
        "reddit-firehose-20260823-10",
        "reddit-firehose-20260823-12",
        "reddit-firehose-20260824-01",
        "reddit-firehose-backfill-20260822",
        "reddit-firehose-backfill-20260823",
        "reddit-firehose-backfill-20260824",
        "junk-batch",
        "_backfill",
    ]
    selected = select_recent_batches(batches, "20260823-11")
    assert selected == [
        "reddit-firehose-20260823-12",
        "reddit-firehose-20260824-01",
        "reddit-firehose-backfill-20260823",
        "reddit-firehose-backfill-20260824",
    ]


def test_select_recent_batches_full():
    batches = [
        "reddit-firehose-20260822-13",
        "reddit-firehose-backfill-20260822",
        "junk-batch",
    ]
    assert select_recent_batches(batches, None) == [
        "reddit-firehose-20260822-13",
        "reddit-firehose-backfill-20260822",
    ]


def test_filter_parquet_paths():
    paths = [
        "s3://b/datasets/reddit-comments/reddit-firehose-20260824-01/merged.parquet",
        "s3://b/datasets/reddit-comments/reddit-firehose-20260823-01/merged.parquet",
        "s3://b/datasets/reddit-comments/reddit-firehose-backfill-20260824/merged.parquet",
    ]
    selected = filter_parquet_paths(
        paths, ["reddit-firehose-20260824-01", "reddit-firehose-backfill-20260824"]
    )
    assert selected == [paths[0], paths[2]]


def test_is_jsonl_chunk_key():
    assert is_jsonl_chunk_key("p/x/batch/ab12cd_chunk_00000.jsonl")
    assert not is_jsonl_chunk_key("p/x/batch/ab12cd.manifest.json")
    assert not is_jsonl_chunk_key("p/x/batch/merged.parquet")


def test_build_refs_query():
    posts = build_refs_query(
        "link_id", '"t3_', ["s3://b/datasets/reddit-comments/b1/merged.parquet"]
    )
    assert "DISTINCT link_id" in posts
    assert "starts_with(link_id, '\"t3_')" in posts
    assert "read_parquet([" in posts

    comments = build_refs_query("parent_id", '"t1_', ["s3://b/x/merged.parquet"])
    assert "DISTINCT parent_id" in comments
    assert "starts_with(parent_id, '\"t1_')" in comments

    assert build_refs_query("link_id", '"t3_', []) is None
    # single quotes in paths are doubled
    weird = build_refs_query("link_id", '"t3_', ["s3://b/weird'path/merged.parquet"])
    assert "weird''path" in weird


def test_new_refs():
    known_posts, known_comments = {"t3_known"}, {"t1_known"}
    refs = new_refs(
        {"link_id": "t3_new", "parent_id": "t1_new"}, known_posts, known_comments
    )
    assert refs == ["t3_new", "t1_new"]
    assert "t3_new" in known_posts and "t1_new" in known_comments

    # top-level comment: parent_id is the post (already covered by link_id)
    assert new_refs({"link_id": "t3_a", "parent_id": "t3_a"}, set(), set()) == ["t3_a"]
    # already-known and invalid values are skipped
    assert new_refs({"link_id": "t3_known", "parent_id": "t1_known"}, known_posts, known_comments) == []
    assert new_refs({"link_id": None, "parent_id": "t5_x"}, set(), set()) == []


def test_plan_index_refresh():
    manifest = {"batches": {"b1": {"last_modified": "2026-08-23T10:00:00+00:00"}}}
    mtimes = {
        "b1": "2026-08-23T10:00:00+00:00",  # unchanged: skip
        "b2": "2026-08-24T01:00:00+00:00",  # new: scan
    }
    assert plan_index_refresh(manifest, mtimes) == ["b2"]
    # changed LastModified (re-merge): rescan
    assert plan_index_refresh(manifest, {"b1": "2026-08-24T02:00:00+00:00"}) == ["b1"]
    # rebuild: everything
    assert plan_index_refresh(manifest, mtimes, rebuild=True) == ["b1", "b2"]
    # empty manifest: everything
    assert plan_index_refresh(None, mtimes) == ["b1", "b2"]


def test_next_run_time():
    now = datetime.datetime(2026, 8, 24, 2, 0)
    assert next_run_time(now, datetime.time(3, 0), 0) == datetime.datetime(2026, 8, 24, 3, 0)
    # exactly at the run time: next day
    assert next_run_time(datetime.datetime(2026, 8, 24, 3, 0), datetime.time(3, 0), 0) == (
        datetime.datetime(2026, 8, 25, 3, 0)
    )
    # fixed jitter is added
    assert next_run_time(now, datetime.time(3, 0), 30) == datetime.datetime(2026, 8, 24, 3, 0, 30)


def test_parse_run_at():
    from reddit_firehose_tool.backfill import parse_args

    args = parse_args(["--once"])
    assert args.run_at == datetime.time(3, 0)
    assert args.window_hours == 48.0
    assert args.max_fetch == 10_000
    assert args.dataset_prefix == "reddit"

    assert parse_args(["--run-at", "14:30"]).run_at == datetime.time(14, 30)
    with pytest.raises(SystemExit):
        parse_args(["--run-at", "25:00"])
    with pytest.raises(SystemExit):
        parse_args(["--run-at", "nope"])
    with pytest.raises(SystemExit):
        parse_args(["--window-hours", "0"])
    with pytest.raises(SystemExit):
        parse_args(["--max-fetch", "0"])


# ---------------------------------------------------------------- closure


def test_closure_fetch_routes_kinds():
    client = make_info_client(
        [
            [
                make_post("t3_post1", 100.0),
                make_child(
                    "t1", "t1_c1", 100.0, body="body t1_c1",
                    link_id="t3_post1", parent_id="t3_post1",
                ),
            ]
        ]
    )
    result = run(
        closure_fetch(
            client, {"t3_post1"}, {"t1_c1"}, set(), set(), FETCHED_AT, max_fetch=100
        )
    )
    assert result.fetched == 2
    assert len(result.posts) == 1 and result.posts[0]["title"] == "title t3_post1"
    assert len(result.comments) == 1 and result.comments[0]["body"] == "body t1_c1"
    assert result.comments[0]["link_id"] == "t3_post1"
    assert result.missing == [] and result.remaining == []


def test_closure_fetch_chunks_101_names_into_two_requests():
    seen = []

    def handler(request):
        seen.append(request.url.params["id"])
        ids = request.url.params["id"].split(",")
        return httpx.Response(200, json=make_listing([make_post(n, 1.0) for n in ids]))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = RedditClient(http, fake_token_provider, "test-agent")
    candidates = {f"t3_p{i:03d}" for i in range(101)}
    result = run(closure_fetch(client, candidates, set(), set(), set(), FETCHED_AT, max_fetch=200))

    assert len(seen) == 2
    assert len(seen[0].split(",")) == 100
    assert len(seen[1].split(",")) == 1
    assert result.fetched == 101
    assert result.missing == [] and result.remaining == []


def test_closure_fetch_revealed_refs_are_fetched():
    # the first response's comment reveals t3_new + t1_other; the second
    # request returns both together (a third request would exhaust the
    # scripted responses and fail the test, proving no duplicate enqueue).
    client = make_info_client(
        [
            [
                make_child(
                    "t1", "t1_a", 1.0, body="body t1_a",
                    link_id="t3_new", parent_id="t1_other",
                )
            ],
            [
                make_post("t3_new", 1.0),
                make_child(
                    "t1", "t1_other", 1.0, body="body t1_other",
                    link_id="t3_new", parent_id="t1_a",
                ),
            ],
        ]
    )
    result = run(closure_fetch(client, set(), {"t1_a"}, set(), set(), FETCHED_AT, max_fetch=100))
    assert result.fetched == 3
    assert {p["name"] for p in result.posts} == {"t3_new"}
    assert {c["name"] for c in result.comments} == {"t1_a", "t1_other"}
    assert result.missing == [] and result.remaining == []


def test_closure_fetch_budget_stops():
    # serve exactly the requested names so chunking (2 names in the first
    # request) hits the budget and leaves t3_c queued
    def handler(request):
        ids = request.url.params["id"].split(",")
        return httpx.Response(200, json=make_listing([make_post(n, 1.0) for n in ids]))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = RedditClient(http, fake_token_provider, "test-agent")
    result = run(
        closure_fetch(
            client, {"t3_a", "t3_b", "t3_c"}, set(), set(), set(), FETCHED_AT, max_fetch=2
        )
    )
    assert result.fetched == 2
    assert result.remaining == ["t3_c"]


def test_closure_fetch_omitted_names_reported_missing():
    # t3_gone is requested but omitted from the response (deleted/removed)
    client = make_info_client([[make_post("t3_a", 1.0)]])
    result = run(
        closure_fetch(
            client, {"t3_a", "t3_gone"}, set(), set(), set(), FETCHED_AT, max_fetch=100
        )
    )
    assert result.fetched == 1
    assert result.missing == ["t3_gone"]
    assert result.remaining == []


def test_closure_fetch_chunk_error_continues():
    # the first chunk fails (after _request's retries are exhausted); the next
    # chunk is still fetched (the failed names are left for the next day's
    # overlapping window)
    client = make_info_client(
        [httpx.ConnectError("boom")] * 4 + [[make_comment("t1_b", 1.0)]],
        sleep=FakeSleeper(),
    )
    result = run(
        closure_fetch(
            client, set(), {"t1_a", "t1_b"}, set(), set(), FETCHED_AT, max_fetch=1
        )
    )
    assert result.failed_chunks == 1
    assert result.fetched == 1
    assert [c["name"] for c in result.comments] == ["t1_b"]


def test_closure_fetch_unknown_kind_skipped():
    client = make_info_client(
        [[{"kind": "t5", "data": {"name": "t5_x"}}, make_post("t3_a", 1.0)]]
    )
    result = run(closure_fetch(client, {"t3_a"}, set(), set(), set(), FETCHED_AT, max_fetch=100))
    assert result.fetched == 1
    assert [p["name"] for p in result.posts] == ["t3_a"]
