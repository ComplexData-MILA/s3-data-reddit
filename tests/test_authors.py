"""Tests for the author enrichment worker's pure helpers (no live S3)."""

from __future__ import annotations

import datetime

import pytest

from reddit_firehose_tool.authors import (
    append_cache_line,
    authors_batch_name,
    build_author_record,
    build_authors_scan_query,
    cache_entry_for,
    cache_lookup,
    entry_is_fresh,
    fetchable_author,
    index_row_for,
    load_cache,
    parse_args,
    plan_fetches,
)

NOW = datetime.datetime(2026, 8, 25, 12, 0, 0)


def scan_row(name, author="alice", first=1.0, last=2.0):
    return {
        "name": name,
        "author": author,
        "first_seen_at": first,
        "last_seen_at": last,
    }


def index_row(name, status="ok", fetched_at="2026-08-24T00:00:00Z"):
    return {
        "name": name,
        "author": "alice",
        "status": status,
        "fetched_at": fetched_at,
        "first_seen_at": 1.0,
        "last_seen_at": 1.0,
    }


# ---------------------------------------------------------------- batch names


def test_authors_batch_name():
    date = datetime.datetime(2026, 8, 25)
    assert authors_batch_name(date) == "reddit-firehose-authors-20260825"
    assert authors_batch_name(date, "test") == "reddit-firehose-authors-20260825-test"


# ---------------------------------------------------------------- fetchability


def test_fetchable_author():
    assert fetchable_author("alice") is True
    assert fetchable_author("[deleted]") is False
    assert fetchable_author("[removed]") is False
    assert fetchable_author(None) is False
    assert fetchable_author(42) is False


# ---------------------------------------------------------------- record builder


def test_build_author_record_curated_columns():
    profile = {
        "name": "alice",
        "id": "abc",
        "created_utc": 1560000000.0,
        "link_karma": 10,
        "comment_karma": 20,
        "total_karma": 30,
        "awardee_karma": 1,
        "awarder_karma": 2,
        "is_mod": True,
        "is_employee": False,
        "is_gold": False,
        "verified": True,
        "has_verified_email": True,
        "icon_img": "https://img",
        "subreddit": {"display_name": "u_alice", "subscribers": 3},
    }
    record = build_author_record(
        profile, name="t2_x", author="alice", fetched_at="F", first_seen_at=1.0, last_seen_at=2.0
    )
    assert record["id"] == "t2_x"
    assert record["name"] == "t2_x"
    assert record["author"] == "alice"
    assert record["author_created_utc"] == 1560000000.0
    assert record["author_link_karma"] == 10
    assert record["author_comment_karma"] == 20
    assert record["author_is_mod"] is True
    assert record["author_verified"] is True
    assert record["author_subreddit"] == "u_alice"
    assert record["author_profile_raw"] is profile
    assert record["fetched_at"] == "F"
    assert record["first_seen_at"] == 1.0
    assert record["last_seen_at"] == 2.0


def test_build_author_record_defaults_missing_fields():
    record = build_author_record(
        {}, name="t2_x", author="alice", fetched_at="F", first_seen_at=None, last_seen_at=None
    )
    assert record["author_is_mod"] is False
    assert record["author_verified"] is False
    assert record["author_subreddit"] is None
    assert record["author_created_utc"] is None
    assert record["author_profile_raw"] == {}


# ---------------------------------------------------------------- staleness


def test_entry_is_fresh():
    fresh = {"status": "ok", "fetched_at": "2026-08-25T00:00:00Z"}
    stale = {"status": "ok", "fetched_at": "2026-08-01T00:00:00Z"}
    assert entry_is_fresh(fresh, 7.0, NOW) is True
    assert entry_is_fresh(stale, 7.0, NOW) is False
    assert entry_is_fresh(stale, 30.0, NOW) is True
    assert entry_is_fresh(stale, 0.0, NOW) is True  # never expires
    assert entry_is_fresh(None, 7.0, NOW) is False
    assert entry_is_fresh({"status": "not_found", "fetched_at": "F"}, 7.0, NOW) is False
    assert entry_is_fresh({"status": "ok"}, 7.0, NOW) is False
    assert entry_is_fresh({"status": "ok", "fetched_at": "garbage"}, 7.0, NOW) is False


def test_cache_lookup():
    assert cache_lookup(None, 7.0, NOW) is None
    assert cache_lookup({"status": "not_found"}, 7.0, NOW) == "tombstone"
    assert (
        cache_lookup({"status": "ok", "fetched_at": "2026-08-25T00:00:00Z"}, 7.0, NOW) == "ok"
    )
    assert cache_lookup({"status": "ok", "fetched_at": "2020-01-01T00:00:00Z"}, 7.0, NOW) is None


# ---------------------------------------------------------------- candidates


def test_plan_fetches_new_first_capped():
    index: dict = {}
    scanned = {
        "t2_a": scan_row("t2_a", "alice"),
        "t2_b": scan_row("t2_b", "bob"),
        "t2_c": scan_row("t2_c", "carol"),
    }
    candidates = plan_fetches(index, scanned, refetch_days=7.0, now=NOW, max_fetch=2)
    assert [c["name"] for c in candidates] == ["t2_a", "t2_b"]
    assert all(c["stale"] is False for c in candidates)


def test_plan_fetches_refetches_stale_only_when_present():
    old = "2026-08-01T00:00:00Z"
    fresh = "2026-08-24T00:00:00Z"
    index = {
        "t2_a": index_row("t2_a", fetched_at=old),
        "t2_b": index_row("t2_b", fetched_at=old),
        "t2_c": index_row("t2_c", fetched_at=fresh),
    }
    scanned = {
        "t2_a": scan_row("t2_a", "alice", last=9.0),
        # t2_b no longer present in the scan -> not refreshed
        "t2_c": scan_row("t2_c", "carol"),
    }
    candidates = plan_fetches(index, scanned, refetch_days=7.0, now=NOW, max_fetch=10)
    assert [c["name"] for c in candidates] == ["t2_a"]
    assert candidates[0]["stale"] is True
    assert candidates[0]["last_seen_at"] == 9.0


def test_plan_fetches_new_authors_precede_stale():
    index = {"t2_old": index_row("t2_old", fetched_at="2020-01-01T00:00:00Z")}
    scanned = {
        "t2_old": scan_row("t2_old", "oldie"),
        "t2_new": scan_row("t2_new", "newbie"),
    }
    candidates = plan_fetches(index, scanned, refetch_days=7.0, now=NOW, max_fetch=10)
    assert [c["name"] for c in candidates] == ["t2_new", "t2_old"]
    assert candidates[0]["stale"] is False
    assert candidates[1]["stale"] is True


def test_plan_fetches_tombstones_are_permanent():
    index = {"t2_a": index_row("t2_a", status="not_found", fetched_at="2020-01-01T00:00:00Z")}
    scanned = {"t2_a": scan_row("t2_a", "alice")}
    assert plan_fetches(index, scanned, refetch_days=7.0, now=NOW, max_fetch=10) == []


def test_plan_fetches_refetch_days_zero_disables_refresh():
    index = {"t2_a": index_row("t2_a", fetched_at="2020-01-01T00:00:00Z")}
    scanned = {"t2_a": scan_row("t2_a", "alice")}
    assert plan_fetches(index, scanned, refetch_days=0.0, now=NOW, max_fetch=10) == []


def test_plan_fetches_skips_unfetchable_authors():
    index: dict = {}
    scanned = {
        "t2_d": scan_row("t2_d", "[deleted]"),
        "t2_n": scan_row("t2_n", None),
        "t2_ok": scan_row("t2_ok", "alice"),
    }
    candidates = plan_fetches(index, scanned, refetch_days=7.0, now=NOW, max_fetch=10)
    assert [c["name"] for c in candidates] == ["t2_ok"]


def test_index_row_for():
    row = index_row_for(
        "t2_x", "alice", "ok", "F", {"first_seen_at": 1.0, "last_seen_at": 2.0}
    )
    assert row == {
        "name": "t2_x",
        "author": "alice",
        "status": "ok",
        "fetched_at": "F",
        "first_seen_at": 1.0,
        "last_seen_at": 2.0,
    }


# ---------------------------------------------------------------- scan query


def test_build_authors_scan_query_shape():
    query = build_authors_scan_query(
        {
            "reddit-posts": ["s3://mybucket/datasets/reddit-posts/b1/merged.parquet"],
            "reddit-comments": ["s3://mybucket/datasets/reddit-comments/b2/merged.parquet"],
        }
    )
    assert "json_extract_string(raw, '$.author_fullname')" in query
    assert "'s3://mybucket/datasets/reddit-posts/b1/merged.parquet'" in query
    assert "'s3://mybucket/datasets/reddit-comments/b2/merged.parquet'" in query
    assert "UNION ALL" in query
    assert "GROUP BY author, author_fullname" in query


def test_build_authors_scan_query_empty_returns_none():
    assert build_authors_scan_query({"reddit-posts": [], "reddit-comments": []}) is None


# ---------------------------------------------------------------- local cache


def test_cache_entry_for():
    entry = cache_entry_for("t2_x", "alice", "ok", {"name": "alice"}, "F")
    assert entry == {
        "name": "t2_x",
        "author": "alice",
        "status": "ok",
        "profile": {"name": "alice"},
        "fetched_at": "F",
    }


def test_cache_file_roundtrip(tmp_path):
    path = tmp_path / "cache.jsonl"
    append_cache_line(path, cache_entry_for("t2_x", "alice", "ok", {"name": "alice"}, "F"))
    append_cache_line(path, cache_entry_for("t2_y", "bob", "not_found", None, "G"))
    cache = load_cache(path)
    assert cache["t2_x"]["profile"] == {"name": "alice"}
    assert cache["t2_y"]["status"] == "not_found"
    assert cache["t2_y"]["profile"] is None


def test_cache_file_ignores_corrupt_lines(tmp_path):
    path = tmp_path / "cache.jsonl"
    path.write_text(
        '{"name": "t2_a", "status": "ok"}\nnot json\n{"name": "t2_b", "status": "ok"}\n',
        encoding="utf-8",
    )
    cache = load_cache(path)
    assert set(cache) == {"t2_a", "t2_b"}


def test_cache_file_missing_returns_empty(tmp_path):
    assert load_cache(tmp_path / "absent.jsonl") == {}


# ---------------------------------------------------------------- CLI


def test_parse_args_defaults():
    args = parse_args([])
    assert args.dataset_prefix == "reddit"
    assert args.poll_interval_seconds == 300.0
    assert args.max_fetch == 500
    assert args.refetch_days == 7.0
    assert args.cache_file == "author_profiles_cache.jsonl"
    assert args.once is False
    assert args.dry_run is False
    assert args.skip_merge is False
    assert args.skip_auth is False
    assert args.qpm_budget == 90
    assert args.lock_ttl_ms == 3_600_000
    assert args.api_base == "https://oauth.reddit.com"


def test_parse_args_rejects_bad_values():
    with pytest.raises(SystemExit):
        parse_args(["--max-fetch", "0"])
    with pytest.raises(SystemExit):
        parse_args(["--refetch-days", "-1"])
    with pytest.raises(SystemExit):
        parse_args(["--poll-interval-seconds", "-1"])
    with pytest.raises(SystemExit):
        parse_args(["--qpm-budget", "0"])
