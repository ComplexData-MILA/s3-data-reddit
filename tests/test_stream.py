"""Tests for RedditFirehose polling, cursor, and dedupe semantics."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from reddit_firehose_tool.client import RedditClient
from reddit_firehose_tool.state import FirehoseState, StreamCursor
from reddit_firehose_tool.stream import RedditFirehose
from tests.conftest import (
    FakeClock,
    FakeSleeper,
    ScriptedAPI,
    make_comment,
    make_post,
)


def make_firehose(state, scripted, *, endpoints=("new",), subreddits=("Quebec",), **kwargs):
    http = httpx.AsyncClient(transport=httpx.MockTransport(scripted.handler))
    client = RedditClient(http, None, "test-agent")
    defaults = dict(interval=0.0, sleep=FakeSleeper(), clock=FakeClock())
    defaults.update(kwargs)
    return RedditFirehose(
        client, state, subreddits, endpoints=endpoints, **defaults
    )


async def collect(firehose):
    records = []
    async for record in firehose:
        records.append(record)
    return records


def run(coro):
    return asyncio.run(coro)


def test_first_run_backfills_one_page_and_sets_cursor(tmp_path):
    scripted = ScriptedAPI()
    scripted.add_page(
        "Quebec", "new", [make_post("t3_b", 200.0), make_post("t3_a", 100.0)], after="more"
    )
    state = FirehoseState(tmp_path / "state.json")
    firehose = make_firehose(state, scripted, max_cycles=1)

    records = run(collect(firehose))
    assert [r["name"] for r in records] == ["t3_b", "t3_a"]
    cur = FirehoseState.load(state.path).cursor("Quebec/new")
    assert cur.last_seen_utc == 200.0
    assert cur.names() == {"t3_b"}  # only boundary-second names are kept


def test_no_backfill_sets_cursor_without_emitting(tmp_path):
    scripted = ScriptedAPI()
    scripted.add_page("Quebec", "new", [make_post("t3_b", 200.0)])
    state = FirehoseState(tmp_path / "state.json")
    firehose = make_firehose(state, scripted, backfill=False, max_cycles=1)

    records = run(collect(firehose))
    assert records == []
    assert FirehoseState.load(state.path).cursor("Quebec/new").last_seen_utc == 200.0


def test_boundary_tie_emits_unseen_and_skips_seen(tmp_path):
    scripted = ScriptedAPI()
    scripted.add_page(
        "Quebec", "new",
        [make_post("t3_new_at_tie", 100.0), make_post("t3_old", 99.0)],
    )
    state = FirehoseState(tmp_path / "state.json")
    state.set_cursor("Quebec/new", StreamCursor(100.0, [("t3_seen_at_tie", 100.0)]))
    firehose = make_firehose(state, scripted, max_cycles=1)

    records = run(collect(firehose))
    # the unseen tie is emitted; the older item stops the stream
    assert [r["name"] for r in records] == ["t3_new_at_tie"]
    cur = FirehoseState.load(state.path).cursor("Quebec/new")
    assert cur.last_seen_utc == 100.0
    assert cur.names() == {"t3_seen_at_tie", "t3_new_at_tie"}


def test_snapshot_rule_emits_page2_items_between_old_and_new_cursor(tmp_path):
    """Regression: the cursor must not advance mid-stream and truncate page 2."""
    scripted = ScriptedAPI()
    scripted.add_page(
        "Quebec", "new", [make_post("t3_p1a", 300.0), make_post("t3_p1b", 250.0)], after="p2"
    )
    scripted.add_page(
        "Quebec", "new", [make_post("t3_p2", 200.0)], after=None  # between old (100) and page-1 max
    )
    state = FirehoseState(tmp_path / "state.json")
    state.set_cursor("Quebec/new", StreamCursor(100.0))
    firehose = make_firehose(state, scripted, max_cycles=1)

    records = run(collect(firehose))
    assert [r["name"] for r in records] == ["t3_p1a", "t3_p1b", "t3_p2"]
    assert FirehoseState.load(state.path).cursor("Quebec/new").last_seen_utc == 300.0


def test_stop_on_older_item(tmp_path):
    scripted = ScriptedAPI()
    scripted.add_page("Quebec", "new", [make_post("t3_old", 99.0)])
    state = FirehoseState(tmp_path / "state.json")
    state.set_cursor("Quebec/new", StreamCursor(100.0))
    firehose = make_firehose(state, scripted, max_cycles=1)
    assert run(collect(firehose)) == []


def test_max_pages_caps_pagination(tmp_path):
    scripted = ScriptedAPI()
    for i in range(3):
        scripted.add_page(
            "Quebec", "new", [make_post(f"t3_p{i}", 1000.0 - i)], after=f"c{i}"
        )
    state = FirehoseState(tmp_path / "state.json")
    state.set_cursor("Quebec/new", StreamCursor(0.0))
    firehose = make_firehose(state, scripted, max_pages=2, max_cycles=1)

    records = run(collect(firehose))
    assert [r["name"] for r in records] == ["t3_p0", "t3_p1"]
    assert len(scripted.requests) == 2  # exactly max_pages pages fetched


def test_failed_stream_preserves_cursor_and_other_stream_proceeds(tmp_path):
    scripted = ScriptedAPI()
    scripted.failures[("Quebec", "new")] = httpx.ConnectError("boom")
    scripted.add_page("Montreal", "new", [make_post("t3_m", 500.0, subreddit="Montreal")])
    state = FirehoseState(tmp_path / "state.json")
    state.set_cursor("Quebec/new", StreamCursor(100.0))
    firehose = make_firehose(
        state, scripted, subreddits=("Quebec", "Montreal"), max_cycles=1
    )

    records = run(collect(firehose))
    assert [r["name"] for r in records] == ["t3_m"]
    loaded = FirehoseState.load(state.path)
    assert loaded.cursor("Quebec/new").last_seen_utc == 100.0  # untouched
    assert loaded.cursor("Montreal/new").last_seen_utc == 500.0


def test_endpoints_filter_only_polls_requested_endpoint(tmp_path):
    scripted = ScriptedAPI()
    scripted.add_page("Quebec", "new", [make_post("t3_a", 100.0)])
    state = FirehoseState(tmp_path / "state.json")
    firehose = make_firehose(state, scripted, endpoints=("comments",), max_cycles=1)

    run(collect(firehose))
    assert all("/comments" in r.url.path for r in scripted.requests)
    assert not any("/new" in r.url.path for r in scripted.requests)


def test_malformed_child_is_skipped(tmp_path):
    scripted = ScriptedAPI()
    scripted.add_page(
        "Quebec",
        "new",
        [
            {"kind": "t3", "data": {"created_utc": 200.0}},  # missing name
            make_post("t3_good", 200.0),
        ],
    )
    state = FirehoseState(tmp_path / "state.json")
    firehose = make_firehose(state, scripted, max_cycles=1)
    records = run(collect(firehose))
    assert [r["name"] for r in records] == ["t3_good"]


def test_restart_resumes_from_saved_cursor(tmp_path):
    state_path = tmp_path / "state.json"
    # cycle 1: backfill
    scripted = ScriptedAPI()
    scripted.add_page("Quebec", "comments", [make_comment("t1_a", 100.0)])
    state = FirehoseState(state_path)
    firehose = make_firehose(state, scripted, endpoints=("comments",), max_cycles=1)
    assert [r["name"] for r in run(collect(firehose))] == ["t1_a"]

    # "restart": fresh objects, same state file; only newer items are emitted
    scripted2 = ScriptedAPI()
    scripted2.add_page(
        "Quebec",
        "comments",
        [make_comment("t1_new", 150.0), make_comment("t1_a", 100.0)],  # t1_a already seen
    )
    state2 = FirehoseState.load(state_path)
    firehose2 = make_firehose(state2, scripted2, endpoints=("comments",), max_cycles=1)
    assert [r["name"] for r in run(collect(firehose2))] == ["t1_new"]


def test_max_cycles_terminates_iterator(tmp_path):
    scripted = ScriptedAPI()
    scripted.add_page("Quebec", "new", [make_post("t3_a", 100.0)])
    state = FirehoseState(tmp_path / "state.json")
    firehose = make_firehose(state, scripted, max_cycles=1)
    records = run(collect(firehose))
    assert len(records) == 1  # iterator stopped after one cycle, no hang


def test_unknown_endpoint_rejected(tmp_path):
    state = FirehoseState(tmp_path / "state.json")
    with pytest.raises(ValueError, match="endpoints"):
        make_firehose(state, ScriptedAPI(), endpoints=("hot",))
