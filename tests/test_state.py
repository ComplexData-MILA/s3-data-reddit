"""Tests for FirehoseState and StreamCursor."""

from __future__ import annotations

import os

from reddit_firehose_tool.state import FirehoseState, StreamCursor


def test_cursor_add_prune():
    cur = StreamCursor(last_seen_utc=100.0)
    cur.add("t1_a", 100.0)
    cur.add("t1_b", 99.0)
    cur.prune()
    assert cur.names() == {"t1_a"}
    assert cur.last_seen_utc == 100.0


def test_cursor_bounded():
    cur = StreamCursor(last_seen_utc=0.0, maxlen=3)
    for i in range(10):
        cur.add(f"t1_{i}", 1.0)
    assert len(cur.recent_names) == 3
    assert cur.recent_names[-1][0] == "t1_9"


def test_state_round_trip(tmp_path):
    path = tmp_path / "state.json"
    state = FirehoseState(path)
    state.set_cursor("Quebec/new", StreamCursor(123.0, [("t3_a", 123.0)]))
    state.refresh_token = "tok"
    state.save()

    loaded = FirehoseState.load(path)
    assert loaded.refresh_token == "tok"
    cur = loaded.cursor("Quebec/new")
    assert cur.last_seen_utc == 123.0
    assert cur.recent_names == [("t3_a", 123.0)]


def test_state_save_is_atomic_and_private(tmp_path):
    path = tmp_path / "state.json"
    state = FirehoseState(path)
    state.set_cursor("Quebec/new", StreamCursor(1.0))
    state.save()
    assert path.exists()
    assert os.stat(path).st_mode & 0o777 == 0o600
    assert not list(tmp_path.glob("*.tmp"))


def test_state_corrupt_file_falls_back(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{ not json !!!")
    state = FirehoseState.load(path)
    assert state.refresh_token is None
    assert state.cursors == {}
    assert (tmp_path / "state.json.corrupt").exists()


def test_state_load_prunes_stale_names_and_malformed_cursors(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(
        '{"version": 1, "oauth": {"refresh_token": "tok"}, "cursors": {'
        '  "Quebec/new": {"last_seen_utc": 100, "recent_names": [["t3_new", 100], ["t3_old", 50]]},'
        '  "Quebec/comments": {"last_seen_utc": "not-a-number", "recent_names": []}'
        "}}"
    )
    state = FirehoseState.load(path)
    assert state.refresh_token == "tok"
    assert state.cursor("Quebec/new").recent_names == [("t3_new", 100.0)]
    assert state.cursor("Quebec/comments") is None


def test_set_refresh_token_persists_immediately(tmp_path):
    path = tmp_path / "state.json"
    state = FirehoseState(path)
    state.set_refresh_token("rt")
    loaded = FirehoseState.load(path)
    assert loaded.refresh_token == "rt"
    state.set_refresh_token(None)
    assert FirehoseState.load(path).refresh_token is None
