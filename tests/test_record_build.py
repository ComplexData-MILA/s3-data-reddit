"""Tests for build_record against the real example_data payloads."""

from __future__ import annotations

from reddit_firehose_tool.stream import build_record

FETCHED_AT = "2026-08-23T17:07:33Z"


def test_post_record_from_example_data(example_posts):
    child = example_posts["data"]["children"][0]
    assert child["kind"] == "t3"
    data = child["data"]
    record = build_record(child, "new", FETCHED_AT)

    assert record["id"] == data["name"]
    assert record["name"] == data["name"]
    assert record["subreddit"] == data["subreddit"]
    assert record["created_utc"] == data["created_utc"]
    assert record["author"] == data["author"]
    assert record["permalink"] == data["permalink"]
    assert record["score"] == data["score"]
    assert record["title"] == data["title"]
    assert record["selftext"] == data["selftext"]
    assert record["url"] == data["url"]
    assert record["num_comments"] == data["num_comments"]
    assert record["upvote_ratio"] == data["upvote_ratio"]
    assert record["fetched_at"] == FETCHED_AT
    assert record["raw"] == data
    # posts carry no comment columns at all
    assert "body" not in record
    assert "link_id" not in record


def test_comment_record_from_example_data(example_comments):
    child = example_comments["data"]["children"][0]
    assert child["kind"] == "t1"
    data = child["data"]
    record = build_record(child, "comments", FETCHED_AT)

    assert record["id"] == data["name"]
    assert record["name"] == data["name"]
    assert record["subreddit"] == data["subreddit"]
    assert record["created_utc"] == data["created_utc"]
    assert record["author"] == data["author"]
    assert record["permalink"] == data["permalink"]
    assert record["score"] == data["score"]
    assert record["body"] == data["body"]
    assert record["link_id"] == data["link_id"]
    assert record["parent_id"] == data["parent_id"]
    assert record["fetched_at"] == FETCHED_AT
    assert record["raw"] == data
    # comments carry no post columns at all
    assert "title" not in record
    assert "selftext" not in record


def test_join_key_link_id_matches_post_names(example_comments, example_posts):
    """The eventual join comments.link_id == posts.name is real in the data."""
    post_names = {c["data"]["name"] for c in example_posts["data"]["children"]}
    comment_links = {c["data"]["link_id"] for c in example_comments["data"]["children"]}
    assert comment_links.intersection(post_names), "comments reference posts in the same data"
