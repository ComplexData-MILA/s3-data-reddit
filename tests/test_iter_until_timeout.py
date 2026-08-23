"""Tests for iter_until_timeout."""

from __future__ import annotations

import asyncio

from reddit_firehose_tool.stream import iter_until_timeout


async def slow_source(items, delay):
    for item in items:
        await asyncio.sleep(delay)
        yield item


def test_yields_items_until_deadline():
    async def scenario():
        out = []
        async for item in iter_until_timeout(slow_source([1, 2, 3], 0.01), timeout=0.05):
            out.append(item)
        return out

    out = asyncio.run(scenario())
    assert out and out == sorted(out) and out[-1] <= 3


def test_stops_when_source_exhausts():
    async def scenario():
        out = []
        async for item in iter_until_timeout(slow_source([1], 0.0), timeout=5.0):
            out.append(item)
        return out

    assert asyncio.run(scenario()) == [1]
