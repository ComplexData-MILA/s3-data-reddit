"""Daily backfill: fetch posts and parent comments referenced by collected comments."""

import asyncio

from reddit_firehose_tool.backfill import main

if __name__ == "__main__":
    asyncio.run(main())
