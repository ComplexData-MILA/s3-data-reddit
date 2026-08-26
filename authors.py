"""Author profile enrichment: build the reddit-authors entity dataset."""

import asyncio

from reddit_firehose_tool.authors import main

if __name__ == "__main__":
    asyncio.run(main())
