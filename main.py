"""Reddit firehose: poll subreddits and stream posts/comments into the S3 data lake."""

import asyncio

from reddit_firehose_tool.main import main

if __name__ == "__main__":
    asyncio.run(main())
