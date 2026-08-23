"""Reddit firehose: poll subreddits for new posts and comments into the S3 data lake."""

from .client import RedditAPIError, RedditClient
from .state import FirehoseState, StreamCursor
from .stream import RedditFirehose, build_record, iter_until_timeout

__version__ = "0.1.0"

__all__ = [
    "RedditFirehose",
    "RedditClient",
    "RedditAPIError",
    "FirehoseState",
    "StreamCursor",
    "build_record",
    "iter_until_timeout",
    "__version__",
]
