"""Serve example_data pages over HTTP for CLI smoke tests (no Reddit needed).

Usage:
    uv run python tests/mock_server.py [port] &
    uv run python main.py --subreddits Quebec --api-base http://127.0.0.1:9000 \
        --skip-auth --dry-run --max-cycles 1 --state-file /tmp/smoke_state.json

Page 1 is served from example_data; any paginated request (`after=...`) gets an
empty listing, so the firehose stops after the first page.
"""

from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

EXAMPLES = Path(__file__).resolve().parent.parent / "example_data"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")
        if len(parts) == 3 and parts[0] == "r" and parts[2] in ("new", "comments"):
            if "after" in parse_qs(parsed.query):
                payload = {
                    "kind": "Listing",
                    "data": {"after": None, "dist": 0, "children": []},
                }
            else:
                path = EXAMPLES / f"{parts[2]}.json"
                if not path.exists():
                    self.send_error(404)
                    return
                payload = json.loads(path.read_text(encoding="utf-8"))
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, *args):  # keep smoke-test output clean
        pass


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"serving example_data on http://127.0.0.1:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
