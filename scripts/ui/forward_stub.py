"""A tiny stand-in for the Forward API, for the browser UAT only.

It answers just the calls the UI makes (network list, snapshots, NQE runs for the
tag and location pickers) from generated fixtures: no customer data, no network.
Unknown requests get a 404 that names the path, so a missing route fails loudly.

Run inside the NetBox container: `python forward_stub.py 9999 <network id>`.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

NETWORK_ID = sys.argv[2] if len(sys.argv) > 2 else "uat-network"
SNAPSHOT_ID = "uat-snapshot-1"
TAGS = [f"Tag-{number:03d}" for number in range(1, 301)]
LOCATIONS = [f"Location-{number:03d}" for number in range(1, 401)] + [
    "DC01-East",
    "DC01-West",
    "DC02-Main",
    "Campus-North",
]


def nqe_rows(query):
    if "tagNames" in query:
        return [{"tagNames": TAGS[i : i + 5]} for i in range(0, len(TAGS), 5)]
    if "network.locations" in query:
        return [{"name": name} for name in LOCATIONS]
    return []


EXECUTIONS = {}


def snapshot():
    return {
        "id": SNAPSHOT_ID,
        "state": "PROCESSED",
        "createdAt": "2026-01-01T00:00:00Z",
        "processedAt": "2026-01-01T00:10:00Z",
    }


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self):
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(length).decode() if length else ""
        sys.stderr.write(f"REQ {self.command} {self.path} {raw[:300]}\n")
        sys.stderr.flush()
        auth = self.headers.get("authorization", "")
        if not auth.startswith("Basic "):
            return self._send(401, {"message": "unauthorized"})
        path = self.path.split("?")[0]
        if path == "/api/networks":
            return self._send(
                200, [{"id": NETWORK_ID, "name": "UAT network", "orgId": "uat-org"}]
            )
        base = f"/api/networks/{NETWORK_ID}"
        if path == f"{base}/snapshots":
            return self._send(200, {"snapshots": [snapshot()]})
        if path == f"{base}/snapshots/latestProcessed":
            return self._send(200, snapshot())
        if path == f"{base}/nqe-executions" and self.command == "POST":
            body = json.loads(raw or "{}")
            key = f"exec-{len(EXECUTIONS) + 1}"
            EXECUTIONS[key] = body
            return self._send(200, {"executionKey": key, "status": "SUBMITTED"})
        prefix = f"{base}/nqe-executions/"
        if path.startswith(prefix):
            key, _, tail = path[len(prefix) :].partition("/")
            body = EXECUTIONS.get(key)
            if body is None:
                return self._send(404, {"message": f"unknown execution {key}"})
            rows = nqe_rows(body.get("query") or "")
            if tail == "result":
                from urllib.parse import parse_qs, urlparse

                query = parse_qs(urlparse(self.path).query)
                offset = int((query.get("offset") or ["0"])[0])
                limit = int((query.get("limit") or [str(len(rows) or 1)])[0])
                return self._send(
                    200,
                    {
                        "items": rows[offset : offset + limit],
                        "snapshotId": SNAPSHOT_ID,
                        "totalNumItems": len(rows),
                    },
                )
            return self._send(
                200,
                {
                    "status": "COMPLETED",
                    "outcome": "OK",
                    "rowsProduced": len(rows),
                    "resultKey": key,
                },
            )
        return self._send(404, {"message": f"stub has no route for {self.command} {path}"})

    do_GET = do_POST = do_PUT = do_DELETE = _handle

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
