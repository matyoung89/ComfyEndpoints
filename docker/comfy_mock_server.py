from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def _respond(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # noqa: N802
        if self.path != "/prompt":
            self._respond(404, {"error": "not_found"})
            return
        self._respond(200, {"prompt_id": "mock-prompt-id"})

    def do_GET(self):  # noqa: N802
        if self.path == "/healthz":
            self._respond(200, {"status": "ok"})
            return
        self._respond(404, {"error": "not_found"})


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8188), Handler)
    server.serve_forever()
