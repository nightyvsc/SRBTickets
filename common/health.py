"""
Lightweight HTTP health-check server for K8s liveness probes.

Runs in a background thread exposing GET /healthz on the configured port.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal handler that returns 200 on /healthz, 404 otherwise."""

    # Will be overridden per-instance with a closure
    _check_fn: Callable[[], bool] = staticmethod(lambda: True)  # type: ignore[assignment]

    def do_GET(self) -> None:  # noqa: N802  — required name by BaseHTTPRequestHandler
        if self.path == "/healthz":
            if self._check_fn():
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(503)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"unhealthy")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Suppress default stderr logging to keep logs clean."""
        pass


class HealthServer:
    """Background HTTP server for /healthz endpoint."""

    def __init__(
        self, port: int = 8100, check_fn: Callable[[], bool] | None = None
    ) -> None:
        self._port = port
        self._check_fn = check_fn or (lambda: True)
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the health server in a daemon thread."""
        handler = type(
            "_BoundHealthHandler",
            (_HealthHandler,),
            {"_check_fn": staticmethod(self._check_fn)},
        )
        self._server = HTTPServer(("0.0.0.0", self._port), handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Shut down the health server."""
        if self._server:
            self._server.shutdown()
