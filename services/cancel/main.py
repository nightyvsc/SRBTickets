"""
Cancel and Notification Service for SRB (Sistema de Reserva de Boletos).

This service processes asynchronous cancellation requests:
1. Receives CANCEL_REQUEST messages from GT via ZeroMQ PULL socket.
2. Forwards cancellation requests to Persistence via ZeroMQ REQ socket (DB_EXECUTE).
3. Awaits DB_RESPONSE from Persistence.
4. Publishes CANCEL_RESULT envelopes to subscribed clients via ZeroMQ PUB socket
   using client_id as the message topic filter.
5. Emits structured JSON logs with performance timing and exposes /healthz via HTTP.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
import types
from typing import Any
import uuid
import zmq

# Ensure component name is set before importing common modules
os.environ.setdefault("COMPONENT_NAME", "cancel")

from common.config import Config
from common.health import HealthServer
from common.logging_json import get_logger, log_request
from common.messages import Envelope, MsgType, make_envelope, parse_envelope


class CancelService:
    """Service handling asynchronous reservation cancellations and client notifications."""

    def __init__(self, config: type[Config] = Config) -> None:
        """Initialize the Cancel Service with configuration and state."""
        self._config = config
        self._config.COMPONENT_NAME = os.getenv("COMPONENT_NAME", "cancel")
        self._logger: logging.Logger = get_logger("cancel_service")

        self._running: bool = False
        self._context: zmq.Context | None = None
        self._pull_socket: zmq.Socket | None = None
        self._persist_socket: zmq.Socket | None = None
        self._pub_socket: zmq.Socket | None = None
        self._health_server: HealthServer | None = None

        self._persist_timeout_ms: int = int(
            os.getenv("CANCEL_PERSIST_TIMEOUT_MS", str(self._config.CLIENT_REQUEST_TIMEOUT_MS))
        )

    def is_healthy(self) -> bool:
        """Check if the service is currently running and healthy."""
        return self._running and (self._context is not None and not self._context.closed)

    def _init_persist_socket(self) -> zmq.Socket:
        """Create and configure the REQ socket connecting to the Persistence service."""
        if self._persist_socket is not None:
            try:
                self._persist_socket.close(linger=0)
            except Exception as exc:
                self._logger.warning("Error closing previous persist socket: %s", exc)

        if self._context is None:
            raise RuntimeError("ZMQ context is not initialized")

        sock = self._context.socket(zmq.REQ)
        # Enable relaxed mode to allow retry/recovery without EFSM errors if a reply is dropped
        sock.setsockopt(zmq.REQ_RELAXED, 1)
        sock.setsockopt(zmq.REQ_CORRELATE, 1)
        sock.setsockopt(zmq.RCVTIMEO, self._persist_timeout_ms)
        sock.setsockopt(zmq.SNDTIMEO, self._persist_timeout_ms)

        persist_endpoint = f"tcp://{self._config.PERSIST_HOST}:{self._config.PERSIST_PORT}"
        self._logger.info("Connecting REQ socket to Persistence at %s", persist_endpoint)
        sock.connect(persist_endpoint)
        return sock

    def _init_sockets(self) -> None:
        """Initialize and bind/connect all ZeroMQ sockets."""
        self._context = zmq.Context()

        # 1. PULL socket: connects to GT_CANCEL_PUSH_HOST:GT_CANCEL_PUSH_PORT
        pull_endpoint = (
            f"tcp://{self._config.GT_CANCEL_PUSH_HOST}:{self._config.GT_CANCEL_PUSH_PORT}"
        )
        self._pull_socket = self._context.socket(zmq.PULL)
        self._logger.info("Connecting PULL socket to GT push endpoint at %s", pull_endpoint)
        self._pull_socket.connect(pull_endpoint)

        # 2. REQ socket: connects to PERSIST_HOST:PERSIST_PORT
        self._persist_socket = self._init_persist_socket()

        # 3. PUB socket: binds to NOTIFY_PORT
        pub_endpoint = f"tcp://*:{self._config.NOTIFY_PORT}"
        self._pub_socket = self._context.socket(zmq.PUB)
        self._logger.info("Binding PUB socket on %s", pub_endpoint)
        self._pub_socket.bind(pub_endpoint)

    def _forward_to_persistence(
        self,
        *,
        reservation_id: str,
        cancellation_id: str,
        client_id: str,
        request_id: str,
    ) -> tuple[str, str]:
        """
        Forward cancellation execution to the Persistence service via REQ socket.

        Returns:
            tuple[str, str]: (status, reason) where status is 'CANCELLED' or 'FAILED'.
        """
        db_payload: dict[str, Any] = {
            "operation": "CANCEL",
            "reservation_id": reservation_id,
            "cancellation_id": cancellation_id,
            "client_id": client_id,
        }
        db_req = make_envelope(
            msg_type=MsgType.DB_EXECUTE,
            payload=db_payload,
            client_id=client_id,
            request_id=request_id,
        )

        if self._persist_socket is None:
            self._persist_socket = self._init_persist_socket()

        try:
            self._persist_socket.send(db_req.to_bytes())
            raw_resp = self._persist_socket.recv()
            db_resp = parse_envelope(raw_resp)

            # Check response type and payload content
            if db_resp.msg_type == MsgType.ERROR:
                err_msg = str(
                    db_resp.payload.get("error")
                    or db_resp.payload.get("reason")
                    or "Database execution error"
                )
                return "FAILED", err_msg

            if db_resp.msg_type == MsgType.DB_RESPONSE:
                resp_status = str(db_resp.payload.get("status", "")).upper()
                is_success = db_resp.payload.get("success")

                if resp_status in ("CANCELLED", "SUCCESS", "OK") or is_success is True:
                    reason = str(
                        db_resp.payload.get("reason")
                        or "Reservation cancelled successfully"
                    )
                    return "CANCELLED", reason
                else:
                    reason = str(
                        db_resp.payload.get("reason")
                        or db_resp.payload.get("error")
                        or "Cancellation rejected by database"
                    )
                    return "FAILED", reason

            return "FAILED", f"Unexpected response message type: {db_resp.msg_type}"

        except zmq.error.Again:
            self._logger.error(
                "Persistence service timed out after %d ms for reservation %s",
                self._persist_timeout_ms,
                reservation_id,
            )
            # Recreate REQ socket on timeout to guarantee clean state
            self._persist_socket = self._init_persist_socket()
            return "FAILED", "Persistence service request timed out"

        except Exception as exc:
            self._logger.exception("Error communicating with Persistence service: %s", exc)
            self._persist_socket = self._init_persist_socket()
            return "FAILED", f"Persistence communication error: {exc}"

    def _publish_result(
        self,
        *,
        client_id: str,
        request_id: str,
        cancellation_id: str,
        reservation_id: str,
        status: str,
        reason: str,
    ) -> None:
        """
        Publish cancellation result to subscribed clients via PUB socket.

        First frame: client_id (topic)
        Second frame: CANCEL_RESULT envelope JSON bytes
        """
        if self._pub_socket is None:
            self._logger.error("Cannot publish result: PUB socket is not initialized")
            return

        result_envelope = make_envelope(
            msg_type=MsgType.CANCEL_RESULT,
            client_id=client_id,
            request_id=request_id,
            payload={
                "cancellation_id": cancellation_id,
                "reservation_id": reservation_id,
                "status": status,
                "reason": reason,
            },
        )

        topic_bytes = client_id.encode("utf-8")
        envelope_bytes = result_envelope.to_bytes()

        self._pub_socket.send_multipart([topic_bytes, envelope_bytes])
        self._logger.debug(
            "Published CANCEL_RESULT for client %s (reservation %s, status %s)",
            client_id,
            reservation_id,
            status,
        )

    def _process_cancel_request(self, raw_msg: bytes) -> None:
        """
        Process a single CANCEL_REQUEST received from the PULL socket.

        Steps:
        1. Parse message envelope.
        2. Forward to Persistence via REQ socket.
        3. Await Persistence DB_RESPONSE.
        4. Publish CANCEL_RESULT on PUB socket with topic = client_id.
        5. Log completion with execution timing.
        """
        t_received = time.perf_counter()
        request_id = ""
        client_id = ""
        cancellation_id = ""
        reservation_id = ""

        try:
            envelope = parse_envelope(raw_msg)
            request_id = envelope.request_id or str(uuid.uuid4())
            client_id = envelope.client_id or ""
            payload = envelope.payload or {}

            reservation_id = str(payload.get("reservation_id") or "")
            cancellation_id = str(payload.get("cancellation_id") or str(uuid.uuid4()))

            self._logger.info(
                "Received CANCEL_REQUEST for reservation %s (cancellation_id=%s, client_id=%s)",
                reservation_id,
                cancellation_id,
                client_id,
                extra={
                    "extra_fields": {
                        "request_id": request_id,
                        "client_id": client_id,
                        "reservation_id": reservation_id,
                        "cancellation_id": cancellation_id,
                        "op_type": "CANCEL_REQUEST_RECEIVED",
                    }
                },
            )

            if not reservation_id:
                status = "FAILED"
                reason = "Missing reservation_id in CANCEL_REQUEST payload"
                self._logger.warning("Rejecting invalid cancel request: %s", reason)
            else:
                # Forward to Persistence
                status, reason = self._forward_to_persistence(
                    reservation_id=reservation_id,
                    cancellation_id=cancellation_id,
                    client_id=client_id,
                    request_id=request_id,
                )

        except Exception as exc:
            self._logger.exception("Failed to process CANCEL_REQUEST: %s", exc)
            status = "FAILED"
            reason = f"Internal processing error: {exc}"
            if not request_id:
                request_id = str(uuid.uuid4())

        # Publish result to client (even if failed)
        try:
            self._publish_result(
                client_id=client_id,
                request_id=request_id,
                cancellation_id=cancellation_id,
                reservation_id=reservation_id,
                status=status,
                reason=reason,
            )
        except Exception as pub_exc:
            self._logger.exception("Failed to publish CANCEL_RESULT: %s", pub_exc)

        # Log completion with timing metrics
        t_completed = time.perf_counter()
        log_request(
            self._logger,
            request_id=request_id,
            client_id=client_id,
            op_type="CANCEL",
            t_received=t_received,
            t_completed=t_completed,
            status=status,
            extra={
                "cancellation_id": cancellation_id,
                "reservation_id": reservation_id,
                "reason": reason,
            },
        )

    def start_health_server(self) -> None:
        """Start the background HTTP health-check server."""
        self._health_server = HealthServer(
            port=self._config.HEALTH_PORT,
            check_fn=self.is_healthy,
        )
        self._health_server.start()
        self._logger.info(
            "Health server started on port %d (/healthz)", self._config.HEALTH_PORT
        )

    def run(self) -> None:
        """Start the service, initialize sockets, and run the main polling loop."""
        self._logger.info("Starting SRB Cancel Service...")
        self._running = True

        self.start_health_server()
        self._init_sockets()

        if self._pull_socket is None:
            raise RuntimeError("PULL socket was not properly initialized")

        poller = zmq.Poller()
        poller.register(self._pull_socket, zmq.POLLIN)

        self._logger.info("Cancel Service is ready and polling for requests.")

        try:
            while self._running:
                try:
                    socks = dict(poller.poll(timeout=500))
                except zmq.ZMQError as exc:
                    if exc.errno == zmq.EINTR:
                        continue
                    self._logger.error("Poller interrupted by ZMQ error: %s", exc)
                    break

                if self._pull_socket in socks and socks[self._pull_socket] == zmq.POLLIN:
                    try:
                        raw_msg = self._pull_socket.recv(flags=zmq.NOBLOCK)
                    except zmq.error.Again:
                        continue
                    except zmq.ZMQError as exc:
                        if exc.errno == zmq.EINTR:
                            continue
                        self._logger.error("Error receiving from PULL socket: %s", exc)
                        continue

                    self._process_cancel_request(raw_msg)

        except KeyboardInterrupt:
            self._logger.info("Keyboard interrupt received.")
        finally:
            self.cleanup()

    def stop(self) -> None:
        """Signal the service to stop running and exit its loop."""
        self._logger.info("Stopping Cancel Service...")
        self._running = False

    def cleanup(self) -> None:
        """Clean up sockets, health server, and ZeroMQ context."""
        self._logger.info("Cleaning up Cancel Service resources...")
        self._running = False

        if self._health_server is not None:
            try:
                self._health_server.stop()
            except Exception as exc:
                self._logger.warning("Error stopping health server: %s", exc)
            self._health_server = None

        sockets_to_close = [
            (self._pull_socket, "PULL"),
            (self._persist_socket, "REQ"),
            (self._pub_socket, "PUB"),
        ]
        for sock, name in sockets_to_close:
            if sock is not None:
                try:
                    sock.close(linger=0)
                except Exception as exc:
                    self._logger.warning("Error closing %s socket: %s", name, exc)

        self._pull_socket = None
        self._persist_socket = None
        self._pub_socket = None

        if self._context is not None and not self._context.closed:
            try:
                self._context.term()
            except Exception as exc:
                self._logger.warning("Error terminating ZMQ context: %s", exc)
            self._context = None

        self._logger.info("Cancel Service shutdown complete.")


def main() -> None:
    """Entrypoint for the Cancel Service."""
    service = CancelService()

    def _sig_handler(signum: int, frame: types.FrameType | None) -> None:
        service.stop()

    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT, _sig_handler)

    service.run()


if __name__ == "__main__":
    main()
