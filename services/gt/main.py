"""
Gestor de Transacciones (GT) Service for the SRB distributed ticket reservation system.

The GT acts as the central routing broker:
- Sync frontend: ROUTER socket on GT_SYNC_PORT (5555) for QUERY, RESERVE, MODIFY (REQ clients)
- Async frontend: ROUTER socket on GT_ASYNC_PORT (5556) for CANCEL (DEALER clients)
- Backend broker: ROUTER socket on GT_RMC_PORT (5570) for RMC workers (LRU queue pattern)
- Cancel backend: PUSH socket on GT_CANCEL_PUSH_PORT (5575) for forwarding cancellations to Cancel service

Features:
- ZGuide Chapter 3 LRU queue pattern for load balancing across RMC workers
- Worker heartbeat monitoring and dead-worker eviction
- Request queueing and timeout handling
- Automatic request retry when a worker dies mid-execution
- Structured JSON logging with timing measurements via time.perf_counter()
- Background HTTP /healthz health server on port 8100
- Clean shutdown on SIGINT/SIGTERM
"""

from __future__ import annotations

import os
import signal
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any

import zmq

# Ensure component name is set before loggers are initialized
if os.getenv("COMPONENT_NAME", "unknown") == "unknown":
    os.environ["COMPONENT_NAME"] = "gt"

from common.config import Config

Config.COMPONENT_NAME = "gt"

from common.health import HealthServer
from common.logging_json import get_logger, log_request
from common.messages import Envelope, MsgType, make_envelope, parse_envelope


@dataclass
class PendingRequest:
    """Tracks an in-flight sync request originating from a client."""

    request_id: str
    client_id: str
    client_routing: list[bytes]
    raw_request: bytes
    envelope: Envelope
    t_received: float
    socket_ref: zmq.Socket
    assigned_worker: bytes | None = None
    t_assigned: float | None = None
    retries: int = 0


class GTService:
    """Gestor de Transacciones (GT) routing broker."""

    MAX_RETRIES: int = 2

    def __init__(self) -> None:
        self.logger = get_logger("gt")
        self.context: zmq.Context | None = None

        # Sockets
        self.socket_sync: zmq.Socket | None = None
        self.socket_async: zmq.Socket | None = None
        self.socket_rmc: zmq.Socket | None = None
        self.socket_cancel_push: zmq.Socket | None = None
        self.poller: zmq.Poller | None = None

        # Health server
        self.health_server: HealthServer | None = None
        self._running: bool = False

        # LRU worker management
        # available_workers maintains worker identities in LRU order (oldest at left)
        self.available_workers: deque[bytes] = deque()
        self.idle_workers: set[bytes] = set()
        self.worker_last_seen: dict[bytes, float] = {}
        self.worker_to_request: dict[bytes, str] = {}  # worker_id -> request_id

        # Request tracking
        self.pending_requests: dict[str, PendingRequest] = {}  # request_id -> PendingRequest
        self.request_queue: deque[str] = deque()  # request_ids waiting for an idle worker

    def is_healthy(self) -> bool:
        """Liveness check for HealthServer."""
        return self._running and self.context is not None

    def start(self) -> None:
        """Initialize sockets, bind ports, and start health server."""
        if self._running:
            return

        self.logger.info("Initializing GT Service sockets...")
        self.context = zmq.Context()

        # 1. ROUTER on GT_SYNC_PORT (5555) — sync client requests (QUERY, RESERVE, MODIFY)
        self.socket_sync = self.context.socket(zmq.ROUTER)
        self.socket_sync.setsockopt(zmq.LINGER, 0)
        sync_addr = f"tcp://*:{Config.GT_SYNC_PORT}"
        self.socket_sync.bind(sync_addr)
        self.logger.info("Sync ROUTER socket bound on %s", sync_addr)

        # 2. ROUTER on GT_ASYNC_PORT (5556) — async cancel requests (CANCEL via DEALER)
        self.socket_async = self.context.socket(zmq.ROUTER)
        self.socket_async.setsockopt(zmq.LINGER, 0)
        async_addr = f"tcp://*:{Config.GT_ASYNC_PORT}"
        self.socket_async.bind(async_addr)
        self.logger.info("Async ROUTER socket bound on %s", async_addr)

        # 3. ROUTER on GT_RMC_PORT (5570) — backend facing RMC workers
        self.socket_rmc = self.context.socket(zmq.ROUTER)
        self.socket_rmc.setsockopt(zmq.LINGER, 0)
        rmc_addr = f"tcp://*:{Config.GT_RMC_PORT}"
        self.socket_rmc.bind(rmc_addr)
        self.logger.info("RMC ROUTER socket bound on %s", rmc_addr)

        # 4. PUSH on GT_CANCEL_PUSH_PORT (5575) — forward cancellations to Cancel service
        self.socket_cancel_push = self.context.socket(zmq.PUSH)
        self.socket_cancel_push.setsockopt(zmq.LINGER, 0)
        self.socket_cancel_push.setsockopt(zmq.SNDHWM, 10000)
        cancel_addr = f"tcp://*:{Config.GT_CANCEL_PUSH_PORT}"
        self.socket_cancel_push.bind(cancel_addr)
        self.logger.info("Cancel PUSH socket bound on %s", cancel_addr)

        # Register sockets in poller
        self.poller = zmq.Poller()
        self.poller.register(self.socket_sync, zmq.POLLIN)
        self.poller.register(self.socket_async, zmq.POLLIN)
        self.poller.register(self.socket_rmc, zmq.POLLIN)

        # Start health check server on port 8100
        self.health_server = HealthServer(
            port=Config.HEALTH_PORT, check_fn=self.is_healthy
        )
        self.health_server.start()
        self.logger.info("Health server started on port %d", Config.HEALTH_PORT)

        self._running = True

    def stop(self) -> None:
        """Gracefully shut down sockets, health server, and context."""
        if not self._running and self.context is None:
            return

        self._running = False
        self.logger.info("Shutting down GT Service...")

        # Stop HTTP health server
        if self.health_server:
            try:
                self.health_server.stop()
            except Exception as exc:
                self.logger.error("Error stopping health server: %s", exc)
            self.health_server = None

        # Notify any remaining pending requests
        for req in list(self.pending_requests.values()):
            try:
                self._send_client_error(req, "GT service is shutting down")
            except Exception:
                pass
        self.pending_requests.clear()
        self.request_queue.clear()
        self.available_workers.clear()
        self.idle_workers.clear()
        self.worker_last_seen.clear()
        self.worker_to_request.clear()

        # Close ZeroMQ sockets
        sockets = [
            (self.socket_sync, "sync"),
            (self.socket_async, "async"),
            (self.socket_rmc, "rmc"),
            (self.socket_cancel_push, "cancel_push"),
        ]
        for sock, name in sockets:
            if sock:
                try:
                    sock.close(linger=0)
                except Exception as exc:
                    self.logger.error("Error closing %s socket: %s", name, exc)

        self.socket_sync = None
        self.socket_async = None
        self.socket_rmc = None
        self.socket_cancel_push = None
        self.poller = None

        # Terminate ZeroMQ context
        if self.context:
            try:
                self.context.term()
            except Exception as exc:
                self.logger.error("Error terminating ZMQ context: %s", exc)
            self.context = None

        self.logger.info("GT Service stopped cleanly.")

    def run(self) -> None:
        """Main non-blocking event loop using zmq.Poller."""
        self.start()
        self.logger.info("GT Service is running and processing events.")

        heartbeat_interval_s = Config.HEARTBEAT_INTERVAL_MS / 1000.0
        timeout_check_interval_s = min(1.0, heartbeat_interval_s / 2.0)

        last_heartbeat_check = time.perf_counter()
        last_timeout_check = time.perf_counter()

        try:
            while self._running:
                if not self.poller:
                    break

                # Poll with timeout = HEARTBEAT_INTERVAL_MS
                try:
                    events = dict(self.poller.poll(timeout=Config.HEARTBEAT_INTERVAL_MS))
                except zmq.ZMQError as exc:
                    if exc.errno == zmq.ETERM or not self._running:
                        break
                    self.logger.error("ZMQ Poller error: %s", exc)
                    continue

                # 1. Drain RMC worker messages first (frees workers before processing new requests)
                if self.socket_rmc and events.get(self.socket_rmc) == zmq.POLLIN:
                    while True:
                        try:
                            frames = self.socket_rmc.recv_multipart(flags=zmq.NOBLOCK)
                            self._handle_rmc_message(frames)
                        except zmq.Again:
                            break

                # 2. Drain sync client requests (QUERY, RESERVE, MODIFY)
                if self.socket_sync and events.get(self.socket_sync) == zmq.POLLIN:
                    while True:
                        try:
                            frames = self.socket_sync.recv_multipart(flags=zmq.NOBLOCK)
                            self._handle_sync_message(frames)
                        except zmq.Again:
                            break

                # 3. Drain async client requests (CANCEL)
                if self.socket_async and events.get(self.socket_async) == zmq.POLLIN:
                    while True:
                        try:
                            frames = self.socket_async.recv_multipart(flags=zmq.NOBLOCK)
                            self._handle_async_message(frames)
                        except zmq.Again:
                            break

                now = time.perf_counter()

                # Periodic worker heartbeat check
                if now - last_heartbeat_check >= heartbeat_interval_s:
                    self._check_heartbeats(now)
                    last_heartbeat_check = now

                # Periodic request timeout check
                if now - last_timeout_check >= timeout_check_interval_s:
                    self._check_timeouts(now)
                    last_timeout_check = now

        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received, stopping...")
        finally:
            self.stop()

    # ── Sync Flow (QUERY / RESERVE / MODIFY) ──────────────────────────

    def _handle_sync_message(self, frames: list[bytes]) -> None:
        """Handle incoming sync request from ROUTER:5555."""
        t_received = time.perf_counter()

        if len(frames) < 2:
            self.logger.warning(
                "Received malformed sync message with too few frames: %d", len(frames)
            )
            return

        # Frames format from REQ: [client_identity, empty, request_data]
        payload_bytes = frames[-1]
        if len(frames) >= 3 and frames[-2] == b"":
            client_routing = frames[:-1]
        else:
            client_routing = [frames[0], b""]

        try:
            envelope = parse_envelope(payload_bytes)
        except Exception as exc:
            self.logger.error("Failed to parse sync request JSON: %s", exc)
            err_env = make_envelope(
                msg_type=MsgType.ERROR,
                payload={"error": f"Invalid JSON envelope: {exc}"},
            )
            if self.socket_sync:
                self.socket_sync.send_multipart(client_routing + [err_env.to_bytes()])
            log_request(
                self.logger,
                request_id=str(uuid.uuid4()),
                client_id="unknown",
                op_type="UNKNOWN",
                t_received=t_received,
                t_completed=time.perf_counter(),
                status="ERROR",
                extra={"error": str(exc)},
            )
            return

        request_id = envelope.request_id or str(uuid.uuid4())
        client_id = envelope.client_id

        # Duplicate check: update routing frames if client retried with a new identity
        if request_id in self.pending_requests:
            self.logger.info("Duplicate request_id %s received, updating routing", request_id)
            self.pending_requests[request_id].client_routing = client_routing
            return

        if self.socket_sync is None:
            return

        pending_req = PendingRequest(
            request_id=request_id,
            client_id=client_id,
            client_routing=client_routing,
            raw_request=payload_bytes,
            envelope=envelope,
            t_received=t_received,
            socket_ref=self.socket_sync,
        )

        self.pending_requests[request_id] = pending_req
        self.request_queue.append(request_id)

        # Attempt to dispatch immediately
        self._dispatch_queued_requests()

    # ── Async Flow (CANCEL) ──────────────────────────────────────────

    def _handle_async_message(self, frames: list[bytes]) -> None:
        """
        Handle incoming async cancel request from ROUTER:5556.

        1. Receives [client_identity, empty, request_data]
        2. Responds immediately with {status: 'ACCEPTED', cancellation_id: uuid}
        3. Forwards cancel request via PUSH:5575 to Cancel Service
        """
        t_received = time.perf_counter()

        if len(frames) < 2:
            self.logger.warning(
                "Received malformed async message with too few frames: %d", len(frames)
            )
            return

        payload_bytes = frames[-1]
        if len(frames) >= 3 and frames[-2] == b"":
            client_routing = frames[:-1]
        else:
            client_routing = [frames[0], b""]

        try:
            envelope = parse_envelope(payload_bytes)
        except Exception as exc:
            self.logger.error("Failed to parse async request JSON: %s", exc)
            err_env = make_envelope(
                msg_type=MsgType.ERROR,
                payload={"error": f"Invalid JSON envelope: {exc}"},
            )
            if self.socket_async:
                self.socket_async.send_multipart(client_routing + [err_env.to_bytes()])
            log_request(
                self.logger,
                request_id=str(uuid.uuid4()),
                client_id="unknown",
                op_type="CANCEL",
                t_received=t_received,
                t_completed=time.perf_counter(),
                status="ERROR",
                extra={"error": str(exc)},
            )
            return

        req_id = envelope.request_id or str(uuid.uuid4())
        client_id = envelope.client_id
        cancellation_id = envelope.payload.get("cancellation_id") or str(uuid.uuid4())

        # Step 3: Respond immediately with {status: 'ACCEPTED', cancellation_id: uuid}
        ack_env = make_envelope(
            msg_type=MsgType.RESPONSE,
            request_id=req_id,
            client_id=client_id,
            payload={
                "status": "ACCEPTED",
                "cancellation_id": cancellation_id,
            },
        )

        if self.socket_async:
            try:
                self.socket_async.send_multipart(client_routing + [ack_env.to_bytes()])
            except Exception as exc:
                self.logger.error("Failed to send async ACK to client %s: %s", req_id, exc)

        t_completed = time.perf_counter()
        log_request(
            self.logger,
            request_id=req_id,
            client_id=client_id,
            op_type=envelope.msg_type,
            t_received=t_received,
            t_completed=t_completed,
            status="ACCEPTED",
            extra={"cancellation_id": cancellation_id},
        )

        # Step 4: Forward cancel request via PUSH:5575 to Cancel Service
        cancel_payload = dict(envelope.payload)
        cancel_payload["cancellation_id"] = cancellation_id
        cancel_env = make_envelope(
            msg_type=MsgType.CANCEL_REQUEST,
            request_id=req_id,
            client_id=client_id,
            payload=cancel_payload,
        )

        if self.socket_cancel_push:
            try:
                self.socket_cancel_push.send(cancel_env.to_bytes(), flags=zmq.NOBLOCK)
                self.logger.debug(
                    "Forwarded CANCEL_REQUEST %s (cancellation_id=%s) to Cancel service",
                    req_id,
                    cancellation_id,
                )
            except zmq.Again:
                self.logger.error(
                    "Cancel service buffer full (zmq.Again); failed to push %s", req_id
                )
            except Exception as exc:
                self.logger.error("Failed to push cancel request %s: %s", req_id, exc)

    # ── RMC Worker Message Handling ──────────────────────────────────

    def _handle_rmc_message(self, frames: list[bytes]) -> None:
        """Handle messages received on backend ROUTER:5570 from RMC workers."""
        if len(frames) < 2:
            self.logger.warning(
                "Received malformed RMC message with too few frames: %d", len(frames)
            )
            return

        worker_id = frames[0]
        payload_bytes = frames[-1]

        # Check for raw string/byte control signals
        if payload_bytes in (b"READY", b"\x01"):
            self._handle_worker_ready(worker_id)
            return

        if payload_bytes in (b"HEARTBEAT", b"\x02"):
            self._handle_worker_heartbeat(worker_id)
            return

        envelope: Envelope | None = None
        try:
            envelope = parse_envelope(payload_bytes)
        except Exception as exc:
            self.logger.warning(
                "Failed to parse envelope from worker %s: %s", worker_id.hex(), exc
            )
            return

        if envelope.msg_type in (MsgType.READY, "READY"):
            self._handle_worker_ready(worker_id)
        elif envelope.msg_type in (MsgType.HEARTBEAT, "HEARTBEAT"):
            self._handle_worker_heartbeat(worker_id)
        else:
            # Worker completed processing a request
            self._handle_worker_response(worker_id, payload_bytes, envelope)

    def _handle_worker_ready(self, worker_id: bytes) -> None:
        """Process worker READY frame."""
        now = time.perf_counter()
        self.worker_last_seen[worker_id] = now

        # If this worker was previously assigned a request (e.g. crashed and restarted),
        # re-queue the orphaned request
        if worker_id in self.worker_to_request:
            old_req_id = self.worker_to_request.pop(worker_id)
            if old_req_id in self.pending_requests:
                old_req = self.pending_requests[old_req_id]
                old_req.assigned_worker = None
                old_req.t_assigned = None
                old_req.retries += 1
                client_timeout_s = Config.CLIENT_REQUEST_TIMEOUT_MS / 1000.0
                if (
                    old_req.retries <= self.MAX_RETRIES
                    and (now - old_req.t_received) < client_timeout_s
                ):
                    self.logger.info(
                        "Worker %s restarted; re-queueing request %s (retry %d/%d)",
                        worker_id.hex(),
                        old_req_id,
                        old_req.retries,
                        self.MAX_RETRIES,
                    )
                    self.request_queue.appendleft(old_req_id)
                else:
                    self._send_client_error(
                        old_req,
                        "Worker restarted before completing request and retry limit reached",
                    )
                    self.pending_requests.pop(old_req_id, None)

        if worker_id not in self.idle_workers:
            self.idle_workers.add(worker_id)
            self.available_workers.append(worker_id)
            self.logger.info(
                "Worker %s registered as READY (total idle: %d)",
                worker_id.hex(),
                len(self.available_workers),
            )
        else:
            self.logger.debug("Worker %s already in idle pool", worker_id.hex())

        self._dispatch_queued_requests()

    def _handle_worker_heartbeat(self, worker_id: bytes) -> None:
        """Process worker HEARTBEAT frame."""
        now = time.perf_counter()
        self.worker_last_seen[worker_id] = now

        # If worker is not currently busy and not yet in idle queue, register it
        if worker_id not in self.worker_to_request and worker_id not in self.idle_workers:
            self.idle_workers.add(worker_id)
            self.available_workers.append(worker_id)
            self.logger.info(
                "Worker %s added to idle queue via heartbeat (total idle: %d)",
                worker_id.hex(),
                len(self.available_workers),
            )
            self._dispatch_queued_requests()
        else:
            self.logger.debug("Heartbeat received from worker %s", worker_id.hex())

    def _handle_worker_response(
        self, worker_id: bytes, payload_bytes: bytes, envelope: Envelope
    ) -> None:
        """
        Process worker response for a completed request.

        1. Updates worker last-seen timestamp
        2. Returns worker to LRU idle queue
        3. Routes response back to original client
        4. Logs completed request with timing
        5. Dispatches next waiting request
        """
        now = time.perf_counter()
        self.worker_last_seen[worker_id] = now

        # Free the worker
        assigned_req_id = self.worker_to_request.pop(worker_id, None)

        if worker_id not in self.idle_workers:
            self.idle_workers.add(worker_id)
            self.available_workers.append(worker_id)

        req_id = envelope.request_id or assigned_req_id
        if not req_id:
            self.logger.warning(
                "Received response without request_id from worker %s", worker_id.hex()
            )
            self._dispatch_queued_requests()
            return

        req = self.pending_requests.pop(req_id, None)
        if not req:
            self.logger.warning(
                "Received late response for expired or unknown request %s from worker %s",
                req_id,
                worker_id.hex(),
            )
            self._dispatch_queued_requests()
            return

        # Route response back to client on ROUTER:5555
        try:
            req.socket_ref.send_multipart(req.client_routing + [payload_bytes])
            t_completed = time.perf_counter()
            log_request(
                self.logger,
                request_id=req.request_id,
                client_id=req.client_id,
                op_type=req.envelope.msg_type,
                t_received=req.t_received,
                t_completed=t_completed,
                status=envelope.msg_type,
            )
        except Exception as exc:
            self.logger.error(
                "Failed to send response for %s back to client: %s", req_id, exc
            )

        # Dispatch next waiting request now that worker is idle
        self._dispatch_queued_requests()

    # ── LRU Dispatching ──────────────────────────────────────────────

    def _dispatch_queued_requests(self) -> None:
        """
        Pair queued client requests with available LRU workers.

        Picks the least-recently-used worker from the front of the queue,
        verifies worker liveness, and forwards the request frame:
        [worker_identity, empty, request_data] on ROUTER:5570.
        """
        if not self.socket_rmc:
            return

        now = time.perf_counter()
        client_timeout_s = Config.CLIENT_REQUEST_TIMEOUT_MS / 1000.0
        worker_timeout_s = (
            Config.HEARTBEAT_LIVENESS * Config.HEARTBEAT_INTERVAL_MS
        ) / 1000.0

        while self.available_workers and self.request_queue:
            req_id = self.request_queue.popleft()
            req = self.pending_requests.get(req_id)
            if not req:
                continue

            # Check if request already timed out while waiting in queue
            if now - req.t_received >= client_timeout_s:
                self.logger.warning(
                    "Request %s timed out while waiting in queue (%.2fs)",
                    req_id,
                    now - req.t_received,
                )
                self._send_client_error(
                    req,
                    f"Request timed out after {Config.CLIENT_REQUEST_TIMEOUT_MS}ms waiting for available worker",
                )
                self.pending_requests.pop(req_id, None)
                continue

            # Pick least-recently-used live worker from front of queue
            worker_id: bytes | None = None
            while self.available_workers:
                candidate = self.available_workers.popleft()
                self.idle_workers.discard(candidate)

                last_seen = self.worker_last_seen.get(candidate, 0.0)
                if (now - last_seen) <= worker_timeout_s:
                    worker_id = candidate
                    break
                else:
                    self.logger.warning(
                        "Discarded dead worker from LRU queue: %s (no heartbeat for %.1fs)",
                        candidate.hex(),
                        now - last_seen,
                    )
                    self.worker_last_seen.pop(candidate, None)

            if worker_id is None:
                # No live workers available; push request back to front of queue
                self.request_queue.appendleft(req_id)
                break

            # Assign worker
            req.assigned_worker = worker_id
            req.t_assigned = now
            self.worker_to_request[worker_id] = req_id

            # Step 5: Send [worker_identity, empty, request_data] to ROUTER:5570
            try:
                self.socket_rmc.send_multipart([worker_id, b"", req.raw_request])
                self.logger.debug(
                    "Dispatched request %s to worker %s",
                    req_id,
                    worker_id.hex(),
                )
            except Exception as exc:
                self.logger.error(
                    "Failed to dispatch request %s to worker %s: %s",
                    req_id,
                    worker_id.hex(),
                    exc,
                )
                # Unassign and put back on queue for retry
                req.assigned_worker = None
                req.t_assigned = None
                self.worker_to_request.pop(worker_id, None)
                self.request_queue.appendleft(req_id)
                break

    # ── Heartbeat and Timeout Management ─────────────────────────────

    def _check_heartbeats(self, now: float) -> None:
        """
        Remove workers that have missed HEARTBEAT_LIVENESS heartbeats.

        If a dead worker was executing a request, re-queue the request for retry
        with another worker or fail if max retries / timeout exceeded.
        """
        worker_timeout_s = (
            Config.HEARTBEAT_LIVENESS * Config.HEARTBEAT_INTERVAL_MS
        ) / 1000.0
        client_timeout_s = Config.CLIENT_REQUEST_TIMEOUT_MS / 1000.0

        dead_workers: list[bytes] = []
        for worker_id, last_seen in list(self.worker_last_seen.items()):
            if now - last_seen > worker_timeout_s:
                dead_workers.append(worker_id)

        for worker_id in dead_workers:
            self.logger.warning(
                "Worker %s timed out (no heartbeat for %.1fs > %.1fs limit); removing from pool",
                worker_id.hex(),
                now - self.worker_last_seen.get(worker_id, 0.0),
                worker_timeout_s,
            )
            self.worker_last_seen.pop(worker_id, None)
            self.idle_workers.discard(worker_id)
            self.available_workers = deque(
                [w for w in self.available_workers if w != worker_id]
            )

            # Re-queue or fail any request assigned to this dead worker
            req_id = self.worker_to_request.pop(worker_id, None)
            if req_id and req_id in self.pending_requests:
                req = self.pending_requests[req_id]
                req.assigned_worker = None
                req.t_assigned = None
                req.retries += 1

                if (
                    req.retries <= self.MAX_RETRIES
                    and (now - req.t_received) < client_timeout_s
                ):
                    self.logger.info(
                        "Worker died mid-request; re-queueing %s (retry %d/%d)",
                        req_id,
                        req.retries,
                        self.MAX_RETRIES,
                    )
                    self.request_queue.appendleft(req_id)
                else:
                    self.logger.error(
                        "Worker died and request %s exceeded retry limit / timeout; returning error to client",
                        req_id,
                    )
                    self._send_client_error(
                        req,
                        f"Worker failed and request exceeded max retries ({self.MAX_RETRIES})",
                    )
                    self.pending_requests.pop(req_id, None)

        if dead_workers:
            self._dispatch_queued_requests()

    def _check_timeouts(self, now: float) -> None:
        """
        Check for queued requests or worker execution timeouts exceeding
        CLIENT_REQUEST_TIMEOUT_MS.
        """
        client_timeout_s = Config.CLIENT_REQUEST_TIMEOUT_MS / 1000.0

        # 1. Check requests waiting in queue without an assigned worker
        expired_queued: list[str] = []
        for req_id in list(self.request_queue):
            req = self.pending_requests.get(req_id)
            if not req:
                expired_queued.append(req_id)
                continue
            if now - req.t_received >= client_timeout_s:
                expired_queued.append(req_id)
                self.logger.warning(
                    "Queued request %s timed out after %.2fs waiting for worker",
                    req_id,
                    now - req.t_received,
                )
                self._send_client_error(
                    req,
                    f"Request timed out after {Config.CLIENT_REQUEST_TIMEOUT_MS}ms waiting for available worker",
                )
                self.pending_requests.pop(req_id, None)

        if expired_queued:
            self.request_queue = deque(
                [r for r in self.request_queue if r not in expired_queued]
            )

        # 2. Check active requests assigned to workers that failed to respond in time
        for req_id, req in list(self.pending_requests.items()):
            if req.assigned_worker is not None and req.t_assigned is not None:
                if (now - req.t_assigned >= client_timeout_s) or (
                    now - req.t_received >= client_timeout_s
                ):
                    worker_id = req.assigned_worker
                    self.logger.warning(
                        "Request %s timed out waiting for worker %s response (>%.1fs)",
                        req_id,
                        worker_id.hex(),
                        client_timeout_s,
                    )

                    # Evict unresponsive worker from pools
                    self.worker_to_request.pop(worker_id, None)
                    self.idle_workers.discard(worker_id)
                    self.available_workers = deque(
                        [w for w in self.available_workers if w != worker_id]
                    )
                    self.worker_last_seen.pop(worker_id, None)

                    # Send timeout error to client
                    self._send_client_error(
                        req,
                        f"Request timed out waiting for worker response after {Config.CLIENT_REQUEST_TIMEOUT_MS}ms",
                    )
                    self.pending_requests.pop(req_id, None)

    def _send_client_error(self, req: PendingRequest, error_msg: str) -> None:
        """Send an ERROR envelope back to the client socket and log it."""
        err_env = make_envelope(
            msg_type=MsgType.ERROR,
            request_id=req.request_id,
            client_id=req.client_id,
            payload={"error": error_msg},
        )
        try:
            req.socket_ref.send_multipart(req.client_routing + [err_env.to_bytes()])
            t_completed = time.perf_counter()
            log_request(
                self.logger,
                request_id=req.request_id,
                client_id=req.client_id,
                op_type=req.envelope.msg_type,
                t_received=req.t_received,
                t_completed=t_completed,
                status="ERROR",
                extra={"error": error_msg},
            )
        except Exception as exc:
            self.logger.error(
                "Failed to send error response to client for request %s: %s",
                req.request_id,
                exc,
            )


def main() -> None:
    """Entry point for the Gestor de Transacciones (GT) service."""
    service = GTService()

    def handle_signal(sig: int, frame: Any) -> None:
        service.logger.info("Signal %d received, initiating graceful shutdown...", sig)
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    service.run()


if __name__ == "__main__":
    main()
