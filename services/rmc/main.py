"""
RMC (Reservations, Modifications, Queries) worker service for SRB.

Scalable business logic component that interacts with the Transaction Manager (GT)
via an LRU worker pattern over ZeroMQ DEALER socket and executes database queries
and transactions against the Persistence Service via ZeroMQ REQ socket.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from typing import Any
import zmq

from common.config import Config
from common.health import HealthServer
from common.logging_json import get_logger, log_request
from common.messages import Envelope, MsgType, make_envelope, parse_envelope

# Ensure component name is set for structured logging
if Config.COMPONENT_NAME == "unknown":
    Config.COMPONENT_NAME = "rmc"

logger = get_logger("rmc")


class PersistenceClient:
    """Client for communicating with the Persistence Service over ZeroMQ REQ/REP.

    Implements the Lazy Pirate pattern: if a request times out, the REQ socket
    is closed with LINGER=0 and reconnected to avoid ZeroMQ EFSM deadlocks.
    """

    def __init__(
        self,
        context: zmq.Context,
        host: str | None = None,
        port: int | None = None,
        timeout_ms: int = 5000,
    ) -> None:
        self._context = context
        self._host = host or Config.PERSIST_HOST
        self._port = port or Config.PERSIST_PORT
        self._endpoint = f"tcp://{self._host}:{self._port}"
        self._timeout_ms = timeout_ms
        self._socket: zmq.Socket | None = None
        self._poller = zmq.Poller()
        self._connect()

    def _connect(self) -> None:
        """Create and connect a new REQ socket, discarding any previous socket."""
        if self._socket is not None:
            try:
                self._poller.unregister(self._socket)
            except Exception:
                pass
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.close()

        self._socket = self._context.socket(zmq.REQ)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._endpoint)
        self._poller.register(self._socket, zmq.POLLIN)

    def request(self, envelope: Envelope) -> Envelope:
        """Send a request envelope and wait for a response envelope.

        Raises:
            TimeoutError: If the persistence service does not reply within timeout_ms.
            RuntimeError: If transmission fails.
        """
        assert self._socket is not None, "Persistence socket not initialized"

        data = envelope.to_bytes()
        try:
            self._socket.send(data)
        except zmq.ZMQError as exc:
            logger.warning(
                "Error sending request to persistence service: %s; reconnecting",
                exc,
            )
            self._connect()
            raise RuntimeError(f"Persistence connection error: {exc}") from exc

        events = dict(self._poller.poll(self._timeout_ms))
        if self._socket in events and events[self._socket] == zmq.POLLIN:
            reply_bytes = self._socket.recv()
            return parse_envelope(reply_bytes)

        # Timeout: reset socket to prevent EFSM lock
        logger.warning(
            "Persistence request %s timed out after %d ms. Reconnecting REQ socket.",
            envelope.request_id,
            self._timeout_ms,
        )
        self._connect()
        raise TimeoutError(
            f"Persistence service timed out after {self._timeout_ms} ms"
        )

    def send_query(
        self,
        payload: dict[str, Any],
        client_id: str = "",
        request_id: str | None = None,
    ) -> Envelope:
        """Send a DB_QUERY message to the persistence service."""
        env = make_envelope(
            msg_type=MsgType.DB_QUERY,
            payload=payload,
            client_id=client_id,
            request_id=request_id,
        )
        return self.request(env)

    def send_execute(
        self,
        payload: dict[str, Any],
        client_id: str = "",
        request_id: str | None = None,
    ) -> Envelope:
        """Send a DB_EXECUTE message to the persistence service."""
        env = make_envelope(
            msg_type=MsgType.DB_EXECUTE,
            payload=payload,
            client_id=client_id,
            request_id=request_id,
        )
        return self.request(env)

    def close(self) -> None:
        """Close the persistence client socket."""
        if self._socket is not None:
            try:
                self._poller.unregister(self._socket)
            except Exception:
                pass
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.close()
            self._socket = None


class RMCService:
    """Reservations, Modifications and Queries (RMC) worker service."""

    def __init__(
        self,
        context: zmq.Context | None = None,
        persist_client: PersistenceClient | None = None,
        worker_id: str | None = None,
        heartbeat_interval_ms: int | None = None,
    ) -> None:
        hostname = os.getenv("HOSTNAME", "local")
        self._worker_id = worker_id or f"rmc-{hostname}-{os.getpid()}"
        self._context = context or zmq.Context.instance()
        self._dealer: zmq.Socket | None = self._context.socket(zmq.DEALER)
        self._dealer.setsockopt_string(zmq.IDENTITY, self._worker_id)
        self._dealer.setsockopt(zmq.LINGER, 0)

        hb_ms = heartbeat_interval_ms or Config.HEARTBEAT_INTERVAL_MS
        self._heartbeat_interval_s: float = hb_ms / 1000.0
        self._last_heartbeat_time: float = 0.0

        self._persist = persist_client or PersistenceClient(self._context)
        self._health_server = HealthServer(
            port=Config.HEALTH_PORT, check_fn=self.is_healthy
        )
        self._running: bool = False
        self._poller = zmq.Poller()

    def is_healthy(self) -> bool:
        """Health check function for HTTP /healthz probe."""
        return self._running

    def send_ready(self) -> None:
        """Send READY announcement frame to GT LRU broker."""
        assert self._dealer is not None
        ready_env = make_envelope(
            msg_type=MsgType.READY,
            payload={"worker_id": self._worker_id},
            client_id=self._worker_id,
        )
        self._dealer.send_multipart([b"", ready_env.to_bytes()])
        self._last_heartbeat_time = time.perf_counter()
        logger.info(
            "Sent READY announcement to GT broker",
            extra={"worker_id": self._worker_id},
        )

    def send_heartbeat(self) -> None:
        """Send periodic HEARTBEAT frame to GT LRU broker."""
        assert self._dealer is not None
        hb_env = make_envelope(
            msg_type=MsgType.HEARTBEAT,
            payload={"worker_id": self._worker_id},
            client_id=self._worker_id,
        )
        self._dealer.send_multipart([b"", hb_env.to_bytes()])
        self._last_heartbeat_time = time.perf_counter()
        logger.debug(
            "Sent HEARTBEAT to GT broker",
            extra={"worker_id": self._worker_id},
        )

    def handle_query(self, req: Envelope) -> Envelope:
        """Process a QUERY request (e.g. events list for a given month)."""
        month = req.payload.get("month", Config.SEED_MONTH)
        try:
            db_resp = self._persist.send_query(
                {"operation": "QUERY_EVENTS", "month": month},
                client_id=req.client_id,
                request_id=req.request_id,
            )
            return make_envelope(
                msg_type=MsgType.RESPONSE,
                payload=db_resp.payload,
                client_id=req.client_id,
                request_id=req.request_id,
            )
        except TimeoutError as exc:
            logger.error("Persistence query timed out for request %s: %s", req.request_id, exc)
            return make_envelope(
                msg_type=MsgType.ERROR,
                payload={"status": "ERROR", "reason": "Persistence service timed out"},
                client_id=req.client_id,
                request_id=req.request_id,
            )
        except Exception as exc:
            logger.exception("Error executing query for request %s: %s", req.request_id, exc)
            return make_envelope(
                msg_type=MsgType.ERROR,
                payload={"status": "ERROR", "reason": f"Persistence error: {exc}"},
                client_id=req.client_id,
                request_id=req.request_id,
            )

    def handle_reserve(self, req: Envelope) -> Envelope:
        """Process a RESERVE request.

        Supports providing occurrence_id directly or resolving it from
        event_id + occurrence_date.
        """
        seats = req.payload.get("seats")
        if seats is None:
            return make_envelope(
                msg_type=MsgType.RESPONSE,
                payload={"status": "REJECTED", "reason": "Missing seats in request payload"},
                client_id=req.client_id,
                request_id=req.request_id,
            )

        try:
            seats_int = int(seats)
            if seats_int <= 0:
                return make_envelope(
                    msg_type=MsgType.RESPONSE,
                    payload={"status": "REJECTED", "reason": "seats must be a positive integer"},
                    client_id=req.client_id,
                    request_id=req.request_id,
                )
        except (ValueError, TypeError):
            return make_envelope(
                msg_type=MsgType.RESPONSE,
                payload={"status": "REJECTED", "reason": "Invalid seats parameter: must be integer"},
                client_id=req.client_id,
                request_id=req.request_id,
            )

        occurrence_id = req.payload.get("occurrence_id")
        if not occurrence_id:
            event_id = req.payload.get("event_id")
            occurrence_date = req.payload.get("occurrence_date") or req.payload.get("date")
            if not event_id or not occurrence_date:
                return make_envelope(
                    msg_type=MsgType.RESPONSE,
                    payload={
                        "status": "REJECTED",
                        "reason": "Missing occurrence_id or (event_id and occurrence_date)",
                    },
                    client_id=req.client_id,
                    request_id=req.request_id,
                )

            # Query persistence for the occurrence
            try:
                occ_resp = self._persist.send_query(
                    {
                        "operation": "GET_OCCURRENCE",
                        "event_id": str(event_id),
                        "date": str(occurrence_date),
                    },
                    client_id=req.client_id,
                    request_id=req.request_id,
                )
            except Exception as exc:
                return make_envelope(
                    msg_type=MsgType.ERROR,
                    payload={"status": "ERROR", "reason": f"Persistence error during occurrence lookup: {exc}"},
                    client_id=req.client_id,
                    request_id=req.request_id,
                )

            if occ_resp.payload.get("status") != "OK":
                return make_envelope(
                    msg_type=MsgType.RESPONSE,
                    payload={
                        "status": "REJECTED",
                        "reason": occ_resp.payload.get(
                            "reason",
                            f"No occurrence found for event {event_id} on {occurrence_date}",
                        ),
                    },
                    client_id=req.client_id,
                    request_id=req.request_id,
                )

            occ_data = occ_resp.payload.get("occurrence")
            if isinstance(occ_data, dict):
                occurrence_id = occ_data.get("occurrence_id")
            else:
                occurrence_id = occ_resp.payload.get("occurrence_id")

            if not occurrence_id:
                return make_envelope(
                    msg_type=MsgType.RESPONSE,
                    payload={
                        "status": "REJECTED",
                        "reason": f"No occurrence found for event {event_id} on {occurrence_date}",
                    },
                    client_id=req.client_id,
                    request_id=req.request_id,
                )

        client_id = req.client_id or req.payload.get("client_id", "")
        try:
            exec_resp = self._persist.send_execute(
                {
                    "operation": "RESERVE",
                    "occurrence_id": str(occurrence_id),
                    "seats": seats_int,
                    "client_id": str(client_id),
                },
                client_id=req.client_id,
                request_id=req.request_id,
            )
            return make_envelope(
                msg_type=MsgType.RESPONSE,
                payload=exec_resp.payload,
                client_id=req.client_id,
                request_id=req.request_id,
            )
        except TimeoutError as exc:
            logger.error("Persistence RESERVE timed out for request %s: %s", req.request_id, exc)
            return make_envelope(
                msg_type=MsgType.ERROR,
                payload={"status": "ERROR", "reason": "Persistence service timed out during reserve"},
                client_id=req.client_id,
                request_id=req.request_id,
            )
        except Exception as exc:
            logger.exception("Persistence RESERVE error for request %s: %s", req.request_id, exc)
            return make_envelope(
                msg_type=MsgType.ERROR,
                payload={"status": "ERROR", "reason": f"Persistence error: {exc}"},
                client_id=req.client_id,
                request_id=req.request_id,
            )

    def handle_modify(self, req: Envelope) -> Envelope:
        """Process a MODIFY request (seat change or occurrence change).

        Atomic rules:
        - If new_seats is provided: modify seats on existing occurrence.
        - If new_occurrence_date is provided: switch to the alternative occurrence
          for the same event if one exists. If no other occurrence exists or no seats
          are available, the modification is rejected and the original reservation is untouched.
        """
        reservation_id = req.payload.get("reservation_id")
        if not reservation_id:
            return make_envelope(
                msg_type=MsgType.RESPONSE,
                payload={"status": "REJECTED", "reason": "Missing reservation_id in modify payload"},
                client_id=req.client_id,
                request_id=req.request_id,
            )

        new_seats = req.payload.get("new_seats")
        new_occurrence_date = req.payload.get("new_occurrence_date")
        new_occurrence_id = req.payload.get("new_occurrence_id")

        if new_seats is not None:
            try:
                new_seats_int = int(new_seats)
                if new_seats_int <= 0:
                    return make_envelope(
                        msg_type=MsgType.RESPONSE,
                        payload={"status": "REJECTED", "reason": "new_seats must be a positive integer"},
                        client_id=req.client_id,
                        request_id=req.request_id,
                    )
            except (ValueError, TypeError):
                return make_envelope(
                    msg_type=MsgType.RESPONSE,
                    payload={"status": "REJECTED", "reason": "Invalid new_seats parameter: must be integer"},
                    client_id=req.client_id,
                    request_id=req.request_id,
                )

            try:
                exec_resp = self._persist.send_execute(
                    {
                        "operation": "MODIFY_SEATS",
                        "reservation_id": str(reservation_id),
                        "new_seats": new_seats_int,
                    },
                    client_id=req.client_id,
                    request_id=req.request_id,
                )
                return make_envelope(
                    msg_type=MsgType.RESPONSE,
                    payload=exec_resp.payload,
                    client_id=req.client_id,
                    request_id=req.request_id,
                )
            except Exception as exc:
                logger.exception("Error modifying seats for request %s: %s", req.request_id, exc)
                return make_envelope(
                    msg_type=MsgType.ERROR,
                    payload={"status": "ERROR", "reason": f"Persistence error: {exc}"},
                    client_id=req.client_id,
                    request_id=req.request_id,
                )

        elif new_occurrence_date is not None or new_occurrence_id is not None:
            try:
                if new_occurrence_id is not None:
                    target_occurrence_id = str(new_occurrence_id)
                else:
                    # 1. Get reservation info
                    res_resp = self._persist.send_query(
                        {
                            "operation": "GET_RESERVATION",
                            "reservation_id": str(reservation_id),
                        },
                        client_id=req.client_id,
                        request_id=req.request_id,
                    )
                    if res_resp.payload.get("status") != "OK":
                        return make_envelope(
                            msg_type=MsgType.RESPONSE,
                            payload={
                                "status": "REJECTED",
                                "reason": res_resp.payload.get(
                                    "reason", f"Reservation {reservation_id} not found"
                                ),
                            },
                            client_id=req.client_id,
                            request_id=req.request_id,
                        )

                    res_info = res_resp.payload.get("reservation")
                    if not isinstance(res_info, dict):
                        res_info = res_resp.payload

                    current_occurrence_id = res_info.get("occurrence_id")
                    if not current_occurrence_id:
                        return make_envelope(
                            msg_type=MsgType.RESPONSE,
                            payload={"status": "REJECTED", "reason": "Reservation has no valid occurrence_id"},
                            client_id=req.client_id,
                            request_id=req.request_id,
                        )

                    # Check reservation status
                    res_status = res_info.get("status")
                    if res_status and res_status != "ACTIVE":
                        return make_envelope(
                            msg_type=MsgType.RESPONSE,
                            payload={
                                "status": "REJECTED",
                                "reason": f"Reservation is {res_status}, cannot be modified",
                            },
                            client_id=req.client_id,
                            request_id=req.request_id,
                        )

                    # 2. Get occurrence info to find event_id if not present in reservation info
                    event_id = res_info.get("event_id")
                    if not event_id:
                        occ_resp = self._persist.send_query(
                            {
                                "operation": "GET_OCCURRENCE",
                                "occurrence_id": str(current_occurrence_id),
                            },
                            client_id=req.client_id,
                            request_id=req.request_id,
                        )
                        if occ_resp.payload.get("status") != "OK":
                            return make_envelope(
                                msg_type=MsgType.RESPONSE,
                                payload={
                                    "status": "REJECTED",
                                    "reason": occ_resp.payload.get(
                                        "reason", "Current occurrence not found"
                                    ),
                                },
                                client_id=req.client_id,
                                request_id=req.request_id,
                            )
                        occ_data = occ_resp.payload.get("occurrence")
                        if isinstance(occ_data, dict):
                            event_id = occ_data.get("event_id")
                        else:
                            event_id = occ_resp.payload.get("event_id")

                    if not event_id:
                        return make_envelope(
                            msg_type=MsgType.RESPONSE,
                            payload={
                                "status": "REJECTED",
                                "reason": "Event ID could not be determined for reservation",
                            },
                            client_id=req.client_id,
                            request_id=req.request_id,
                        )

                    # 3. Find the target occurrence
                    if new_occurrence_date is not None:
                        target_resp = self._persist.send_query(
                            {
                                "operation": "GET_OCCURRENCE",
                                "event_id": str(event_id),
                                "date": str(new_occurrence_date),
                            },
                            client_id=req.client_id,
                            request_id=req.request_id,
                        )
                        if target_resp.payload.get("status") != "OK" or not target_resp.payload.get("occurrence_id"):
                            return make_envelope(
                                msg_type=MsgType.RESPONSE,
                                payload={
                                    "status": "REJECTED",
                                    "reason": f"No occurrence found for event {event_id} on date {new_occurrence_date}",
                                },
                                client_id=req.client_id,
                                request_id=req.request_id,
                            )
                        target_occurrence_id = target_resp.payload.get("occurrence_id")
                        if str(target_occurrence_id) == str(current_occurrence_id):
                            return make_envelope(
                                msg_type=MsgType.RESPONSE,
                                payload={
                                    "status": "REJECTED",
                                    "reason": "Reservation is already for this occurrence date",
                                },
                                client_id=req.client_id,
                                request_id=req.request_id,
                            )
                    else:
                        other_resp = self._persist.send_query(
                            {
                                "operation": "GET_OTHER_OCCURRENCE",
                                "event_id": str(event_id),
                                "exclude_occurrence_id": str(current_occurrence_id),
                            },
                            client_id=req.client_id,
                            request_id=req.request_id,
                        )
                        if other_resp.payload.get("status") != "OK":
                            return make_envelope(
                                msg_type=MsgType.RESPONSE,
                                payload={
                                    "status": "REJECTED",
                                    "reason": other_resp.payload.get(
                                        "reason", "No other occurrence exists for this event"
                                    ),
                                },
                                client_id=req.client_id,
                                request_id=req.request_id,
                            )

                        other_data = other_resp.payload.get("occurrence")
                        if isinstance(other_data, dict):
                            target_occurrence_id = other_data.get("occurrence_id")
                        else:
                            target_occurrence_id = other_resp.payload.get("occurrence_id")

                        if not target_occurrence_id:
                            return make_envelope(
                                msg_type=MsgType.RESPONSE,
                                payload={
                                    "status": "REJECTED",
                                    "reason": "No other occurrence exists for this event",
                                },
                                client_id=req.client_id,
                                request_id=req.request_id,
                            )

                # 4. Send DB_EXECUTE: MODIFY_DATE
                exec_resp = self._persist.send_execute(
                    {
                        "operation": "MODIFY_DATE",
                        "reservation_id": str(reservation_id),
                        "new_occurrence_id": str(target_occurrence_id),
                    },
                    client_id=req.client_id,
                    request_id=req.request_id,
                )
                return make_envelope(
                    msg_type=MsgType.RESPONSE,
                    payload=exec_resp.payload,
                    client_id=req.client_id,
                    request_id=req.request_id,
                )
            except Exception as exc:
                logger.exception("Error modifying date for request %s: %s", req.request_id, exc)
                return make_envelope(
                    msg_type=MsgType.ERROR,
                    payload={"status": "ERROR", "reason": f"Persistence error: {exc}"},
                    client_id=req.client_id,
                    request_id=req.request_id,
                )

        else:
            return make_envelope(
                msg_type=MsgType.RESPONSE,
                payload={
                    "status": "REJECTED",
                    "reason": "Neither new_seats nor new_occurrence_date provided",
                },
                client_id=req.client_id,
                request_id=req.request_id,
            )

    def process_request(self, req: Envelope) -> Envelope:
        """Route client request to the appropriate business logic handler."""
        if req.msg_type == MsgType.QUERY:
            return self.handle_query(req)
        elif req.msg_type == MsgType.RESERVE:
            return self.handle_reserve(req)
        elif req.msg_type == MsgType.MODIFY:
            return self.handle_modify(req)
        else:
            logger.warning("Unsupported message type received: %s", req.msg_type)
            return make_envelope(
                msg_type=MsgType.ERROR,
                payload={
                    "status": "ERROR",
                    "reason": f"Unsupported message type: {req.msg_type}",
                },
                client_id=req.client_id,
                request_id=req.request_id,
            )

    def run(self) -> None:
        """Main event loop: announce READY, poll GT requests, send heartbeats."""
        assert self._dealer is not None, "DEALER socket is not initialized"

        logger.info(
            "Starting RMC worker service: %s",
            self._worker_id,
            extra={
                "worker_id": self._worker_id,
                "gt_rmc_host": Config.GT_RMC_HOST,
                "gt_rmc_port": Config.GT_RMC_PORT,
                "persist_host": Config.PERSIST_HOST,
                "persist_port": Config.PERSIST_PORT,
                "health_port": Config.HEALTH_PORT,
            },
        )

        # Start health server in background thread for K8s liveness probes
        self._health_server.start()
        logger.info("Health server listening on port %d", Config.HEALTH_PORT)

        # Connect DEALER socket to GT broker
        gt_endpoint = f"tcp://{Config.GT_RMC_HOST}:{Config.GT_RMC_PORT}"
        self._dealer.connect(gt_endpoint)
        self._poller.register(self._dealer, zmq.POLLIN)
        logger.info("Connected DEALER socket to GT broker at %s", gt_endpoint)

        # Send initial READY announcement
        self.send_ready()

        self._running = True
        while self._running:
            try:
                # Calculate time until next heartbeat
                time_since_hb = time.perf_counter() - self._last_heartbeat_time
                remaining_s = self._heartbeat_interval_s - time_since_hb
                if remaining_s <= 0:
                    self.send_heartbeat()
                    poll_timeout_ms = int(self._heartbeat_interval_s * 1000)
                else:
                    poll_timeout_ms = max(10, int(remaining_s * 1000))

                events = dict(self._poller.poll(poll_timeout_ms))

                if self._dealer in events and events[self._dealer] == zmq.POLLIN:
                    frames = self._dealer.recv_multipart()
                    if not self._running:
                        break

                    # In LRU broker: dealer receives [empty, request_data]
                    if len(frames) >= 2 and frames[0] == b"":
                        raw_data = frames[1]
                    elif len(frames) == 1:
                        raw_data = frames[0]
                    else:
                        raw_data = frames[-1]

                    t_received = time.perf_counter()

                    try:
                        req_env = parse_envelope(raw_data)
                    except Exception as exc:
                        logger.error("Failed to parse incoming envelope: %s", exc)
                        err_env = make_envelope(
                            MsgType.ERROR,
                            payload={"status": "ERROR", "reason": f"Malformed envelope: {exc}"},
                        )
                        self._dealer.send_multipart([b"", err_env.to_bytes()])
                        continue

                    # Ignore ping heartbeats from broker
                    if req_env.msg_type == MsgType.HEARTBEAT:
                        logger.debug("Received heartbeat ping from GT")
                        continue

                    # Process request through business logic
                    try:
                        resp_env = self.process_request(req_env)
                    except Exception as exc:
                        logger.exception(
                            "Unhandled exception processing request %s",
                            req_env.request_id,
                        )
                        resp_env = make_envelope(
                            MsgType.ERROR,
                            payload={"status": "ERROR", "reason": f"Internal server error: {exc}"},
                            client_id=req_env.client_id,
                            request_id=req_env.request_id,
                        )

                    t_completed = time.perf_counter()

                    # Send response back to GT: [empty, response_data]
                    self._dealer.send_multipart([b"", resp_env.to_bytes()])

                    # Structured JSON log with execution timing
                    status = str(resp_env.payload.get("status", "OK"))
                    log_request(
                        logger,
                        request_id=req_env.request_id,
                        client_id=req_env.client_id,
                        op_type=req_env.msg_type,
                        t_received=t_received,
                        t_completed=t_completed,
                        status=status,
                        extra={"worker_id": self._worker_id},
                    )

            except zmq.ZMQError as exc:
                if not self._running or exc.errno in (zmq.ETERM, 4):  # 4 = EINTR
                    break
                logger.error("ZMQ error in RMC main loop: %s", exc)
            except Exception as exc:
                if not self._running:
                    break
                logger.exception("Unexpected error in RMC main loop: %s", exc)

        self.stop()

    def request_stop(self) -> None:
        """Signal the service loop to terminate gracefully."""
        self._running = False

    def stop(self) -> None:
        """Clean up sockets and stop background threads."""
        if not self._running and self._dealer is None:
            return
        logger.info("Stopping RMC worker service: %s", self._worker_id)
        self._running = False

        # Stop HTTP health server
        try:
            self._health_server.stop()
        except Exception as exc:
            logger.warning("Error stopping health server: %s", exc)

        # Close persistence client
        try:
            self._persist.close()
        except Exception as exc:
            logger.warning("Error closing persistence client: %s", exc)

        # Close DEALER socket
        if self._dealer is not None:
            try:
                self._poller.unregister(self._dealer)
            except Exception:
                pass
            self._dealer.setsockopt(zmq.LINGER, 0)
            self._dealer.close()
            self._dealer = None

        logger.info("RMC worker shutdown complete: %s", self._worker_id)


def main() -> None:
    """Service entrypoint."""
    service = RMCService()

    def handle_signal(sig: int, frame: Any) -> None:
        logger.info("Signal %d received, requesting graceful shutdown...", sig)
        service.request_stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        service.run()
    except KeyboardInterrupt:
        pass
    finally:
        service.stop()


if __name__ == "__main__":
    main()
