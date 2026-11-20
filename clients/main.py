"""
SRB Client service pod implementation.

Generates realistic workload of ticket reservation operations:
- Query events for a month (sync via REQ -> GT)
- Reserve seats for an event occurrence (sync via REQ -> GT)
- Modify existing reservation: seats or date (sync via REQ -> GT)
- Cancel existing reservation (async via DEALER -> GT, notification on SUB)

Fault tolerance:
- Lazy Pirate pattern for REQ socket: recreates socket on zmq.RCVTIMEO with exponential backoff.
- Thread-safe local reservation tracking.
- Health server exposing GET /healthz on port 8100.
"""

from __future__ import annotations

import hashlib
import os
import random
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

import zmq

from common.config import Config
from common.health import HealthServer
from common.logging_json import get_logger, log_request
from common.messages import Envelope, MsgType, make_envelope, parse_envelope


@dataclass
class ActiveReservation:
    """Tracks an active reservation owned by this client."""

    reservation_id: str
    event_id: str
    occurrence_date: str
    seats: int
    occurrence_id: str = ""


class SRBClient:
    """Simulated user client pod for the SRB distributed system."""

    def __init__(self) -> None:
        self.client_id: str = os.getenv("HOSTNAME", f"client-{uuid.uuid4().hex[:8]}")
        Config.COMPONENT_NAME = f"client-{self.client_id}"
        self.logger = get_logger("srb.client")

        # Configurable operation probabilities (%)
        self.pct_query: float = float(os.getenv("CLIENT_QUERY_PCT", "40"))
        self.pct_reserve: float = float(os.getenv("CLIENT_RESERVE_PCT", "30"))
        self.pct_modify: float = float(os.getenv("CLIENT_MODIFY_PCT", "20"))
        self.pct_cancel: float = float(os.getenv("CLIENT_CANCEL_PCT", "10"))

        # Target rate in requests per minute (at least 20 req/min)
        self.client_rate: float = float(os.getenv("CLIENT_RATE", "20"))
        self.request_interval_s: float = 60.0 / max(1.0, self.client_rate)

        # Reproducible random seed: SEED_RANDOM_SEED + hash(client_id)
        # Using md5 prefix for deterministic hash across Python processes
        hash_offset = int(hashlib.md5(self.client_id.encode("utf-8")).hexdigest()[:8], 16)
        seed_value = Config.SEED_RANDOM_SEED + (hash_offset % 100000)
        self.rng = random.Random(seed_value)

        # Local state
        self._res_lock = threading.Lock()
        self.active_reservations: list[ActiveReservation] = []
        self.known_events: list[dict[str, Any]] = []

        # ZeroMQ context and sockets
        self.zmq_ctx = zmq.Context()
        self.req_sock: zmq.Socket | None = None
        self.dealer_sock: zmq.Socket | None = None
        self.sub_sock: zmq.Socket | None = None

        # Threading and lifecycle
        self.running: bool = False
        self.sub_thread: threading.Thread | None = None
        self.health_server = HealthServer(port=Config.HEALTH_PORT, check_fn=lambda: self.running)

    def start(self) -> None:
        """Initialize sockets, start background threads and run operation loop."""
        self.running = True
        self.health_server.start()
        self.logger.info(
            "Client starting",
            extra={
                "extra_fields": {
                    "client_id": self.client_id,
                    "target_rate_rpm": self.client_rate,
                    "weights": {
                        "query": self.pct_query,
                        "reserve": self.pct_reserve,
                        "modify": self.pct_modify,
                        "cancel": self.pct_cancel,
                    },
                }
            },
        )

        self._init_sockets()
        self._start_sub_listener()
        self._run_loop()

    def stop(self) -> None:
        """Gracefully shut down sockets and threads."""
        if not self.running:
            return
        self.running = False
        self.logger.info("Stopping client...")

        self.health_server.stop()

        if self.sub_thread and self.sub_thread.is_alive():
            self.sub_thread.join(timeout=2.0)

        # Close sockets
        for sock in (self.req_sock, self.dealer_sock, self.sub_sock):
            if sock is not None:
                try:
                    sock.close(linger=0)
                except Exception:
                    pass

        try:
            self.zmq_ctx.term()
        except Exception:
            pass
        self.logger.info("Client stopped cleanly.")

    # ── ZeroMQ Socket Management ──────────────────────────────────────

    def _init_sockets(self) -> None:
        """Create and connect REQ, DEALER and SUB sockets."""
        # 1. REQ socket for synchronous operations (QUERY, RESERVE, MODIFY)
        self._reconnect_req()

        # 2. DEALER socket for asynchronous cancellation
        self.dealer_sock = self.zmq_ctx.socket(zmq.DEALER)
        self.dealer_sock.setsockopt_string(zmq.IDENTITY, self.client_id)
        self.dealer_sock.setsockopt(zmq.RCVTIMEO, 2000)
        self.dealer_sock.setsockopt(zmq.SNDTIMEO, 5000)
        self.dealer_sock.setsockopt(zmq.LINGER, 0)
        dealer_endpoint = f"tcp://{Config.GT_ASYNC_HOST}:{Config.GT_ASYNC_PORT}"
        self.dealer_sock.connect(dealer_endpoint)
        self.logger.info("Connected DEALER to %s", dealer_endpoint)

        # 3. SUB socket for cancellation notifications
        self.sub_sock = self.zmq_ctx.socket(zmq.SUB)
        self.sub_sock.setsockopt(zmq.RCVTIMEO, 1000)  # check self.running every second
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, self.client_id)
        sub_endpoint = f"tcp://{Config.NOTIFY_HOST}:{Config.NOTIFY_PORT}"
        self.sub_sock.connect(sub_endpoint)
        self.logger.info(
            "Connected SUB to %s (topic=%s)", sub_endpoint, self.client_id
        )

    def _create_req_socket(self) -> zmq.Socket:
        """Create a fresh REQ socket with configured timeouts."""
        sock = self.zmq_ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, Config.CLIENT_REQUEST_TIMEOUT_MS)
        sock.setsockopt(zmq.SNDTIMEO, Config.CLIENT_REQUEST_TIMEOUT_MS)
        sock.setsockopt(zmq.LINGER, 0)
        endpoint = f"tcp://{Config.GT_SYNC_HOST}:{Config.GT_SYNC_PORT}"
        sock.connect(endpoint)
        return sock

    def _reconnect_req(self) -> None:
        """Recreate the REQ socket to recover from timeout or network disruption."""
        if self.req_sock is not None:
            try:
                self.req_sock.close(linger=0)
            except Exception:
                pass
        self.req_sock = self._create_req_socket()

    def _send_req_with_retry(
        self, envelope: Envelope, max_retries: int = 3
    ) -> Envelope | None:
        """
        Send a synchronous message on the REQ socket with Lazy Pirate fault tolerance.

        Recreates the REQ socket on timeout with exponential backoff.
        """
        backoff = Config.CLIENT_RECONNECT_DELAY_S
        t_perf_start = time.perf_counter()
        t_epoch_start = time.time()

        for attempt in range(1, max_retries + 1):
            if not self.running:
                return None
            try:
                assert self.req_sock is not None
                self.req_sock.send(envelope.to_bytes())
                reply_bytes = self.req_sock.recv()

                t_perf_end = time.perf_counter()
                elapsed_s = t_perf_end - t_perf_start
                resp_envelope = parse_envelope(reply_bytes)

                status = "OK"
                if resp_envelope.msg_type == MsgType.ERROR:
                    status = "ERROR"
                elif isinstance(resp_envelope.payload, dict):
                    status = str(resp_envelope.payload.get("status", "OK"))

                log_request(
                    self.logger,
                    request_id=envelope.request_id,
                    client_id=self.client_id,
                    op_type=envelope.msg_type,
                    t_received=t_epoch_start,
                    t_completed=t_epoch_start + elapsed_s,
                    status=status,
                )
                return resp_envelope

            except zmq.Again:
                self.logger.warning(
                    "REQ timeout on %s (attempt %d/%d). Reconnecting socket...",
                    envelope.msg_type,
                    attempt,
                    max_retries,
                )
                self._reconnect_req()
                if attempt < max_retries:
                    time.sleep(backoff)
                    backoff = min(backoff * 2.0, 10.0)

            except zmq.ZMQError as exc:
                self.logger.warning(
                    "REQ ZMQError on %s: %s (attempt %d/%d). Reconnecting...",
                    envelope.msg_type,
                    exc,
                    attempt,
                    max_retries,
                )
                self._reconnect_req()
                if attempt < max_retries:
                    time.sleep(backoff)
                    backoff = min(backoff * 2.0, 10.0)

        # If all retries exhausted
        elapsed_s = time.perf_counter() - t_perf_start
        log_request(
            self.logger,
            request_id=envelope.request_id,
            client_id=self.client_id,
            op_type=envelope.msg_type,
            t_received=t_epoch_start,
            t_completed=t_epoch_start + elapsed_s,
            status="TIMEOUT",
            extra={"error": f"Failed after {max_retries} attempts"},
        )
        return None

    # ── Asynchronous SUB Listener ─────────────────────────────────────

    def _start_sub_listener(self) -> None:
        """Start background daemon thread listening for async cancel notifications."""
        self.sub_thread = threading.Thread(
            target=self._sub_listener_loop,
            name="sub-listener",
            daemon=True,
        )
        self.sub_thread.start()

    def _sub_listener_loop(self) -> None:
        """Process notifications arriving on the SUB socket."""
        self.logger.info("SUB listener thread started.")
        while self.running and self.sub_sock is not None:
            try:
                frames = self.sub_sock.recv_multipart()
                if not frames:
                    continue

                # Multipart format: [topic, envelope_json]
                # Single part format: "topic json_envelope" or raw json
                if len(frames) >= 2:
                    body = frames[1]
                else:
                    raw = frames[0]
                    prefix = self.client_id.encode("utf-8")
                    if raw.startswith(prefix):
                        body = raw[len(prefix):].strip()
                    else:
                        body = raw

                envelope = parse_envelope(body)
                self._handle_notification(envelope)

            except zmq.Again:
                continue
            except Exception as exc:
                if self.running:
                    self.logger.error("Error receiving on SUB socket: %s", exc)

    def _handle_notification(self, envelope: Envelope) -> None:
        """Handle received async notifications such as CANCEL_RESULT."""
        if envelope.msg_type == MsgType.CANCEL_RESULT:
            res_id = str(envelope.payload.get("reservation_id", ""))
            status = str(envelope.payload.get("status", "SUCCESS"))
            self.logger.info(
                "Received CANCEL_RESULT notification for %s, status=%s",
                res_id,
                status,
            )
            # Remove from local active reservations if successfully cancelled
            if status.upper() in ("CANCELLED", "SUCCESS", "OK", "ACCEPTED"):
                with self._res_lock:
                    self.active_reservations = [
                        r for r in self.active_reservations if r.reservation_id != res_id
                    ]
        else:
            self.logger.debug(
                "Received notification of type %s: %s",
                envelope.msg_type,
                envelope.payload,
            )

    # ── Main Operation Loop ───────────────────────────────────────────

    def _run_loop(self) -> None:
        """Continuously select and execute operations according to target rate."""
        ops = ["QUERY", "RESERVE", "MODIFY", "CANCEL"]
        weights = [self.pct_query, self.pct_reserve, self.pct_modify, self.pct_cancel]

        # First operation: always query events to populate local cache
        self.op_query()

        while self.running:
            t0 = time.perf_counter()
            chosen_op = self.rng.choices(ops, weights=weights, k=1)[0]

            if chosen_op == "QUERY":
                self.op_query()
            elif chosen_op == "RESERVE":
                self.op_reserve()
            elif chosen_op == "MODIFY":
                self.op_modify()
            elif chosen_op == "CANCEL":
                self.op_cancel()

            # Rate limiting sleep
            elapsed = time.perf_counter() - t0
            sleep_time = max(0.01, self.request_interval_s - elapsed)
            time.sleep(sleep_time)

    # ── Operations Implementation ─────────────────────────────────────

    def op_query(self) -> None:
        """
        Query events and occurrences for Config.SEED_MONTH.

        Sent synchronously via REQ to GT.
        """
        req = make_envelope(
            msg_type=MsgType.QUERY,
            payload={"month": Config.SEED_MONTH},
            client_id=self.client_id,
        )
        resp = self._send_req_with_retry(req)
        if resp and resp.msg_type != MsgType.ERROR:
            # Update known events cache
            events_data = resp.payload.get("events")
            if isinstance(events_data, list):
                self.known_events = events_data
            elif isinstance(resp.payload, list):
                self.known_events = resp.payload

    def op_reserve(self) -> None:
        """
        Reserve 1-5 seats for a randomly selected event occurrence.

        Sent synchronously via REQ to GT.
        """
        event_id, occ_date = self._pick_event_and_date()
        seats = self.rng.randint(1, 5)

        req = make_envelope(
            msg_type=MsgType.RESERVE,
            payload={
                "event_id": event_id,
                "occurrence_date": occ_date,
                "seats": seats,
            },
            client_id=self.client_id,
        )
        resp = self._send_req_with_retry(req)
        if resp and resp.msg_type != MsgType.ERROR:
            payload = resp.payload
            res_id = payload.get("reservation_id")
            status = payload.get("status", "OK")
            if res_id and status in ("OK", "SUCCESS", "RESERVED"):
                occ_id = str(payload.get("occurrence_id", ""))
                with self._res_lock:
                    self.active_reservations.append(
                        ActiveReservation(
                            reservation_id=str(res_id),
                            event_id=event_id,
                            occurrence_date=occ_date,
                            seats=seats,
                            occurrence_id=occ_id,
                        )
                    )

    def op_modify(self) -> None:
        """
        Modify an existing reservation (change seats or change occurrence date).

        Sent synchronously via REQ to GT.
        """
        with self._res_lock:
            if not self.active_reservations:
                # Fallback to RESERVE if no active reservation exists yet
                self.op_reserve()
                return
            target_res = self.rng.choice(self.active_reservations)

        # Decide whether to change seats (50%) or date (50%)
        change_seats = self.rng.choice([True, False])
        payload: dict[str, Any] = {"reservation_id": target_res.reservation_id}

        if change_seats:
            new_seats = self.rng.randint(1, 5)
            if new_seats == target_res.seats:
                new_seats = (new_seats % 5) + 1
            payload["new_seats"] = new_seats
        else:
            # Find an alternate occurrence date for the same event
            alt_date = self._find_alternate_date(
                target_res.event_id, target_res.occurrence_date
            )
            payload["new_occurrence_date"] = alt_date

        req = make_envelope(
            msg_type=MsgType.MODIFY,
            payload=payload,
            client_id=self.client_id,
        )
        resp = self._send_req_with_retry(req)
        if resp and resp.msg_type != MsgType.ERROR:
            status = resp.payload.get("status", "OK")
            if status in ("OK", "SUCCESS", "MODIFIED"):
                with self._res_lock:
                    # Update local state
                    for r in self.active_reservations:
                        if r.reservation_id == target_res.reservation_id:
                            if "new_seats" in payload:
                                r.seats = int(payload["new_seats"])
                            if "new_occurrence_date" in payload:
                                r.occurrence_date = str(payload["new_occurrence_date"])
                            break

    def op_cancel(self) -> None:
        """
        Cancel an existing reservation asynchronously.

        Sent via DEALER to GT. The result will arrive via SUB.
        """
        with self._res_lock:
            if not self.active_reservations:
                # Fallback to QUERY if no active reservations to cancel
                self.op_query()
                return
            target_res = self.rng.choice(self.active_reservations)

        req = make_envelope(
            msg_type=MsgType.CANCEL,
            payload={"reservation_id": target_res.reservation_id},
            client_id=self.client_id,
        )

        t_perf_start = time.perf_counter()
        t_epoch_start = time.time()
        try:
            assert self.dealer_sock is not None
            self.dealer_sock.send(req.to_bytes())

            # Attempt to read immediate ACK from GT router
            status = "ACCEPTED"
            if self.dealer_sock.poll(timeout=1000):
                reply_bytes = self.dealer_sock.recv()
                ack_envelope = parse_envelope(reply_bytes)
                if isinstance(ack_envelope.payload, dict):
                    status = str(ack_envelope.payload.get("status", "ACCEPTED"))

            elapsed_s = time.perf_counter() - t_perf_start
            log_request(
                self.logger,
                request_id=req.request_id,
                client_id=self.client_id,
                op_type=MsgType.CANCEL,
                t_received=t_epoch_start,
                t_completed=t_epoch_start + elapsed_s,
                status=status,
                extra={"reservation_id": target_res.reservation_id},
            )
        except Exception as exc:
            elapsed_s = time.perf_counter() - t_perf_start
            log_request(
                self.logger,
                request_id=req.request_id,
                client_id=self.client_id,
                op_type=MsgType.CANCEL,
                t_received=t_epoch_start,
                t_completed=t_epoch_start + elapsed_s,
                status="ERROR",
                extra={"error": str(exc)},
            )

    # ── Helpers ───────────────────────────────────────────────────────

    def _pick_event_and_date(self) -> tuple[str, str]:
        """Pick an event ID and date, prioritizing known events from QUERY."""
        if self.known_events:
            ev = self.rng.choice(self.known_events)
            event_id = str(ev.get("event_id", f"E{self.rng.randint(1, 50):02d}"))
            occurrences = ev.get("occurrences", [])
            if occurrences:
                occ = self.rng.choice(occurrences)
                date_str = str(occ.get("date", f"{Config.SEED_MONTH}-15"))
                return event_id, date_str

        # Fallback to random event and day within SEED_MONTH
        rand_id = f"E{self.rng.randint(1, 50):02d}"
        rand_day = self.rng.randint(1, 28)
        return rand_id, f"{Config.SEED_MONTH}-{rand_day:02d}"

    def _find_alternate_date(self, event_id: str, current_date: str) -> str:
        """Find a second occurrence date for the same event, or pick an alternate day."""
        for ev in self.known_events:
            if ev.get("event_id") == event_id:
                for occ in ev.get("occurrences", []):
                    occ_d = str(occ.get("date", ""))
                    if occ_d and occ_d != current_date:
                        return occ_d

        # Fallback alternate date in SEED_MONTH
        rand_day = self.rng.randint(1, 28)
        new_d = f"{Config.SEED_MONTH}-{rand_day:02d}"
        if new_d == current_date:
            new_d = f"{Config.SEED_MONTH}-{(rand_day % 28) + 1:02d}"
        return new_d


def main() -> None:
    """Entry point for SRB client pod."""
    client = SRBClient()

    def handle_signal(sig: int, _frame: Any) -> None:
        client.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    client.start()


if __name__ == "__main__":
    main()
