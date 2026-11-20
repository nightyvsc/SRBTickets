"""
Locust load testing suite for SRB (Sistema de Reserva de Boletos).

Custom ZeroMQ user connecting to the GT service via NodePort (default: localhost:30055).
Supports load testing scenarios:
- Scenario 3: 3 clients (2 query-only, 1 query+reserve)
- Scenario 5: 5 clients (3 query-only, 2 reserve+modify)
- Scenario 8: 8 clients (4 query-only, 4 reserve+modify)
- Custom / standard scenario: operation weights from environment variables

Usage:
    locust -f locust/locustfile.py
    LOCUST_SCENARIO=3 locust -f locust/locustfile.py --headless -u 3 -r 3 -t 2m
    LOCUST_SCENARIO=5 locust -f locust/locustfile.py --headless -u 5 -r 5 -t 2m
    LOCUST_SCENARIO=8 locust -f locust/locustfile.py --headless -u 8 -r 8 -t 2m
"""

from __future__ import annotations

import os
import random
import sys
import time
import uuid
from typing import Any

import zmq
from locust import User, between, task

# Ensure SRB root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import Config
from common.messages import Envelope, MsgType, make_envelope, parse_envelope


def get_gt_endpoint() -> str:
    """Format ZeroMQ target endpoint from GT_HOST environment variable."""
    raw = os.getenv("GT_HOST", "localhost:30055").strip()
    if raw.startswith("tcp://"):
        return raw
    if ":" in raw:
        return f"tcp://{raw}"
    return f"tcp://{raw}:30055"


class SRBZMQUser(User):
    """Base Locust User interacting with SRB GT service over ZeroMQ REQ socket."""

    abstract = True
    wait_time = between(0.1, 0.5)

    def on_start(self) -> None:
        """Initialize ZeroMQ context, REQ socket and local state."""
        self.client_id = f"locust-{uuid.uuid4().hex[:8]}"
        self.endpoint = get_gt_endpoint()
        self.timeout_ms = int(os.getenv("LOCUST_TIMEOUT_MS", "5000"))

        self.zmq_ctx = zmq.Context()
        self.sock: zmq.Socket | None = None
        self._connect_socket()

        # Local user state
        self.known_events: list[dict[str, Any]] = []
        self.active_reservations: list[dict[str, Any]] = []

    def on_stop(self) -> None:
        """Close socket and terminate ZMQ context on worker termination."""
        if self.sock is not None:
            try:
                self.sock.close(linger=0)
            except Exception:
                pass
        try:
            self.zmq_ctx.term()
        except Exception:
            pass

    def _connect_socket(self) -> None:
        """Create and connect the REQ socket."""
        if self.sock is not None:
            try:
                self.sock.close(linger=0)
            except Exception:
                pass
        self.sock = self.zmq_ctx.socket(zmq.REQ)
        self.sock.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        self.sock.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.connect(self.endpoint)

    def _send_request(self, op_name: str, envelope: Envelope) -> Envelope | None:
        """Send a request over REQ socket and report metrics to Locust."""
        t0 = time.perf_counter()
        resp_envelope: Envelope | None = None
        err: Exception | None = None
        resp_len = 0

        try:
            assert self.sock is not None
            data = envelope.to_bytes()
            self.sock.send(data)
            reply_bytes = self.sock.recv()
            resp_len = len(reply_bytes)

            resp_envelope = parse_envelope(reply_bytes)
            if resp_envelope.msg_type == MsgType.ERROR:
                err = Exception(
                    f"Server returned ERROR: {resp_envelope.payload.get('error', 'unknown')}"
                )
        except Exception as exc:
            err = exc
            # Reconnect REQ socket on failure to restore clean state
            self._connect_socket()
        finally:
            duration_ms = (time.perf_counter() - t0) * 1000.0
            self.environment.events.request.fire(
                request_type="ZMQ_REQ",
                name=op_name,
                response_time=duration_ms,
                response_length=resp_len,
                exception=err,
                context={"client_id": self.client_id},
            )

        return resp_envelope

    # ── Operations ────────────────────────────────────────────────────

    def execute_query(self) -> None:
        """Query available events for the configured month."""
        envelope = make_envelope(
            msg_type=MsgType.QUERY,
            payload={"month": Config.SEED_MONTH},
            client_id=self.client_id,
        )
        resp = self._send_request("query_events", envelope)
        if resp and resp.msg_type != MsgType.ERROR:
            events_data = resp.payload.get("events")
            if isinstance(events_data, list):
                self.known_events = events_data
            elif isinstance(resp.payload, list):
                self.known_events = resp.payload

    def execute_reserve(self) -> None:
        """Reserve 1-5 seats for an event occurrence."""
        event_id, occ_date = self._pick_event_and_date()
        seats = random.randint(1, 5)

        envelope = make_envelope(
            msg_type=MsgType.RESERVE,
            payload={
                "event_id": event_id,
                "occurrence_date": occ_date,
                "seats": seats,
            },
            client_id=self.client_id,
        )
        resp = self._send_request("reserve_seats", envelope)
        if resp and resp.msg_type != MsgType.ERROR:
            payload = resp.payload
            res_id = payload.get("reservation_id")
            status = payload.get("status", "OK")
            if res_id and status in ("OK", "SUCCESS", "RESERVED"):
                self.active_reservations.append(
                    {
                        "reservation_id": str(res_id),
                        "event_id": event_id,
                        "occurrence_date": occ_date,
                        "seats": seats,
                    }
                )

    def execute_modify(self) -> None:
        """Modify an existing reservation: change seats or date."""
        if not self.active_reservations:
            self.execute_reserve()
            return

        target = random.choice(self.active_reservations)
        payload: dict[str, Any] = {"reservation_id": target["reservation_id"]}

        if random.choice([True, False]):
            # Change seats
            new_seats = random.randint(1, 5)
            if new_seats == target["seats"]:
                new_seats = (new_seats % 5) + 1
            payload["new_seats"] = new_seats
        else:
            # Change date
            payload["new_occurrence_date"] = self._find_alternate_date(
                target["event_id"], target["occurrence_date"]
            )

        envelope = make_envelope(
            msg_type=MsgType.MODIFY,
            payload=payload,
            client_id=self.client_id,
        )
        resp = self._send_request("modify_reservation", envelope)
        if resp and resp.msg_type != MsgType.ERROR:
            status = resp.payload.get("status", "OK")
            if status in ("OK", "SUCCESS", "MODIFIED"):
                for r in self.active_reservations:
                    if r["reservation_id"] == target["reservation_id"]:
                        if "new_seats" in payload:
                            r["seats"] = payload["new_seats"]
                        if "new_occurrence_date" in payload:
                            r["occurrence_date"] = payload["new_occurrence_date"]
                        break

    def execute_cancel(self) -> None:
        """Cancel an existing reservation."""
        if not self.active_reservations:
            self.execute_query()
            return

        target = random.choice(self.active_reservations)
        envelope = make_envelope(
            msg_type=MsgType.CANCEL,
            payload={"reservation_id": target["reservation_id"]},
            client_id=self.client_id,
        )
        resp = self._send_request("cancel_reservation", envelope)
        if resp and resp.msg_type != MsgType.ERROR:
            self.active_reservations = [
                r
                for r in self.active_reservations
                if r["reservation_id"] != target["reservation_id"]
            ]

    # ── Helpers ───────────────────────────────────────────────────────

    def _pick_event_and_date(self) -> tuple[str, str]:
        """Pick an event ID and date from known events or fallback."""
        if self.known_events:
            ev = random.choice(self.known_events)
            event_id = str(ev.get("event_id", f"E{random.randint(1, 50):02d}"))
            occurrences = ev.get("occurrences", [])
            if occurrences:
                occ = random.choice(occurrences)
                date_str = str(occ.get("date", f"{Config.SEED_MONTH}-15"))
                return event_id, date_str

        rand_id = f"E{random.randint(1, 50):02d}"
        rand_day = random.randint(1, 28)
        return rand_id, f"{Config.SEED_MONTH}-{rand_day:02d}"

    def _find_alternate_date(self, event_id: str, current_date: str) -> str:
        """Find an alternate occurrence date for the given event."""
        for ev in self.known_events:
            if ev.get("event_id") == event_id:
                for occ in ev.get("occurrences", []):
                    occ_d = str(occ.get("date", ""))
                    if occ_d and occ_d != current_date:
                        return occ_d
        rand_day = random.randint(1, 28)
        new_d = f"{Config.SEED_MONTH}-{rand_day:02d}"
        if new_d == current_date:
            new_d = f"{Config.SEED_MONTH}-{(rand_day % 28) + 1:02d}"
        return new_d


# ── Scenario-Specific User Classes ────────────────────────────────────


class QueryOnlyUser(SRBZMQUser):
    """User that exclusively performs query operations."""

    @task
    def query(self) -> None:
        self.execute_query()


class QueryReserveUser(SRBZMQUser):
    """User that performs queries and reservations."""

    @task(50)
    def query(self) -> None:
        self.execute_query()

    @task(50)
    def reserve(self) -> None:
        self.execute_reserve()


class ReserveModifyUser(SRBZMQUser):
    """User that performs reservations and modifications."""

    @task(50)
    def reserve(self) -> None:
        self.execute_reserve()

    @task(50)
    def modify(self) -> None:
        self.execute_modify()


class StandardUser(SRBZMQUser):
    """User with configurable task weights from environment variables."""

    q_weight = int(os.getenv("LOCUST_QUERY_WEIGHT", "40"))
    r_weight = int(os.getenv("LOCUST_RESERVE_WEIGHT", "30"))
    m_weight = int(os.getenv("LOCUST_MODIFY_WEIGHT", "20"))
    c_weight = int(os.getenv("LOCUST_CANCEL_WEIGHT", "10"))

    @task(q_weight)
    def query(self) -> None:
        self.execute_query()

    @task(r_weight)
    def reserve(self) -> None:
        self.execute_reserve()

    @task(m_weight)
    def modify(self) -> None:
        self.execute_modify()

    @task(c_weight)
    def cancel(self) -> None:
        self.execute_cancel()


# ── Scenario Configuration ────────────────────────────────────────────

scenario = os.getenv("LOCUST_SCENARIO", "").strip().lower()

if scenario == "3":
    # 3 clients: 2 solo consulta, 1 consulta+reserva
    QueryOnlyUser.weight = 2
    QueryReserveUser.weight = 1
    ReserveModifyUser.weight = 0
    StandardUser.weight = 0
elif scenario == "5":
    # 5 clients: 3 solo consulta, 2 reserva+modificación
    QueryOnlyUser.weight = 3
    QueryReserveUser.weight = 0
    ReserveModifyUser.weight = 2
    StandardUser.weight = 0
elif scenario == "8":
    # 8 clients: 4 solo consulta, 4 reserva+modificación
    QueryOnlyUser.weight = 4
    QueryReserveUser.weight = 0
    ReserveModifyUser.weight = 4
    StandardUser.weight = 0
else:
    # Default: use StandardUser with env weights
    QueryOnlyUser.weight = 0
    QueryReserveUser.weight = 0
    ReserveModifyUser.weight = 0
    StandardUser.weight = 1
