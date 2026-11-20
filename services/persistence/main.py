"""
Persistence Service for the SRB (Sistema de Reserva de Boletos) ticket reservation system.

Responsibilities:
- Binds a ZeroMQ REP socket on PERSIST_PORT (default 5580).
- Receives DB_QUERY and DB_EXECUTE messages from RMC workers and the Cancel service.
- Executes SQL operations against CockroachDB using psycopg v3 and psycopg_pool.ConnectionPool.
- Guarantees atomic transaction isolation with CockroachDB retry handling.
- Returns DB_RESPONSE envelopes with execution results.
- Runs an HTTP HealthServer on HEALTH_PORT (default 8100) for K8s liveness probes.
- Logs structured JSON metrics with operation timing via log_request().
"""

from __future__ import annotations

import logging
import os
import random
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg
from psycopg_pool import ConnectionPool
import zmq

from common.config import Config
from common.health import HealthServer
from common.logging_json import get_logger, log_request
from common.messages import Envelope, MsgType, make_envelope, parse_envelope


class RollbackException(Exception):
    """Exception raised inside transaction callback to force rollback with a rejection response."""

    def __init__(self, result: dict[str, Any]) -> None:
        super().__init__(str(result))
        self.result = result


def _to_uuid(val: Any) -> uuid.UUID:
    """Convert input to UUID or raise ValueError."""
    if isinstance(val, uuid.UUID):
        return val
    return uuid.UUID(str(val))


class PersistenceService:
    """ZeroMQ REP service executing CockroachDB operations for SRB components."""

    def __init__(
        self,
        conninfo: str | None = None,
        port: int | None = None,
        health_port: int | None = None,
    ) -> None:
        """Initialize the Persistence Service configuration and state."""
        self._conninfo = conninfo or os.getenv("CRDB_CONNSTR", Config.crdb_connstr())
        self._port = port or Config.PERSIST_PORT
        self._health_port = health_port or Config.HEALTH_PORT

        self._logger = get_logger("services.persistence")
        self._pool: ConnectionPool | None = None
        self._health_server: HealthServer | None = None

        self._context: zmq.Context | None = None
        self._socket: zmq.Socket | None = None

        self._is_running: bool = False
        self._is_ready: bool = False
        self._shutdown_event: threading.Event = threading.Event()

    # ── Database Initialization & Health ──────────────────────────────

    def _init_schema(self, conn: psycopg.Connection) -> None:
        """Ensure all required tables and indexes exist in CockroachDB."""
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        event_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        month CHAR(7) NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS occurrences (
                        occurrence_id UUID PRIMARY KEY,
                        event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
                        date DATE NOT NULL,
                        capacity INT NOT NULL,
                        free_seats INT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS reservations (
                        reservation_id UUID PRIMARY KEY,
                        client_id TEXT NOT NULL,
                        occurrence_id UUID NOT NULL REFERENCES occurrences(occurrence_id) ON DELETE CASCADE,
                        seats INT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_events_month ON events(month);
                    CREATE INDEX IF NOT EXISTS idx_occurrences_event_id ON occurrences(event_id);
                    CREATE INDEX IF NOT EXISTS idx_reservations_occurrence_id ON reservations(occurrence_id);
                    CREATE INDEX IF NOT EXISTS idx_reservations_client_id ON reservations(client_id);
                    """
                )
        self._logger.info("Database schema verified / initialized.")

    def wait_for_db(self, max_retries: int = 60, retry_delay: float = 2.0) -> None:
        """Wait for CockroachDB to be ready and verify connectivity before starting."""
        attempt = 0
        while not self._shutdown_event.is_set():
            attempt += 1
            try:
                self._logger.info(
                    f"Checking CockroachDB connection (attempt {attempt})..."
                )
                with psycopg.connect(self._conninfo, connect_timeout=5) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1;")
                    self._init_schema(conn)
                self._logger.info("Successfully connected to CockroachDB.")
                return
            except Exception as exc:
                self._logger.warning(
                    f"CockroachDB not ready yet (attempt {attempt}): {exc}. Retrying in {retry_delay}s..."
                )
                if attempt >= max_retries:
                    self._logger.error(
                        f"Exceeded maximum retries ({max_retries}) connecting to CockroachDB."
                    )
                    raise
                time.sleep(retry_delay)

    def _init_pool(self) -> None:
        """Initialize the psycopg connection pool."""
        min_size = int(os.getenv("DB_POOL_MIN_SIZE", "5"))
        max_size = int(os.getenv("DB_POOL_MAX_SIZE", "20"))
        timeout = float(os.getenv("DB_POOL_TIMEOUT", "10.0"))

        self._logger.info(
            f"Initializing ConnectionPool (min_size={min_size}, max_size={max_size}, timeout={timeout}s)..."
        )
        self._pool = ConnectionPool(
            conninfo=self._conninfo,
            min_size=min_size,
            max_size=max_size,
            timeout=timeout,
            open=True,
        )
        self._logger.info("ConnectionPool is open and ready.")

    def is_healthy(self) -> bool:
        """Health-check function invoked by HealthServer for /healthz probe."""
        if not self._is_ready or not self._is_running or self._pool is None:
            return False
        try:
            with self._pool.connection(timeout=2.0) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            return True
        except Exception as exc:
            self._logger.warning(f"Health check probe failed: {exc}")
            return False

    # ── Transaction Execution Wrappers ────────────────────────────────

    def _execute_transaction_with_retry(
        self,
        fn: Callable[[psycopg.Cursor], dict[str, Any]],
        max_retries: int = 5,
        initial_backoff: float = 0.05,
    ) -> dict[str, Any]:
        """
        Execute fn inside an atomic transaction with automatic CockroachDB retry on 40001 contention.

        If fn raises RollbackException, the transaction is rolled back and the rejection
        payload is returned without retrying.
        """
        if self._pool is None:
            return {"status": "ERROR", "reason": "database pool not initialized"}

        backoff = initial_backoff
        for attempt in range(max_retries):
            try:
                with self._pool.connection() as conn:
                    with conn.transaction():
                        with conn.cursor() as cur:
                            return fn(cur)
            except RollbackException as r_exc:
                return r_exc.result
            except psycopg.errors.SerializationFailure as s_exc:
                if attempt >= max_retries - 1:
                    self._logger.error(
                        f"Serialization failure after {max_retries} attempts: {s_exc}"
                    )
                    return {
                        "status": "ERROR",
                        "reason": f"Transaction contention limit reached: {s_exc}",
                    }
                sleep_time = backoff * (1.5**attempt) + random.uniform(0.005, 0.02)
                self._logger.warning(
                    f"CockroachDB SerializationFailure (attempt {attempt + 1}/{max_retries}): {s_exc}. "
                    f"Retrying in {sleep_time:.3f}s..."
                )
                time.sleep(sleep_time)
            except Exception as exc:
                self._logger.error(f"Database error during transaction: {exc}")
                return {"status": "ERROR", "reason": f"Database error: {exc}"}

        return {"status": "ERROR", "reason": "transaction retry limit reached"}

    def _execute_readonly_with_retry(
        self,
        fn: Callable[[psycopg.Cursor], dict[str, Any]],
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """Execute a read-only query using a pooled connection with transient failure retry."""
        if self._pool is None:
            return {"status": "ERROR", "reason": "database pool not initialized"}

        for attempt in range(max_retries):
            try:
                with self._pool.connection() as conn:
                    with conn.cursor() as cur:
                        return fn(cur)
            except psycopg.errors.SerializationFailure as s_exc:
                if attempt >= max_retries - 1:
                    return {"status": "ERROR", "reason": f"Read query conflict: {s_exc}"}
                time.sleep(0.02 * (2**attempt))
            except Exception as exc:
                self._logger.error(f"Database error during read query: {exc}")
                return {"status": "ERROR", "reason": f"Database error: {exc}"}

        return {"status": "ERROR", "reason": "read query failed"}

    # ── Database Operations ───────────────────────────────────────────

    def query_events(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Query all events and occurrences for a given month.

        payload: {operation: 'QUERY_EVENTS', month: 'YYYY-MM'}
        returns: {events: [{event_id, name, occurrences: [{occurrence_id, date, capacity, free_seats}]}]}
        """
        month = payload.get("month", "")
        if not month:
            return {"status": "ERROR", "reason": "missing month parameter", "events": []}

        def _query(cur: psycopg.Cursor) -> dict[str, Any]:
            cur.execute(
                """
                SELECT e.event_id, e.name, o.occurrence_id, o.date, o.capacity, o.free_seats
                FROM events e
                JOIN occurrences o ON e.event_id = o.event_id
                WHERE e.month = %(month)s
                ORDER BY e.event_id, o.date;
                """,
                {"month": month},
            )
            rows = cur.fetchall()
            events_map: dict[str, dict[str, Any]] = {}
            for row in rows:
                ev_id = str(row[0])
                ev_name = str(row[1])
                occ_id = str(row[2])
                occ_date = row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3])
                capacity = int(row[4])
                free_seats = int(row[5])

                if ev_id not in events_map:
                    events_map[ev_id] = {
                        "event_id": ev_id,
                        "name": ev_name,
                        "occurrences": [],
                    }
                events_map[ev_id]["occurrences"].append(
                    {
                        "occurrence_id": occ_id,
                        "date": occ_date,
                        "capacity": capacity,
                        "free_seats": free_seats,
                    }
                )

            return {
                "status": "OK",
                "events": list(events_map.values()),
            }

        return self._execute_readonly_with_retry(_query)

    def reserve(self, payload: dict[str, Any], client_id: str) -> dict[str, Any]:
        """
        Atomically reserve seats for an occurrence.

        payload: {operation: 'RESERVE', occurrence_id: uuid, seats: int, client_id: str}
        OR:      {operation: 'RESERVE', event_id: str, occurrence_date: str, seats: int, client_id: str}
        returns: {status: 'OK', reservation_id: uuid} or {status: 'REJECTED', reason: 'insufficient seats'}
        """
        raw_occ_id = payload.get("occurrence_id")
        event_id = payload.get("event_id")
        occ_date = payload.get("occurrence_date") or payload.get("date")

        if not raw_occ_id and (not event_id or not occ_date):
            return {"status": "REJECTED", "reason": "missing occurrence_id or (event_id and occurrence_date)"}

        occ_uuid: uuid.UUID | None = None
        if raw_occ_id:
            try:
                occ_uuid = _to_uuid(raw_occ_id)
            except ValueError:
                return {"status": "REJECTED", "reason": f"invalid occurrence_id format: {raw_occ_id}"}

        try:
            seats = int(payload.get("seats", 0))
        except (ValueError, TypeError):
            return {"status": "REJECTED", "reason": "invalid seats count"}

        if seats <= 0:
            return {"status": "REJECTED", "reason": "seats must be greater than zero"}

        cid = str(payload.get("client_id") or client_id or "unknown")

        def _txn(cur: psycopg.Cursor) -> dict[str, Any]:
            target_occ_id = occ_uuid
            if target_occ_id is None:
                cur.execute(
                    "SELECT occurrence_id FROM occurrences WHERE event_id = %(event_id)s AND date = %(date)s;",
                    {"event_id": str(event_id), "date": str(occ_date)},
                )
                row = cur.fetchone()
                if row is None:
                    raise RollbackException({"status": "REJECTED", "reason": "occurrence not found"})
                target_occ_id = row[0]

            cur.execute(
                """
                UPDATE occurrences
                SET free_seats = free_seats - %(seats)s
                WHERE occurrence_id = %(occurrence_id)s AND free_seats >= %(seats)s;
                """,
                {"seats": seats, "occurrence_id": target_occ_id},
            )
            if cur.rowcount != 1:
                cur.execute(
                    "SELECT 1 FROM occurrences WHERE occurrence_id = %(occurrence_id)s;",
                    {"occurrence_id": target_occ_id},
                )
                if cur.fetchone() is None:
                    raise RollbackException({"status": "REJECTED", "reason": "occurrence not found"})
                raise RollbackException({"status": "REJECTED", "reason": "insufficient seats"})

            res_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            cur.execute(
                """
                INSERT INTO reservations (reservation_id, client_id, occurrence_id, seats, status, created_at, updated_at)
                VALUES (%(reservation_id)s, %(client_id)s, %(occurrence_id)s, %(seats)s, 'ACTIVE', %(created_at)s, %(updated_at)s);
                """,
                {
                    "reservation_id": _to_uuid(res_id),
                    "client_id": cid,
                    "occurrence_id": target_occ_id,
                    "seats": seats,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            return {"status": "OK", "reservation_id": res_id}

        return self._execute_transaction_with_retry(_txn)

    def modify_seats(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Atomically modify the number of seats for an active reservation.

        payload: {operation: 'MODIFY_SEATS', reservation_id: uuid, new_seats: int}
        returns: {status: 'OK'} or {status: 'REJECTED', reason: ...}
        """
        raw_res_id = payload.get("reservation_id")
        if not raw_res_id:
            return {"status": "REJECTED", "reason": "missing reservation_id"}

        try:
            res_uuid = _to_uuid(raw_res_id)
        except ValueError:
            return {"status": "REJECTED", "reason": f"invalid reservation_id format: {raw_res_id}"}

        try:
            new_seats = int(payload.get("new_seats", 0))
        except (ValueError, TypeError):
            return {"status": "REJECTED", "reason": "invalid new_seats value"}

        if new_seats <= 0:
            return {"status": "REJECTED", "reason": "new_seats must be greater than zero"}

        def _txn(cur: psycopg.Cursor) -> dict[str, Any]:
            cur.execute(
                """
                SELECT occurrence_id, seats, status
                FROM reservations
                WHERE reservation_id = %(reservation_id)s
                FOR UPDATE;
                """,
                {"reservation_id": res_uuid},
            )
            row = cur.fetchone()
            if row is None:
                raise RollbackException({"status": "REJECTED", "reason": "reservation not found"})

            occurrence_id, old_seats, status = row[0], int(row[1]), str(row[2])
            if status != "ACTIVE":
                raise RollbackException(
                    {"status": "REJECTED", "reason": f"reservation is not active (status: {status})"}
                )

            diff = new_seats - old_seats
            now = datetime.now(timezone.utc)

            if diff > 0:
                cur.execute(
                    """
                    UPDATE occurrences
                    SET free_seats = free_seats - %(diff)s
                    WHERE occurrence_id = %(occurrence_id)s AND free_seats >= %(diff)s;
                    """,
                    {"diff": diff, "occurrence_id": occurrence_id},
                )
                if cur.rowcount != 1:
                    raise RollbackException({"status": "REJECTED", "reason": "insufficient seats"})
            elif diff < 0:
                abs_diff = abs(diff)
                cur.execute(
                    """
                    UPDATE occurrences
                    SET free_seats = free_seats + %(abs_diff)s
                    WHERE occurrence_id = %(occurrence_id)s;
                    """,
                    {"abs_diff": abs_diff, "occurrence_id": occurrence_id},
                )

            cur.execute(
                """
                UPDATE reservations
                SET seats = %(new_seats)s, updated_at = %(updated_at)s
                WHERE reservation_id = %(reservation_id)s;
                """,
                {
                    "new_seats": new_seats,
                    "updated_at": now,
                    "reservation_id": res_uuid,
                },
            )
            return {"status": "OK"}

        return self._execute_transaction_with_retry(_txn)

    def modify_date(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Atomically move an active reservation to another occurrence of the same event and month.

        payload: {operation: 'MODIFY_DATE', reservation_id: uuid, new_occurrence_id: uuid}
        returns: {status: 'OK'} or {status: 'REJECTED', reason: ...}
        """
        raw_res_id = payload.get("reservation_id")
        raw_new_occ_id = payload.get("new_occurrence_id")
        if not raw_res_id or not raw_new_occ_id:
            return {"status": "REJECTED", "reason": "missing reservation_id or new_occurrence_id"}

        try:
            res_uuid = _to_uuid(raw_res_id)
        except ValueError:
            return {"status": "REJECTED", "reason": f"invalid reservation_id format: {raw_res_id}"}

        try:
            new_occ_uuid = _to_uuid(raw_new_occ_id)
        except ValueError:
            return {"status": "REJECTED", "reason": f"invalid new_occurrence_id format: {raw_new_occ_id}"}

        def _txn(cur: psycopg.Cursor) -> dict[str, Any]:
            # 1. Fetch current reservation, old occurrence, and event
            cur.execute(
                """
                SELECT r.occurrence_id, r.seats, r.status, o.event_id, e.month
                FROM reservations r
                JOIN occurrences o ON r.occurrence_id = o.occurrence_id
                JOIN events e ON o.event_id = e.event_id
                WHERE r.reservation_id = %(reservation_id)s
                FOR UPDATE;
                """,
                {"reservation_id": res_uuid},
            )
            row = cur.fetchone()
            if row is None:
                raise RollbackException({"status": "REJECTED", "reason": "reservation not found"})

            old_occ_id, seats, status, old_event_id, old_month = (
                row[0],
                int(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
            )

            if status != "ACTIVE":
                raise RollbackException(
                    {"status": "REJECTED", "reason": f"reservation is not active (status: {status})"}
                )

            if str(old_occ_id) == str(new_occ_uuid):
                raise RollbackException(
                    {"status": "REJECTED", "reason": "reservation is already for this occurrence"}
                )

            # 2. Verify new occurrence is of same event and month
            cur.execute(
                """
                SELECT o.occurrence_id, o.event_id, e.month
                FROM occurrences o
                JOIN events e ON o.event_id = e.event_id
                WHERE o.occurrence_id = %(new_occurrence_id)s;
                """,
                {"new_occurrence_id": new_occ_uuid},
            )
            new_row = cur.fetchone()
            if new_row is None:
                raise RollbackException({"status": "REJECTED", "reason": "new occurrence not found"})

            new_event_id, new_month = str(new_row[1]), str(new_row[2])
            if new_event_id != old_event_id:
                raise RollbackException(
                    {"status": "REJECTED", "reason": "new occurrence belongs to different event"}
                )
            if new_month != old_month:
                raise RollbackException(
                    {"status": "REJECTED", "reason": "new occurrence belongs to different month"}
                )

            # 3. Release seats on old occurrence
            cur.execute(
                """
                UPDATE occurrences
                SET free_seats = free_seats + %(seats)s
                WHERE occurrence_id = %(old_occurrence_id)s;
                """,
                {"seats": seats, "old_occurrence_id": old_occ_id},
            )

            # 4. Take seats on new occurrence
            cur.execute(
                """
                UPDATE occurrences
                SET free_seats = free_seats - %(seats)s
                WHERE occurrence_id = %(new_occurrence_id)s AND free_seats >= %(seats)s;
                """,
                {"seats": seats, "new_occurrence_id": new_occ_uuid},
            )
            if cur.rowcount != 1:
                raise RollbackException(
                    {"status": "REJECTED", "reason": "insufficient seats on new occurrence"}
                )

            # 5. Update reservation occurrence and timestamp
            now = datetime.now(timezone.utc)
            cur.execute(
                """
                UPDATE reservations
                SET occurrence_id = %(new_occurrence_id)s, updated_at = %(updated_at)s
                WHERE reservation_id = %(reservation_id)s;
                """,
                {
                    "new_occurrence_id": new_occ_uuid,
                    "updated_at": now,
                    "reservation_id": res_uuid,
                },
            )
            return {"status": "OK"}

        return self._execute_transaction_with_retry(_txn)

    def cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Atomically cancel an active reservation and restore occurrence seats.

        payload: {operation: 'CANCEL', reservation_id: uuid}
        returns: {status: 'OK', seats_freed: int}
        """
        raw_res_id = payload.get("reservation_id")
        if not raw_res_id:
            return {"status": "REJECTED", "reason": "missing reservation_id"}

        try:
            res_uuid = _to_uuid(raw_res_id)
        except ValueError:
            return {"status": "REJECTED", "reason": f"invalid reservation_id format: {raw_res_id}"}

        def _txn(cur: psycopg.Cursor) -> dict[str, Any]:
            cur.execute(
                """
                SELECT occurrence_id, seats, status
                FROM reservations
                WHERE reservation_id = %(reservation_id)s
                FOR UPDATE;
                """,
                {"reservation_id": res_uuid},
            )
            row = cur.fetchone()
            if row is None:
                raise RollbackException({"status": "REJECTED", "reason": "reservation not found"})

            occurrence_id, seats, status = row[0], int(row[1]), str(row[2])
            if status != "ACTIVE":
                if status == "CANCELLED":
                    raise RollbackException(
                        {"status": "REJECTED", "reason": "reservation already cancelled"}
                    )
                raise RollbackException(
                    {"status": "REJECTED", "reason": f"reservation is not active (status: {status})"}
                )

            # Release seats back to occurrence
            cur.execute(
                """
                UPDATE occurrences
                SET free_seats = free_seats + %(seats)s
                WHERE occurrence_id = %(occurrence_id)s;
                """,
                {"seats": seats, "occurrence_id": occurrence_id},
            )

            now = datetime.now(timezone.utc)
            cur.execute(
                """
                UPDATE reservations
                SET status = 'CANCELLED', updated_at = %(updated_at)s
                WHERE reservation_id = %(reservation_id)s;
                """,
                {
                    "updated_at": now,
                    "reservation_id": res_uuid,
                },
            )
            return {"status": "OK", "seats_freed": seats}

        return self._execute_transaction_with_retry(_txn)

    def get_reservation(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Retrieve reservation details.

        payload: {operation: 'GET_RESERVATION', reservation_id: uuid}
        returns: {reservation_id, client_id, occurrence_id, seats, status, event_id}
        """
        raw_res_id = payload.get("reservation_id")
        if not raw_res_id:
            return {
                "status": "NOT_FOUND",
                "reason": "missing reservation_id",
                "reservation_id": None,
            }

        try:
            res_uuid = _to_uuid(raw_res_id)
        except ValueError:
            return {
                "status": "NOT_FOUND",
                "reason": f"invalid reservation_id format: {raw_res_id}",
                "reservation_id": None,
            }

        def _query(cur: psycopg.Cursor) -> dict[str, Any]:
            cur.execute(
                """
                SELECT r.reservation_id, r.client_id, r.occurrence_id, r.seats, r.status, o.event_id
                FROM reservations r
                JOIN occurrences o ON r.occurrence_id = o.occurrence_id
                WHERE r.reservation_id = %(reservation_id)s;
                """,
                {"reservation_id": res_uuid},
            )
            row = cur.fetchone()
            if row is None:
                return {
                    "status": "NOT_FOUND",
                    "reason": "reservation not found",
                    "reservation_id": None,
                }

            return {
                "status": str(row[4]),
                "reservation_id": str(row[0]),
                "client_id": str(row[1]),
                "occurrence_id": str(row[2]),
                "seats": int(row[3]),
                "event_id": str(row[5]),
            }

        return self._execute_readonly_with_retry(_query)

    def get_occurrence_info(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Retrieve details and seat counts for an occurrence.

        payload: {operation: 'GET_OCCURRENCE' | 'GET_OCCURRENCE_INFO', occurrence_id: uuid}
        OR:      {operation: 'GET_OCCURRENCE', event_id: str, date: str}
        returns: {occurrence_id, event_id, date, capacity, free_seats, month, occurrence: {...}}
        """
        raw_occ_id = payload.get("occurrence_id")
        event_id = payload.get("event_id")
        raw_date = payload.get("date") or payload.get("occurrence_date")

        if not raw_occ_id and (not event_id or not raw_date):
            return {
                "status": "NOT_FOUND",
                "reason": "missing occurrence_id or (event_id and date)",
                "occurrence_id": None,
            }

        occ_uuid: uuid.UUID | None = None
        if raw_occ_id:
            try:
                occ_uuid = _to_uuid(raw_occ_id)
            except ValueError:
                return {
                    "status": "NOT_FOUND",
                    "reason": f"invalid occurrence_id format: {raw_occ_id}",
                    "occurrence_id": None,
                }

        def _query(cur: psycopg.Cursor) -> dict[str, Any]:
            if occ_uuid is not None:
                cur.execute(
                    """
                    SELECT o.occurrence_id, o.event_id, o.date, o.capacity, o.free_seats, e.month
                    FROM occurrences o
                    JOIN events e ON o.event_id = e.event_id
                    WHERE o.occurrence_id = %(occurrence_id)s;
                    """,
                    {"occurrence_id": occ_uuid},
                )
            else:
                cur.execute(
                    """
                    SELECT o.occurrence_id, o.event_id, o.date, o.capacity, o.free_seats, e.month
                    FROM occurrences o
                    JOIN events e ON o.event_id = e.event_id
                    WHERE o.event_id = %(event_id)s AND o.date = %(date)s;
                    """,
                    {"event_id": str(event_id), "date": str(raw_date)},
                )
            row = cur.fetchone()
            if row is None:
                return {
                    "status": "NOT_FOUND",
                    "reason": "occurrence not found",
                    "occurrence_id": None,
                }

            occ_data = {
                "occurrence_id": str(row[0]),
                "event_id": str(row[1]),
                "date": row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2]),
                "capacity": int(row[3]),
                "free_seats": int(row[4]),
                "month": str(row[5]),
            }
            return {
                "status": "OK",
                "occurrence": occ_data,
                **occ_data,
            }

        return self._execute_readonly_with_retry(_query)

    def get_other_occurrence(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Find an alternative occurrence for the same event excluding a specified occurrence.

        payload: {operation: 'GET_OTHER_OCCURRENCE', event_id: str, exclude_occurrence_id: uuid}
        returns: {occurrence_id, date, free_seats} or {occurrence_id: null} if none
        """
        event_id = payload.get("event_id")
        if not event_id:
            return {
                "status": "REJECTED",
                "reason": "missing event_id",
                "occurrence_id": None,
            }

        raw_exclude_id = payload.get("exclude_occurrence_id")
        exclude_uuid: uuid.UUID | None = None
        if raw_exclude_id:
            try:
                exclude_uuid = _to_uuid(raw_exclude_id)
            except ValueError:
                pass

        def _query(cur: psycopg.Cursor) -> dict[str, Any]:
            if exclude_uuid is not None:
                cur.execute(
                    """
                    SELECT o.occurrence_id, o.date, o.free_seats
                    FROM occurrences o
                    WHERE o.event_id = %(event_id)s AND o.occurrence_id != %(exclude_id)s
                    ORDER BY o.date
                    LIMIT 1;
                    """,
                    {"event_id": str(event_id), "exclude_id": exclude_uuid},
                )
            else:
                cur.execute(
                    """
                    SELECT o.occurrence_id, o.date, o.free_seats
                    FROM occurrences o
                    WHERE o.event_id = %(event_id)s
                    ORDER BY o.date
                    LIMIT 1;
                    """,
                    {"event_id": str(event_id)},
                )

            row = cur.fetchone()
            if row is None:
                return {
                    "status": "OK",
                    "occurrence_id": None,
                    "date": None,
                    "free_seats": None,
                }

            return {
                "status": "OK",
                "occurrence_id": str(row[0]),
                "date": row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1]),
                "free_seats": int(row[2]),
            }

        return self._execute_readonly_with_retry(_query)

    # ── Operation Dispatcher & Message Handling ───────────────────────

    def dispatch_operation(
        self, op: str, payload: dict[str, Any], client_id: str
    ) -> dict[str, Any]:
        """Route an operation name to its corresponding service method."""
        match op:
            case "QUERY_EVENTS":
                return self.query_events(payload)
            case "RESERVE":
                return self.reserve(payload, client_id)
            case "MODIFY_SEATS":
                return self.modify_seats(payload)
            case "MODIFY_DATE":
                return self.modify_date(payload)
            case "CANCEL":
                return self.cancel(payload)
            case "GET_RESERVATION":
                return self.get_reservation(payload)
            case "GET_OCCURRENCE" | "GET_OCCURRENCE_INFO":
                return self.get_occurrence_info(payload)
            case "GET_OTHER_OCCURRENCE":
                return self.get_other_occurrence(payload)
            case "PING":
                return {"status": "OK", "message": "pong"}
            case _:
                return {"status": "ERROR", "reason": f"Unknown operation: '{op}'"}

    def handle_message(self, raw_msg: bytes, t_received: float) -> Envelope:
        """
        Parse incoming ZeroMQ frame, execute requested DB operation, log metrics, and return DB_RESPONSE.
        """
        try:
            env = parse_envelope(raw_msg)
        except Exception as exc:
            t_completed = time.perf_counter()
            self._logger.error(f"Failed to parse incoming envelope: {exc}")
            log_request(
                self._logger,
                request_id="unknown",
                client_id="unknown",
                op_type="PARSE_ERROR",
                t_received=t_received,
                t_completed=t_completed,
                status="ERROR",
                extra={"error": str(exc)},
            )
            return make_envelope(
                MsgType.ERROR,
                payload={"status": "ERROR", "reason": f"Malformed envelope: {exc}"},
            )

        req_id = env.request_id
        client_id = env.client_id
        payload = env.payload if isinstance(env.payload, dict) else {}
        op = str(payload.get("operation", "")).strip()

        if not op:
            t_completed = time.perf_counter()
            result = {"status": "ERROR", "reason": "Missing 'operation' in payload"}
            log_request(
                self._logger,
                request_id=req_id,
                client_id=client_id,
                op_type="UNKNOWN",
                t_received=t_received,
                t_completed=t_completed,
                status="ERROR",
                extra={"reason": "Missing operation in payload"},
            )
            return make_envelope(
                MsgType.DB_RESPONSE,
                payload=result,
                client_id=client_id,
                request_id=req_id,
            )

        try:
            result = self.dispatch_operation(op, payload, client_id)
        except Exception as exc:
            self._logger.exception(f"Unhandled exception executing operation '{op}': {exc}")
            result = {"status": "ERROR", "reason": f"Unhandled server error: {exc}"}

        t_completed = time.perf_counter()

        status_str = "OK"
        res_status = result.get("status")
        if res_status in ("OK", "ACTIVE", "CANCELLED"):
            status_str = "OK"
        elif res_status in ("REJECTED", "NOT_FOUND"):
            status_str = "REJECTED"
        elif res_status == "ERROR" or "error" in result:
            status_str = "ERROR"

        extra: dict[str, Any] = {}
        if "reason" in result:
            extra["reason"] = result["reason"]
        if "reservation_id" in result and result["reservation_id"]:
            extra["reservation_id"] = str(result["reservation_id"])

        log_request(
            self._logger,
            request_id=req_id,
            client_id=client_id,
            op_type=op,
            t_received=t_received,
            t_completed=t_completed,
            status=status_str,
            extra=extra if extra else None,
        )

        return make_envelope(
            MsgType.DB_RESPONSE,
            payload=result,
            client_id=client_id,
            request_id=req_id,
        )

    # ── Service Lifecycle ─────────────────────────────────────────────

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Handle termination signals by triggering shutdown event."""
        self._logger.info(f"Received signal {signum}, initiating shutdown...")
        self._is_running = False
        self._shutdown_event.set()

    def start(self) -> None:
        """Start the Persistence Service and enter the request-processing loop."""
        self._logger.info(
            f"Starting Persistence Service on port {self._port}, health port {self._health_port}..."
        )

        # 1. Connect to CockroachDB with retry
        self.wait_for_db()

        # 2. Initialize connection pool
        self._init_pool()

        # 3. Start background HTTP HealthServer for /healthz probe
        self._health_server = HealthServer(
            port=self._health_port, check_fn=self.is_healthy
        )
        self._health_server.start()
        self._logger.info(f"HealthServer running on port {self._health_port}")

        # 4. Bind ZeroMQ REP socket
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.REP)
        self._socket.setsockopt(zmq.LINGER, 0)
        bind_addr = f"tcp://0.0.0.0:{self._port}"
        self._socket.bind(bind_addr)
        self._logger.info(f"ZeroMQ REP socket bound to {bind_addr}")

        self._is_ready = True
        self._is_running = True

        # Attach signal handlers for graceful termination
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
        except (ValueError, AttributeError):
            # Not in main thread or unsupported platform
            pass

        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)

        self._logger.info("Persistence Service ready and listening for incoming messages.")

        while self._is_running and not self._shutdown_event.is_set():
            try:
                socks = dict(poller.poll(timeout=1000))
                if self._socket in socks and socks[self._socket] == zmq.POLLIN:
                    raw_msg = self._socket.recv()
                    t_received = time.perf_counter()

                    resp_env: Envelope
                    try:
                        resp_env = self.handle_message(raw_msg, t_received)
                    except Exception as exc:
                        self._logger.exception(f"Unexpected error in handle_message: {exc}")
                        resp_env = make_envelope(
                            MsgType.DB_RESPONSE,
                            payload={"status": "ERROR", "reason": f"Fatal server error: {exc}"},
                        )

                    self._socket.send(resp_env.to_bytes())
            except zmq.ZMQError as exc:
                if self._shutdown_event.is_set():
                    break
                self._logger.error(f"ZeroMQ socket error: {exc}")
            except Exception as exc:
                if self._shutdown_event.is_set():
                    break
                self._logger.exception(f"Unexpected error in main loop: {exc}")

        self.stop()

    def stop(self) -> None:
        """Gracefully stop all listeners, thread servers, and database pools."""
        if not self._is_running and self._shutdown_event.is_set() and self._pool is None:
            return

        self._logger.info("Stopping Persistence Service...")
        self._is_running = False
        self._is_ready = False
        self._shutdown_event.set()

        if self._health_server:
            try:
                self._health_server.stop()
            except Exception as exc:
                self._logger.warning(f"Error stopping HealthServer: {exc}")

        if self._socket:
            try:
                self._socket.close(linger=0)
            except Exception as exc:
                self._logger.warning(f"Error closing ZeroMQ socket: {exc}")

        if self._context:
            try:
                self._context.term()
            except Exception as exc:
                self._logger.warning(f"Error terminating ZeroMQ context: {exc}")

        if self._pool:
            try:
                self._pool.close()
                self._logger.info("CockroachDB connection pool closed.")
            except Exception as exc:
                self._logger.warning(f"Error closing connection pool: {exc}")
            finally:
                self._pool = None

        self._logger.info("Persistence Service shutdown complete.")


if __name__ == "__main__":
    service = PersistenceService()
    try:
        service.start()
    except KeyboardInterrupt:
        service.stop()
