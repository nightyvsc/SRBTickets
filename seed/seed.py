"""
Deterministic database seeder for SRB (Sistema de Reserva de Boletos).

Connects to CockroachDB using psycopg v3, creates required relational schema,
truncates existing tables, and seeds 50 events for the configured month:
- 20 events (E01..E20) have 2 occurrences (random dates in the month).
- 30 events (E21..E50) have 1 occurrence (random date in the month).
- Each occurrence has capacity=100, free_seats=100.
- Completely reproducible using SEED_RANDOM_SEED (default 42).
"""

from __future__ import annotations

import calendar
import datetime
import os
import random
import sys
import time
from typing import Any

import psycopg

# Ensure SRB root is in path if run directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import Config


# ── SQL Schema Definitions ────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    month CHAR(7) NOT NULL
);

CREATE TABLE IF NOT EXISTS occurrences (
    occurrence_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    capacity INT NOT NULL DEFAULT 100,
    free_seats INT NOT NULL DEFAULT 100
);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id TEXT NOT NULL,
    occurrence_id UUID NOT NULL REFERENCES occurrences(occurrence_id) ON DELETE CASCADE,
    seats INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_occurrences_event_id ON occurrences(event_id);
CREATE INDEX IF NOT EXISTS idx_reservations_client_id ON reservations(client_id);
CREATE INDEX IF NOT EXISTS idx_reservations_occurrence_id ON reservations(occurrence_id);
"""

TRUNCATE_SQL = """
TRUNCATE TABLE reservations, occurrences, events CASCADE;
"""


def connect_with_retry(
    connstr: str, max_retries: int = 30, retry_delay: float = 2.0
) -> psycopg.Connection[Any]:
    """
    Connect to CockroachDB, retrying until the server is ready.

    Useful for Kubernetes Job initialization where CockroachDB might still be starting.
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[seed] Attempting connection to CockroachDB (attempt {attempt}/{max_retries})...")
            conn = psycopg.connect(connstr, autocommit=True)
            print("[seed] Connected successfully to CockroachDB.")
            return conn
        except psycopg.OperationalError as exc:
            if attempt == max_retries:
                print(f"[seed] ERROR: Failed to connect to database: {exc}", file=sys.stderr)
                raise
            time.sleep(retry_delay)
    raise RuntimeError("Unreachable")


def seed_database() -> None:
    """Execute schema creation, truncation, and deterministic initial data insertion."""
    seed = Config.SEED_RANDOM_SEED
    month_str = Config.SEED_MONTH
    total_events = Config.SEED_NUM_EVENTS
    dual_count = Config.SEED_DUAL_OCCURRENCE_COUNT
    capacity = Config.SEAT_CAPACITY
    connstr = Config.crdb_connstr()

    print(f"==================================================")
    print(f"SRB DATABASE SEEDER")
    print(f"==================================================")
    print(f"Target Month:       {month_str}")
    print(f"Total Events:       {total_events}")
    print(f"Dual Occurrences:   {dual_count} events (2 dates each)")
    print(f"Single Occurrences: {total_events - dual_count} events (1 date each)")
    print(f"Occurrence Seats:   {capacity}")
    print(f"Random Seed:        {seed}")
    print(f"Connection:         {connstr}")
    print(f"==================================================")

    # Deterministic RNG
    rng = random.Random(seed)

    # Calculate valid dates in configured month
    try:
        year, month = map(int, month_str.split("-"))
    except ValueError:
        raise ValueError(f"Invalid SEED_MONTH format '{month_str}'. Expected 'YYYY-MM'.")

    num_days = calendar.monthrange(year, month)[1]
    all_possible_dates = [
        datetime.date(year, month, day) for day in range(1, num_days + 1)
    ]

    conn = connect_with_retry(connstr)
    try:
        with conn.cursor() as cur:
            # 1. Apply Schema
            print("[seed] Creating schema if not exists...")
            cur.execute(SCHEMA_SQL)

            # 2. Truncate existing data
            print("[seed] Truncating existing tables...")
            cur.execute(TRUNCATE_SQL)

        # 3. Generate seed data
        events_rows: list[tuple[str, str, str]] = []
        occurrences_rows: list[tuple[str, datetime.date, int, int]] = []

        for i in range(1, total_events + 1):
            event_id = f"E{i:02d}"
            name = (
                f"Evento Cultural {event_id}"
                if i % 2 == 1
                else f"Evento Deportivo {event_id}"
            )
            events_rows.append((event_id, name, month_str))

            if i <= dual_count:
                # 2 occurrences with distinct dates in the month
                picked_dates = sorted(rng.sample(all_possible_dates, 2))
                for d in picked_dates:
                    occurrences_rows.append((event_id, d, capacity, capacity))
            else:
                # 1 occurrence
                picked_date = rng.choice(all_possible_dates)
                occurrences_rows.append((event_id, picked_date, capacity, capacity))

        # 4. Insert data in transaction
        print(f"[seed] Inserting {len(events_rows)} events and {len(occurrences_rows)} occurrences...")
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO events (event_id, name, month) VALUES (%s, %s, %s);",
                    events_rows,
                )
                cur.executemany(
                    "INSERT INTO occurrences (event_id, date, capacity, free_seats) "
                    "VALUES (%s, %s, %s, %s);",
                    occurrences_rows,
                )

        # 5. Verify counts
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM events;")
            ev_count = cur.fetchone()[0]  # type: ignore[index]
            cur.execute("SELECT count(*) FROM occurrences;")
            occ_count = cur.fetchone()[0]  # type: ignore[index]
            cur.execute("SELECT count(*) FROM reservations;")
            res_count = cur.fetchone()[0]  # type: ignore[index]

        print(f"==================================================")
        print(f"SEED SUMMARY (SUCCESS):")
        print(f"  Events inserted:       {ev_count}")
        print(f"  Occurrences inserted:  {occ_count}")
        print(f"  Reservations active:   {res_count}")
        print(f"==================================================")

    finally:
        conn.close()


if __name__ == "__main__":
    seed_database()
