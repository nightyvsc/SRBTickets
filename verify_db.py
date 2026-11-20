"""
Database verification script for SRB (Sistema de Reserva de Boletos).

Supports two verification modes:
1. 'initial': Verifies the deterministic seed state:
   - 50 events for configured month (E01..E50)
   - 20 dual-occurrence events (E01..E20, 2 distinct dates each)
   - 30 single-occurrence events (E21..E50, 1 date each)
   - Total of 70 occurrences, all with capacity=100, free_seats=100
   - 0 reservations
2. 'case': Verifies DB state against expectations defined in a JSON case file.

Usage:
    python verify_db.py --mode initial
    python verify_db.py --mode case --case-file tests/cases/01_reserve.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import psycopg

from common.config import Config


class DBVerifier:
    """Performs structured verification checks against CockroachDB."""

    def __init__(self, connstr: str | None = None) -> None:
        self.connstr = connstr or Config.crdb_connstr()
        self.passed: int = 0
        self.failed: int = 0

    def check(self, name: str, condition: bool, details: str = "") -> bool:
        """Record and print PASS/FAIL for an assertion."""
        if condition:
            self.passed += 1
            print(f"  [PASS] {name}{f' - {details}' if details else ''}")
            return True
        else:
            self.failed += 1
            print(f"  [FAIL] {name}{f' - {details}' if details else ''}", file=sys.stderr)
            return False

    def connect(self) -> psycopg.Connection[Any]:
        """Establish connection to CockroachDB."""
        try:
            return psycopg.connect(self.connstr, autocommit=True)
        except Exception as exc:
            print(f"ERROR: Could not connect to CockroachDB ({self.connstr}): {exc}", file=sys.stderr)
            sys.exit(2)

    # ── Mode: Initial ─────────────────────────────────────────────────

    def verify_initial(self) -> bool:
        """Verify initial seed state according to architectural specifications."""
        print(f"==================================================")
        print(f"VERIFYING INITIAL SEED STATE")
        print(f"Month: {Config.SEED_MONTH}, Expected Events: {Config.SEED_NUM_EVENTS}")
        print(f"Connection: {self.connstr}")
        print(f"==================================================")

        with self.connect() as conn:
            with conn.cursor() as cur:
                # 1. Total events count
                cur.execute("SELECT count(*) FROM events;")
                total_events = cur.fetchone()[0]  # type: ignore[index]
                self.check(
                    "Total events count",
                    total_events == Config.SEED_NUM_EVENTS,
                    f"expected {Config.SEED_NUM_EVENTS}, got {total_events}",
                )

                # 2. Events matching configured month
                cur.execute("SELECT count(*) FROM events WHERE month = %s;", (Config.SEED_MONTH,))
                events_in_month = cur.fetchone()[0]  # type: ignore[index]
                self.check(
                    f"Events in month '{Config.SEED_MONTH}'",
                    events_in_month == Config.SEED_NUM_EVENTS,
                    f"expected {Config.SEED_NUM_EVENTS}, got {events_in_month}",
                )

                # 3. Events IDs E01..E50 present
                cur.execute("SELECT event_id FROM events ORDER BY event_id;")
                event_ids = [row[0] for row in cur.fetchall()]
                expected_ids = [f"E{i:02d}" for i in range(1, Config.SEED_NUM_EVENTS + 1)]
                self.check(
                    "Event ID sequence E01..E50",
                    event_ids == expected_ids,
                    f"{len(event_ids)} IDs matched",
                )

                # 4. Total occurrences count
                expected_total_occurrences = (
                    Config.SEED_DUAL_OCCURRENCE_COUNT * 2
                    + (Config.SEED_NUM_EVENTS - Config.SEED_DUAL_OCCURRENCE_COUNT) * 1
                )
                cur.execute("SELECT count(*) FROM occurrences;")
                total_occurrences = cur.fetchone()[0]  # type: ignore[index]
                self.check(
                    "Total occurrences count",
                    total_occurrences == expected_total_occurrences,
                    f"expected {expected_total_occurrences}, got {total_occurrences}",
                )

                # 5. Dual occurrence events (E01..E20)
                cur.execute(
                    """
                    SELECT event_id, count(*)
                    FROM occurrences
                    GROUP BY event_id
                    HAVING count(*) = 2
                    ORDER BY event_id;
                    """
                )
                dual_events = [row[0] for row in cur.fetchall()]
                expected_dual = [f"E{i:02d}" for i in range(1, Config.SEED_DUAL_OCCURRENCE_COUNT + 1)]
                self.check(
                    f"Dual occurrence count ({Config.SEED_DUAL_OCCURRENCE_COUNT} events)",
                    dual_events == expected_dual,
                    f"matched {len(dual_events)} dual-occurrence events (E01..E{Config.SEED_DUAL_OCCURRENCE_COUNT:02d})",
                )

                # 6. Single occurrence events (E21..E50)
                cur.execute(
                    """
                    SELECT event_id, count(*)
                    FROM occurrences
                    GROUP BY event_id
                    HAVING count(*) = 1
                    ORDER BY event_id;
                    """
                )
                single_events = [row[0] for row in cur.fetchall()]
                expected_single = [
                    f"E{i:02d}"
                    for i in range(
                        Config.SEED_DUAL_OCCURRENCE_COUNT + 1, Config.SEED_NUM_EVENTS + 1
                    )
                ]
                self.check(
                    f"Single occurrence count ({Config.SEED_NUM_EVENTS - Config.SEED_DUAL_OCCURRENCE_COUNT} events)",
                    single_events == expected_single,
                    f"matched {len(single_events)} single-occurrence events",
                )

                # 7. All occurrences have capacity == 100 and free_seats == 100
                cur.execute(
                    """
                    SELECT count(*) FROM occurrences
                    WHERE capacity != %s OR free_seats != %s;
                    """,
                    (Config.SEAT_CAPACITY, Config.SEAT_CAPACITY),
                )
                mismatched_seats = cur.fetchone()[0]  # type: ignore[index]
                self.check(
                    f"Occurrences seats capacity={Config.SEAT_CAPACITY} and free_seats={Config.SEAT_CAPACITY}",
                    mismatched_seats == 0,
                    f"non-conforming occurrences: {mismatched_seats}",
                )

                # 8. All occurrence dates belong to SEED_MONTH
                cur.execute(
                    """
                    SELECT count(*) FROM occurrences
                    WHERE to_char(date, 'YYYY-MM') != %s;
                    """,
                    (Config.SEED_MONTH,),
                )
                invalid_dates = cur.fetchone()[0]  # type: ignore[index]
                self.check(
                    f"All occurrence dates within month '{Config.SEED_MONTH}'",
                    invalid_dates == 0,
                    f"out of range occurrences: {invalid_dates}",
                )

                # 9. No initial reservations
                cur.execute("SELECT count(*) FROM reservations;")
                total_reservations = cur.fetchone()[0]  # type: ignore[index]
                self.check(
                    "Initial reservations table is empty",
                    total_reservations == 0,
                    f"expected 0 reservations, found {total_reservations}",
                )

        return self.failed == 0

    # ── Mode: Case ────────────────────────────────────────────────────

    def verify_case(self, case_file_path: str) -> bool:
        """Verify DB state against rules specified in a JSON case file."""
        if not os.path.exists(case_file_path):
            print(f"ERROR: Case file not found: {case_file_path}", file=sys.stderr)
            return False

        with open(case_file_path, "r", encoding="utf-8") as f:
            case_data = json.load(f)

        name = case_data.get("name", os.path.basename(case_file_path))
        print(f"==================================================")
        print(f"VERIFYING TEST CASE: {name}")
        print(f"Case File: {case_file_path}")
        print(f"==================================================")

        expected = case_data.get("expected", {})

        with self.connect() as conn:
            with conn.cursor() as cur:
                # 1. Total events count
                if "events_count" in expected:
                    cur.execute("SELECT count(*) FROM events;")
                    actual_events = cur.fetchone()[0]  # type: ignore[index]
                    self.check(
                        "Events count",
                        actual_events == expected["events_count"],
                        f"expected {expected['events_count']}, got {actual_events}",
                    )

                # 2. Total reservations count
                if "reservations_count" in expected or "total_reservations" in expected:
                    exp_count = expected.get("reservations_count", expected.get("total_reservations"))
                    cur.execute("SELECT count(*) FROM reservations;")
                    actual_count = cur.fetchone()[0]  # type: ignore[index]
                    self.check(
                        "Total reservations count",
                        actual_count == exp_count,
                        f"expected {exp_count}, got {actual_count}",
                    )

                # 3. Reservations by status
                if "reservations_by_status" in expected:
                    for status, count in expected["reservations_by_status"].items():
                        cur.execute(
                            "SELECT count(*) FROM reservations WHERE status = %s;",
                            (status,),
                        )
                        actual_status_count = cur.fetchone()[0]  # type: ignore[index]
                        self.check(
                            f"Reservations with status '{status}'",
                            actual_status_count == count,
                            f"expected {count}, got {actual_status_count}",
                        )

                # 4. Specific reservations check
                if "reservations" in expected:
                    for idx, exp_res in enumerate(expected["reservations"]):
                        where_clauses: list[str] = []
                        params: list[Any] = []
                        for field, val in exp_res.items():
                            where_clauses.append(f"{field} = %s")
                            params.append(val)

                        query = f"SELECT count(*) FROM reservations WHERE {' AND '.join(where_clauses)};"
                        cur.execute(query, tuple(params))
                        res_matches = cur.fetchone()[0]  # type: ignore[index]
                        self.check(
                            f"Expected reservation #{idx + 1} exists",
                            res_matches >= 1,
                            f"criteria: {exp_res}",
                        )

                # 5. Occurrences check
                if "occurrences" in expected:
                    for idx, exp_occ in enumerate(expected["occurrences"]):
                        event_id = exp_occ.get("event_id")
                        date_val = exp_occ.get("date")
                        exp_free = exp_occ.get("free_seats")

                        if date_val:
                            cur.execute(
                                "SELECT free_seats, capacity FROM occurrences WHERE event_id = %s AND date = %s;",
                                (event_id, date_val),
                            )
                        else:
                            cur.execute(
                                "SELECT free_seats, capacity FROM occurrences WHERE event_id = %s;",
                                (event_id,),
                            )
                        row = cur.fetchone()
                        if row is None:
                            self.check(f"Occurrence #{idx + 1} exists", False, f"for event {event_id}")
                            continue

                        actual_free, actual_cap = row[0], row[1]
                        if exp_free is not None:
                            self.check(
                                f"Occurrence {event_id} free seats",
                                actual_free == exp_free,
                                f"expected {exp_free}, got {actual_free}",
                            )

                        if "occupied_seats" in exp_occ:
                            expected_occupied = exp_occ["occupied_seats"]
                            actual_occupied = actual_cap - actual_free
                            self.check(
                                f"Occurrence {event_id} occupied seats",
                                actual_occupied == expected_occupied,
                                f"expected {expected_occupied}, got {actual_occupied}",
                            )

                # 6. Global Invariants
                invariants = expected.get("invariants", {})
                if invariants.get("no_negative_free_seats", True):
                    cur.execute("SELECT count(*) FROM occurrences WHERE free_seats < 0;")
                    neg_count = cur.fetchone()[0]  # type: ignore[index]
                    self.check(
                        "Invariant: no occurrences with free_seats < 0",
                        neg_count == 0,
                        f"found {neg_count} violations",
                    )

                if invariants.get("no_overcapacity", True):
                    cur.execute("SELECT count(*) FROM occurrences WHERE free_seats > capacity;")
                    over_count = cur.fetchone()[0]  # type: ignore[index]
                    self.check(
                        "Invariant: no occurrences with free_seats > capacity",
                        over_count == 0,
                        f"found {over_count} violations",
                    )

                if invariants.get("active_seats_match_occupied", True):
                    # For every occurrence, occupied seats must match sum of ACTIVE reservation seats
                    cur.execute(
                        """
                        SELECT o.occurrence_id, o.capacity - o.free_seats AS occupied,
                               COALESCE(SUM(r.seats), 0) AS reserved
                        FROM occurrences o
                        LEFT JOIN reservations r ON o.occurrence_id = r.occurrence_id AND r.status = 'ACTIVE'
                        GROUP BY o.occurrence_id, o.capacity, o.free_seats
                        HAVING (o.capacity - o.free_seats) != COALESCE(SUM(r.seats), 0);
                        """
                    )
                    mismatches = cur.fetchall()
                    self.check(
                        "Invariant: occurrence occupied seats == sum of ACTIVE reservation seats",
                        len(mismatches) == 0,
                        f"found {len(mismatches)} inconsistent occurrences",
                    )

                # 7. Custom SQL checks
                if "custom_queries" in expected:
                    for cq in expected["custom_queries"]:
                        cq_name = cq.get("name", "Custom query")
                        sql = cq.get("sql", "")
                        exp_val = cq.get("expected")
                        cur.execute(sql)
                        actual_val = cur.fetchone()[0]  # type: ignore[index]
                        self.check(
                            cq_name,
                            actual_val == exp_val,
                            f"expected {exp_val}, got {actual_val}",
                        )

        return self.failed == 0


def main() -> None:
    """CLI entrypoint for database verification."""
    parser = argparse.ArgumentParser(
        description="Verify SRB CockroachDB state (initial seed or test case)."
    )
    parser.add_argument(
        "--mode",
        choices=["initial", "case"],
        required=True,
        help="Verification mode: 'initial' for seed validation, 'case' for test case file.",
    )
    parser.add_argument(
        "--case-file",
        type=str,
        default=None,
        help="Path to JSON test case expectation file (required when --mode case).",
    )
    parser.add_argument(
        "--connstr",
        type=str,
        default=None,
        help="CockroachDB connection string (defaults to Config.crdb_connstr()).",
    )

    args = parser.parse_args()

    if args.mode == "case" and not args.case_file:
        parser.error("--case-file is required when --mode case is specified.")

    verifier = DBVerifier(connstr=args.connstr)

    if args.mode == "initial":
        success = verifier.verify_initial()
    else:
        success = verifier.verify_case(args.case_file)

    print(f"==================================================")
    print(f"SUMMARY: {verifier.passed} passed, {verifier.failed} failed.")
    print(f"RESULT:  {'ALL CHECKS PASSED' if success else 'VERIFICATION FAILED'}")
    print(f"==================================================")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
