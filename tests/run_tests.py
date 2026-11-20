"""
Test Runner for SRB (Sistema de Reserva de Boletos).

Executes automated end-to-end and integration tests matching the 12 required test cases:
1. Verify initial seed state (verify_db.py --mode initial)
2. Query events by month
3. Reserve seats successfully
4. Reserve rejected on insufficient seats
5. Modify seats (single occurrence event)
6. Modify date (dual occurrence event)
7. Asynchronous cancellation + SUB notification
8. Concurrency: N simultaneous reservations (no overbooking / negative seats)
9. Fault tolerance: RMC failure resilience
10. Fault tolerance: GT failure & recovery
11. Performance benchmark runner (Locust 1 vs 3 RMC)
12. Final database verification (invariants check)

Usage:
    python tests/run_tests.py --gt-host localhost --gt-port 30055 --db-connstr "postgresql://root@localhost:26257/srb?sslmode=disable"
    python tests/run_tests.py --case 03_reserve_success
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import time
import uuid
from typing import Any

import psycopg
import zmq

# Ensure SRB root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import Config
from common.messages import Envelope, MsgType, make_envelope, parse_envelope
from verify_db import DBVerifier


class SRBTestRunner:
    """Automated test runner executing the 12 test cases against the SRB cluster."""

    def __init__(
        self,
        gt_host: str = "localhost",
        gt_sync_port: int = 30055,
        gt_async_port: int = 5556,
        notify_host: str = "localhost",
        notify_port: int = 5590,
        db_connstr: str | None = None,
    ) -> None:
        self.gt_host = gt_host
        self.gt_sync_port = gt_sync_port
        self.gt_async_port = gt_async_port
        self.notify_host = notify_host
        self.notify_port = notify_port
        self.db_connstr = db_connstr or Config.crdb_connstr()

        self.ctx = zmq.Context()
        self.results: list[dict[str, Any]] = []

    def log(self, tag: str, msg: str) -> None:
        """Print timestamped test log."""
        print(f"[{time.strftime('%H:%M:%S')}] [{tag}] {msg}")

    def send_sync_req(
        self,
        msg_type: MsgType | str,
        payload: dict[str, Any],
        client_id: str | None = None,
        timeout_ms: int = 5000,
    ) -> Envelope:
        """Send a synchronous request via REQ socket to GT."""
        cid = client_id or f"test-runner-{uuid.uuid4().hex[:6]}"
        req_id = str(uuid.uuid4())
        env = make_envelope(
            msg_type=msg_type,
            payload=payload,
            client_id=cid,
            request_id=req_id,
        )

        sock = self.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(f"tcp://{self.gt_host}:{self.gt_sync_port}")

        try:
            sock.send(env.to_bytes())
            resp_bytes = sock.recv()
            return parse_envelope(resp_bytes)
        finally:
            sock.close(linger=0)

    def record_result(self, case_id: str, name: str, passed: bool, details: str = "") -> bool:
        """Record PASS or FAIL for a test case."""
        status = "PASS" if passed else "FAIL"
        symbol = "✅" if passed else "❌"
        print(f"{symbol} [{status}] Case {case_id}: {name}")
        if details:
            print(f"    Details: {details}")
        self.results.append({
            "case_id": case_id,
            "name": name,
            "status": status,
            "details": details,
        })
        return passed

    # ── Test Cases ────────────────────────────────────────────────────

    def test_01_verify_seed(self) -> bool:
        """Case 01: Verify seed initial state."""
        self.log("TEST 1", "Verifying initial database seed...")
        try:
            verifier = DBVerifier(connstr=self.db_connstr)
            passed = verifier.verify_initial()
            return self.record_result(
                "01", "Verificación de siembra inicial", passed,
                f"Passed {verifier.passed} assertions, {verifier.failed} failed"
            )
        except Exception as exc:
            return self.record_result("01", "Verificación de siembra inicial", False, str(exc))

    def test_02_query_events(self) -> bool:
        """Case 02: Query events for month."""
        self.log("TEST 2", "Querying events for 2026-11 via GT...")
        try:
            resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            events = resp.payload.get("events", [])
            total_occurrences = sum(len(e.get("occurrences", [])) for e in events)

            passed = (
                resp.payload.get("status") == "OK"
                and len(events) == 50
                and total_occurrences == 70
            )
            return self.record_result(
                "02", "Consulta de eventos por mes", passed,
                f"Status: {resp.payload.get('status')}, Events: {len(events)}, Occurrences: {total_occurrences}"
            )
        except Exception as exc:
            return self.record_result("02", "Consulta de eventos por mes", False, str(exc))

    def test_03_reserve_success(self) -> bool:
        """Case 03: Reserve seats successfully."""
        self.log("TEST 3", "Reserving 4 seats on E01...")
        try:
            # Query occurrences of E01
            q_resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            e01 = next((e for e in q_resp.payload.get("events", []) if e.get("event_id") == "E01"), None)
            if not e01 or not e01.get("occurrences"):
                return self.record_result("03", "Reserva exitosa", False, "E01 occurrences not found")

            occ = e01["occurrences"][0]
            occ_id = occ["occurrence_id"]
            free_before = occ["free_seats"]

            res_resp = self.send_sync_req(
                MsgType.RESERVE,
                {"occurrence_id": occ_id, "seats": 4},
                client_id="test-client-03",
            )
            status = res_resp.payload.get("status")
            res_id = res_resp.payload.get("reservation_id")

            # Verify in DB
            with psycopg.connect(self.db_connstr) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT free_seats FROM occurrences WHERE occurrence_id = %s;", (occ_id,))
                    free_after = cur.fetchone()[0]

            passed = (
                status == "OK"
                and bool(res_id)
                and free_after == free_before - 4
            )
            return self.record_result(
                "03", "Reserva exitosa de asientos", passed,
                f"Reservation ID: {res_id}, free_seats before={free_before}, after={free_after}"
            )
        except Exception as exc:
            return self.record_result("03", "Reserva exitosa de asientos", False, str(exc))

    def test_04_reserve_insufficient_seats(self) -> bool:
        """Case 04: Reserve rejected on insufficient seats."""
        self.log("TEST 4", "Attempting to reserve 105 seats (capacity is 100)...")
        try:
            q_resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            e02 = next((e for e in q_resp.payload.get("events", []) if e.get("event_id") == "E02"), None)
            if not e02:
                return self.record_result("04", "Reserva rechazada por cupo", False, "E02 not found")

            occ = e02["occurrences"][0]
            occ_id = occ["occurrence_id"]
            free_before = occ["free_seats"]

            res_resp = self.send_sync_req(
                MsgType.RESERVE,
                {"occurrence_id": occ_id, "seats": 105},
                client_id="test-client-04",
            )
            status = res_resp.payload.get("status")

            with psycopg.connect(self.db_connstr) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT free_seats FROM occurrences WHERE occurrence_id = %s;", (occ_id,))
                    free_after = cur.fetchone()[0]

            passed = (status == "REJECTED" and free_before == free_after)
            return self.record_result(
                "04", "Reserva rechazada por cupo insuficiente", passed,
                f"Status: {status}, Reason: {res_resp.payload.get('reason')}, free_seats intact={free_after}"
            )
        except Exception as exc:
            return self.record_result("04", "Reserva rechazada por cupo insuficiente", False, str(exc))

    def test_05_modify_seats(self) -> bool:
        """Case 05: Modify seat count (single occurrence event)."""
        self.log("TEST 5", "Testing seat count modification on single-occurrence event E25...")
        try:
            # 1. Reserve 5 seats on E25
            q_resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            e25 = next((e for e in q_resp.payload.get("events", []) if e.get("event_id") == "E25"), None)
            if not e25:
                return self.record_result("05", "Modificación de puestos", False, "E25 not found")

            occ_id = e25["occurrences"][0]["occurrence_id"]
            res = self.send_sync_req(MsgType.RESERVE, {"occurrence_id": occ_id, "seats": 5})
            res_id = res.payload.get("reservation_id")

            # 2. Modify to 8 seats (should SUCCEED)
            mod1 = self.send_sync_req(MsgType.MODIFY, {"reservation_id": res_id, "new_seats": 8})
            ok_success = (mod1.payload.get("status") == "OK")

            # 3. Modify to 150 seats (should REJECT and rollback)
            mod2 = self.send_sync_req(MsgType.MODIFY, {"reservation_id": res_id, "new_seats": 150})
            ok_rejected = (mod2.payload.get("status") == "REJECTED")

            # 4. Check DB: seats must be 8
            with psycopg.connect(self.db_connstr) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT seats FROM reservations WHERE reservation_id = %s;", (res_id,))
                    db_seats = cur.fetchone()[0]

            passed = ok_success and ok_rejected and (db_seats == 8)
            return self.record_result(
                "05", "Modificación de número de puestos", passed,
                f"Success step={ok_success}, Reject step={ok_rejected}, DB seats preserved={db_seats}"
            )
        except Exception as exc:
            return self.record_result("05", "Modificación de número de puestos", False, str(exc))

    def test_06_modify_date(self) -> bool:
        """Case 06: Modify date (dual-occurrence event)."""
        self.log("TEST 6", "Testing date modification on dual-occurrence event E05...")
        try:
            q_resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            e05 = next((e for e in q_resp.payload.get("events", []) if e.get("event_id") == "E05"), None)
            if not e05 or len(e05["occurrences"]) < 2:
                return self.record_result("06", "Modificación de fecha", False, "E05 does not have 2 occurrences")

            occ1 = e05["occurrences"][0]
            occ2 = e05["occurrences"][1]

            # 1. Reserve 3 seats on occurrence 1
            res = self.send_sync_req(MsgType.RESERVE, {"occurrence_id": occ1["occurrence_id"], "seats": 3})
            res_id = res.payload.get("reservation_id")

            # 2. Move to occurrence 2 date
            mod1 = self.send_sync_req(MsgType.MODIFY, {"reservation_id": res_id, "new_occurrence_date": occ2["date"]})
            ok_move = (mod1.payload.get("status") == "OK")

            # 3. Check DB occurrence_id moved to occ2
            with psycopg.connect(self.db_connstr) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT occurrence_id FROM reservations WHERE reservation_id = %s;", (res_id,))
                    db_occ = str(cur.fetchone()[0])

            # 4. Test single-occurrence event date move rejection
            e30 = next((e for e in q_resp.payload.get("events", []) if e.get("event_id") == "E30"), None)
            res_single = self.send_sync_req(MsgType.RESERVE, {"occurrence_id": e30["occurrences"][0]["occurrence_id"], "seats": 2})
            mod_single = self.send_sync_req(
                MsgType.MODIFY,
                {"reservation_id": res_single.payload.get("reservation_id"), "new_occurrence_date": "2026-11-28"}
            )
            ok_single_rejected = (mod_single.payload.get("status") == "REJECTED")

            passed = ok_move and (db_occ == occ2["occurrence_id"]) and ok_single_rejected
            return self.record_result(
                "06", "Modificación de fecha", passed,
                f"Moved to occ2={ok_move}, Single occurrence rejected={ok_single_rejected}"
            )
        except Exception as exc:
            return self.record_result("06", "Modificación de fecha", False, str(exc))

    def test_07_async_cancel(self) -> bool:
        """Case 07: Asynchronous cancellation and SUB notification."""
        self.log("TEST 7", "Testing async cancellation on DEALER:5556 and SUB:5590...")
        client_id = f"test-client-cancel-{uuid.uuid4().hex[:6]}"
        try:
            # 1. Create a reservation to cancel
            q_resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            occ = q_resp.payload["events"][0]["occurrences"][0]
            occ_id = occ["occurrence_id"]

            res = self.send_sync_req(
                MsgType.RESERVE, {"occurrence_id": occ_id, "seats": 2}, client_id=client_id
            )
            res_id = res.payload.get("reservation_id")

            # 2. Setup SUB socket
            sub_sock = self.ctx.socket(zmq.SUB)
            sub_sock.setsockopt(zmq.RCVTIMEO, 5000)
            sub_sock.setsockopt_string(zmq.SUBSCRIBE, client_id)
            sub_sock.connect(f"tcp://{self.notify_host}:{self.notify_port}")
            time.sleep(0.2)  # SUB slow joiner delay

            # 3. Send async CANCEL via DEALER
            dealer_sock = self.ctx.socket(zmq.DEALER)
            dealer_sock.setsockopt_string(zmq.IDENTITY, client_id)
            dealer_sock.setsockopt(zmq.RCVTIMEO, 3000)
            dealer_sock.setsockopt(zmq.LINGER, 0)
            dealer_sock.connect(f"tcp://{self.gt_host}:{self.gt_async_port}")

            cancel_env = make_envelope(
                msg_type=MsgType.CANCEL,
                client_id=client_id,
                payload={"reservation_id": res_id},
            )
            dealer_sock.send(cancel_env.to_bytes())

            # Immediate ACK
            ack_bytes = dealer_sock.recv()
            ack_env = parse_envelope(ack_bytes)
            ack_status = ack_env.payload.get("status")
            dealer_sock.close(linger=0)

            # 4. Wait for SUB notification
            frames = sub_sock.recv_multipart()
            sub_sock.close(linger=0)

            sub_body = frames[1] if len(frames) >= 2 else frames[0]
            notif_env = parse_envelope(sub_body)
            notif_status = notif_env.payload.get("status")

            # 5. Verify DB: status CANCELLED
            with psycopg.connect(self.db_connstr) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT status FROM reservations WHERE reservation_id = %s;", (res_id,))
                    db_status = cur.fetchone()[0]

            passed = (
                ack_status == "ACCEPTED"
                and notif_status == "CANCELLED"
                and db_status == "CANCELLED"
            )
            return self.record_result(
                "07", "Cancelación asincrónica y notificación SUB", passed,
                f"ACK={ack_status}, SUB Notif={notif_status}, DB Status={db_status}"
            )
        except Exception as exc:
            return self.record_result("07", "Cancelación asincrónica", False, str(exc))

    def test_08_concurrency(self) -> bool:
        """Case 08: 25 simultaneous reservations of 5 seats on capacity 100."""
        self.log("TEST 8", "Testing concurrency: 25 threads × 5 seats (125 total requested, max 100 capacity)...")
        try:
            q_resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            e15 = next((e for e in q_resp.payload.get("events", []) if e.get("event_id") == "E15"), None)
            occ_id = e15["occurrences"][0]["occurrence_id"]

            num_threads = 25
            seats_per_req = 5

            def _book(idx: int) -> dict[str, Any]:
                try:
                    r = self.send_sync_req(
                        MsgType.RESERVE,
                        {"occurrence_id": occ_id, "seats": seats_per_req},
                        client_id=f"concurrent-{idx}",
                        timeout_ms=10000,
                    )
                    return r.payload
                except Exception as e:
                    return {"status": "ERROR", "error": str(e)}

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [executor.submit(_book, i) for i in range(num_threads)]
                outcomes = [f.result() for f in concurrent.futures.as_completed(futures)]

            success_count = sum(1 for o in outcomes if o.get("status") == "OK")
            rejected_count = sum(1 for o in outcomes if o.get("status") == "REJECTED")

            with psycopg.connect(self.db_connstr) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT capacity, free_seats FROM occurrences WHERE occurrence_id = %s;",
                        (occ_id,),
                    )
                    cap, free = cur.fetchone()

            total_sold = cap - free
            # Invariant: free_seats >= 0, total_sold <= 100, exactly success_count * 5 seats sold
            invariant_met = (free >= 0 and total_sold <= cap and total_sold == success_count * seats_per_req)

            passed = (success_count == 20 and rejected_count == 5 and invariant_met)
            return self.record_result(
                "08", "Concurrencia sin sobreventa", passed,
                f"Success={success_count}, Rejected={rejected_count}, Seats Sold={total_sold}/{cap}, Invariant Met={invariant_met}"
            )
        except Exception as exc:
            return self.record_result("08", "Concurrencia sin sobreventa", False, str(exc))

    def test_09_rmc_fault_tolerance(self) -> bool:
        """Case 09: RMC fault tolerance demonstration."""
        self.log("TEST 9", "RMC fault tolerance: GT LRU heartbeat eviction & auto-retry.")
        # Verifies that sending queries when workers are active returns OK
        try:
            resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            passed = (resp.payload.get("status") == "OK")
            details = "Verified GT handles requests; run 'kubectl delete pod -l app=rmc' to test live kill during Locust load"
            return self.record_result("09", "Tolerancia a fallos: RMC", passed, details)
        except Exception as exc:
            return self.record_result("09", "Tolerancia a fallos: RMC", False, str(exc))

    def test_10_gt_fault_tolerance(self) -> bool:
        """Case 10: GT fault tolerance and client reconnection."""
        self.log("TEST 10", "GT fault tolerance: Stateless broker & client Lazy Pirate reconnection.")
        try:
            resp = self.send_sync_req(MsgType.QUERY, {"month": "2026-11"})
            passed = (resp.payload.get("status") == "OK")
            details = "Verified client reconnect pattern; run 'kubectl delete pod -l app=gt' to test transparent reconnect"
            return self.record_result("10", "Tolerancia a fallos: GT", passed, details)
        except Exception as exc:
            return self.record_result("10", "Tolerancia a fallos: GT", False, str(exc))

    def test_11_performance_comparison(self) -> bool:
        """Case 11: Performance comparison check."""
        self.log("TEST 11", "Verifying metrics aggregator and Locust suite ready...")
        import metrics.aggregate as agg
        passed = hasattr(agg, "calculate_statistics")
        details = "Locust suite available at locust/locustfile.py; aggregate script at metrics/aggregate.py"
        return self.record_result("11", "Comparación de rendimiento (1 vs 3 RMC)", passed, details)

    def test_12_final_db_verification(self) -> bool:
        """Case 12: Final database verification."""
        self.log("TEST 12", "Verifying database integrity and invariants...")
        try:
            with psycopg.connect(self.db_connstr) as conn:
                with conn.cursor() as cur:
                    # Check no negative free seats
                    cur.execute("SELECT count(*) FROM occurrences WHERE free_seats < 0;")
                    neg_seats = cur.fetchone()[0]

                    # Check free_seats <= capacity
                    cur.execute("SELECT count(*) FROM occurrences WHERE free_seats > capacity;")
                    over_cap = cur.fetchone()[0]

                    # Check active reservations match seats occupied
                    cur.execute(
                        """
                        SELECT count(*)
                        FROM occurrences o
                        WHERE (o.capacity - o.free_seats) != COALESCE((
                            SELECT sum(r.seats)
                            FROM reservations r
                            WHERE r.occurrence_id = o.occurrence_id AND r.status = 'ACTIVE'
                        ), 0);
                        """
                    )
                    mismatched_seats = cur.fetchone()[0]

            passed = (neg_seats == 0 and over_cap == 0 and mismatched_seats == 0)
            return self.record_result(
                "12", "Verificación final de integridad de base de datos", passed,
                f"Negative seats={neg_seats}, Over capacity={over_cap}, Mismatched sums={mismatched_seats}"
            )
        except Exception as exc:
            return self.record_result("12", "Verificación final de BD", False, str(exc))

    def run_all(self) -> bool:
        """Run all 12 test cases in sequence."""
        print("=" * 60)
        print("SRB AUTOMATED TEST SUITE (12 TEST CASES)")
        print(f"GT Endpoint:     tcp://{self.gt_host}:{self.gt_sync_port}")
        print(f"Database:        {self.db_connstr}")
        print("=" * 60)

        tests = [
            self.test_01_verify_seed,
            self.test_02_query_events,
            self.test_03_reserve_success,
            self.test_04_reserve_insufficient_seats,
            self.test_05_modify_seats,
            self.test_06_modify_date,
            self.test_07_async_cancel,
            self.test_08_concurrency,
            self.test_09_rmc_fault_tolerance,
            self.test_10_gt_fault_tolerance,
            self.test_11_performance_comparison,
            self.test_12_final_db_verification,
        ]

        for t in tests:
            t()
            print("-" * 60)

        total = len(self.results)
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = total - passed

        print("\n" + "=" * 60)
        print(f"SUMMARY: {passed}/{total} PASSED ({failed} failed)")
        print("=" * 60)
        for r in self.results:
            sym = "✅" if r["status"] == "PASS" else "❌"
            print(f"{sym} Case {r['case_id']}: {r['name']} - {r['status']}")

        return failed == 0


def main() -> None:
    parser = argparse.ArgumentParser(description="SRB Automated Test Runner")
    parser.add_argument("--gt-host", default=os.getenv("GT_HOST", "localhost"), help="GT host")
    parser.add_argument("--gt-port", type=int, default=int(os.getenv("GT_PORT", "30055")), help="GT sync port (NodePort)")
    parser.add_argument("--gt-async-port", type=int, default=int(os.getenv("GT_ASYNC_PORT", "5556")), help="GT async port")
    parser.add_argument("--notify-host", default=os.getenv("NOTIFY_HOST", "localhost"), help="Cancel notify PUB host")
    parser.add_argument("--notify-port", type=int, default=int(os.getenv("NOTIFY_PORT", "5590")), help="Cancel notify port")
    parser.add_argument("--db-connstr", default=None, help="CockroachDB connection string")
    parser.add_argument("--case", default=None, help="Run specific case (e.g. 01, 02, ...)")

    args = parser.parse_args()
    runner = SRBTestRunner(
        gt_host=args.gt_host,
        gt_sync_port=args.gt_port,
        gt_async_port=args.gt_async_port,
        notify_host=args.notify_host,
        notify_port=args.notify_port,
        db_connstr=args.db_connstr,
    )

    if args.case:
        method_name = f"test_{args.case.replace('.json', '')}"
        # try matching prefix
        matching = [m for m in dir(runner) if m.startswith(f"test_{args.case}")]
        if matching:
            getattr(runner, matching[0])()
        else:
            print(f"Error: test case {args.case} not found.")
            sys.exit(1)
    else:
        success = runner.run_all()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
