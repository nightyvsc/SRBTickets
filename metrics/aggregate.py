"""
Metrics aggregation script for SRB (Sistema de Reserva de Boletos).

Reads JSON log lines from files or stdin, computes performance metrics per operation:
- Average response time (ms)
- Standard deviation of response time (ms)
- Min, Max, p50, p95, p99 percentiles
- Transactions Per Minute (TPM)
- Error and success counts

Outputs:
- Pretty printed console table
- JSON report
- CSV export
- Comparison mode: compares 1 RMC vs 3 RMC log datasets with relative speedup / TPM gains.

Usage:
    python -m metrics.aggregate logs.json
    cat gt.log | python -m metrics.aggregate
    python -m metrics.aggregate --from 2026-11-20T10:00:00 --to 2026-11-20T10:05:00 logs.json
    python -m metrics.aggregate --compare 1rmc.json 3rmc.json --labels "1 RMC" "3 RMC"
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, TextIO


def parse_timestamp(value: Any) -> float | None:
    """Convert various timestamp formats (epoch float, ISO 8601 string) to epoch seconds."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        val = value.strip()
        # Try float
        try:
            return float(val)
        except ValueError:
            pass
        # Try ISO format
        try:
            dt = datetime.fromisoformat(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            pass
    return None


def extract_json_record(line: str) -> dict[str, Any] | None:
    """Extract a JSON object from a log line, handling raw JSON or prefixed logs."""
    line = line.strip()
    if not line:
        return None

    # Fast path: pure JSON line
    if line.startswith("{") and line.endswith("}"):
        try:
            return json.loads(line)  # type: ignore[no-any-return]
        except Exception:
            pass

    # Look for first '{' and last '}'
    start_idx = line.find("{")
    end_idx = line.rfind("}")
    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        try:
            return json.loads(line[start_idx : end_idx + 1])  # type: ignore[no-any-return]
        except Exception:
            pass

    return None


def load_records(
    sources: list[str] | None,
    start_time: float | None = None,
    end_time: float | None = None,
    component_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Read log records from files or stdin, filtering by time and component."""
    records: list[dict[str, Any]] = []

    def process_stream(stream: TextIO) -> None:
        for line in stream:
            record = extract_json_record(line)
            if not record:
                continue

            # Identify if this is a request log record
            op_type = record.get("op_type") or record.get("operation")
            if not op_type:
                continue

            # Component filter
            if component_filter:
                comp = str(record.get("component", ""))
                if component_filter.lower() not in comp.lower():
                    continue

            # Timestamp extraction (check ts, t_received, t_completed, timestamp)
            rec_ts = (
                parse_timestamp(record.get("t_received"))
                or parse_timestamp(record.get("ts"))
                or parse_timestamp(record.get("timestamp"))
                or parse_timestamp(record.get("t_completed"))
            )

            # Time range filter
            if rec_ts is not None:
                if start_time is not None and rec_ts < start_time:
                    continue
                if end_time is not None and rec_ts > end_time:
                    continue

            # Normalize duration_ms
            duration_ms = record.get("duration_ms")
            if duration_ms is None:
                t_rec = parse_timestamp(record.get("t_received"))
                t_comp = parse_timestamp(record.get("t_completed"))
                if t_rec is not None and t_comp is not None:
                    duration_ms = (t_comp - t_rec) * 1000.0

            if duration_ms is not None:
                record["_duration_ms"] = float(duration_ms)
                record["_ts"] = rec_ts
                record["_op_type"] = str(op_type).upper()
                records.append(record)

    if not sources or sources == ["-"]:
        process_stream(sys.stdin)
    else:
        for path in sources:
            if not os.path.exists(path):
                print(f"Warning: Log file not found: {path}", file=sys.stderr)
                continue
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                process_stream(f)

    return records


def calculate_metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute aggregate statistics for records grouped by operation type."""
    by_op: dict[str, list[float]] = defaultdict(list)
    by_op_status: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    all_durations: list[float] = []

    min_ts: float | None = None
    max_ts: float | None = None

    for r in records:
        op = r["_op_type"]
        dur = r["_duration_ms"]
        ts = r["_ts"]
        status = str(r.get("status", "OK")).upper()

        by_op[op].append(dur)
        by_op_status[op][status] += 1
        all_durations.append(dur)

        if ts is not None:
            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts is None or ts > max_ts:
                max_ts = ts

    # Determine time window in minutes for TPM
    if min_ts is not None and max_ts is not None and max_ts > min_ts:
        window_minutes = (max_ts - min_ts) / 60.0
    else:
        window_minutes = 1.0  # fallback to avoid zero division

    metrics: dict[str, Any] = {
        "time_window_seconds": round((max_ts - min_ts), 2) if (min_ts and max_ts) else 0.0,
        "time_window_minutes": round(window_minutes, 3),
        "total_operations": len(records),
        "operations": {},
    }

    op_keys = sorted(by_op.keys())
    for op in op_keys:
        durations = sorted(by_op[op])
        n = len(durations)
        if n == 0:
            continue

        avg_dur = sum(durations) / n
        std_dur = statistics.stdev(durations) if n > 1 else 0.0
        tpm = n / window_minutes if window_minutes > 0 else 0.0

        p50 = durations[int(n * 0.50)]
        p95 = durations[min(int(n * 0.95), n - 1)]
        p99 = durations[min(int(n * 0.99), n - 1)]

        success_count = sum(
            c for s, c in by_op_status[op].items() if s in ("OK", "SUCCESS", "ACCEPTED", "MODIFIED", "RESERVED")
        )
        error_count = sum(
            c for s, c in by_op_status[op].items() if s in ("ERROR", "TIMEOUT", "FAILED", "REJECTED")
        )

        metrics["operations"][op] = {
            "count": n,
            "success_count": success_count,
            "error_count": error_count,
            "tpm": round(tpm, 2),
            "avg_ms": round(avg_dur, 3),
            "std_ms": round(std_dur, 3),
            "min_ms": round(durations[0], 3),
            "p50_ms": round(p50, 3),
            "p95_ms": round(p95, 3),
            "p99_ms": round(p99, 3),
            "max_ms": round(durations[-1], 3),
        }

    # Summary overall
    if all_durations:
        all_sorted = sorted(all_durations)
        total_n = len(all_sorted)
        total_avg = sum(all_sorted) / total_n
        total_std = statistics.stdev(all_sorted) if total_n > 1 else 0.0
        total_tpm = total_n / window_minutes

        metrics["summary"] = {
            "count": total_n,
            "tpm": round(total_tpm, 2),
            "avg_ms": round(total_avg, 3),
            "std_ms": round(total_std, 3),
            "min_ms": round(all_sorted[0], 3),
            "p50_ms": round(all_sorted[int(total_n * 0.50)], 3),
            "p95_ms": round(all_sorted[min(int(total_n * 0.95), total_n - 1)], 3),
            "p99_ms": round(all_sorted[min(int(total_n * 0.99), total_n - 1)], 3),
            "max_ms": round(all_sorted[-1], 3),
        }

    return metrics


# ── Formatting and Output ─────────────────────────────────────────────


def format_table(metrics: dict[str, Any], title: str = "SRB PERFORMANCE METRICS") -> str:
    """Format single dataset metrics as an ASCII table."""
    ops = metrics.get("operations", {})
    if not ops:
        return f"{title}\nNo matching request records found."

    lines: list[str] = []
    lines.append("=" * 88)
    lines.append(f"{title:^88}")
    lines.append(
        f"Window: {metrics.get('time_window_seconds', 0)}s ({metrics.get('time_window_minutes', 0)} min) | Total Records: {metrics.get('total_operations', 0)}"
    )
    lines.append("-" * 88)
    header = (
        f"{'Operation':<12} | {'Count':>6} | {'TPM':>8} | {'Avg (ms)':>9} | "
        f"{'Std (ms)':>9} | {'p50 (ms)':>9} | {'p95 (ms)':>9} | {'Max (ms)':>9}"
    )
    lines.append(header)
    lines.append("-" * 88)

    for op, data in ops.items():
        row = (
            f"{op:<12} | {data['count']:>6} | {data['tpm']:>8.1f} | {data['avg_ms']:>9.2f} | "
            f"{data['std_ms']:>9.2f} | {data['p50_ms']:>9.2f} | {data['p95_ms']:>9.2f} | {data['max_ms']:>9.2f}"
        )
        lines.append(row)

    if "summary" in metrics:
        lines.append("-" * 88)
        s = metrics["summary"]
        tot_row = (
            f"{'TOTAL / ALL':<12} | {s['count']:>6} | {s['tpm']:>8.1f} | {s['avg_ms']:>9.2f} | "
            f"{s['std_ms']:>9.2f} | {s['p50_ms']:>9.2f} | {s['p95_ms']:>9.2f} | {s['max_ms']:>9.2f}"
        )
        lines.append(tot_row)

    lines.append("=" * 88)
    return "\n".join(lines)


def format_comparison_table(
    m1: dict[str, Any], m2: dict[str, Any], label1: str, label2: str
) -> str:
    """Format side-by-side comparison between two runs (e.g., 1 RMC vs 3 RMC)."""
    ops1 = m1.get("operations", {})
    ops2 = m2.get("operations", {})
    all_ops = sorted(set(ops1.keys()) | set(ops2.keys()))

    lines: list[str] = []
    lines.append("=" * 96)
    lines.append(f"{'SRB SCALABILITY COMPARISON: ' + label1 + ' vs ' + label2:^96}")
    lines.append("-" * 96)
    header = (
        f"{'Operation':<10} | {'Metric':<10} | {label1:>12} | {label2:>12} | {'Diff':>12} | {'% Change':>12}"
    )
    lines.append(header)
    lines.append("-" * 96)

    def add_metric_rows(op_name: str, d1: dict[str, Any], d2: dict[str, Any]) -> None:
        metrics_to_show = [
            ("Avg (ms)", "avg_ms", "lower_is_better"),
            ("Std (ms)", "std_ms", "lower_is_better"),
            ("p95 (ms)", "p95_ms", "lower_is_better"),
            ("TPM", "tpm", "higher_is_better"),
        ]
        for idx, (label, key, direction) in enumerate(metrics_to_show):
            v1 = d1.get(key, 0.0)
            v2 = d2.get(key, 0.0)
            diff = v2 - v1
            pct = ((v2 - v1) / v1 * 100.0) if v1 > 0 else 0.0
            pct_sign = "+" if pct > 0 else ""

            op_label = op_name if idx == 0 else ""
            lines.append(
                f"{op_label:<10} | {label:<10} | {v1:>12.2f} | {v2:>12.2f} | {diff:>+12.2f} | {pct_sign + f'{pct:.1f}%':>12}"
            )
        lines.append("-" * 96)

    for op in all_ops:
        d1 = ops1.get(op, {})
        d2 = ops2.get(op, {})
        add_metric_rows(op, d1, d2)

    # Summary row
    if "summary" in m1 and "summary" in m2:
        add_metric_rows("TOTAL", m1["summary"], m2["summary"])

    lines.append("=" * 96)
    return "\n".join(lines)


def export_csv(metrics: dict[str, Any]) -> str:
    """Export metrics as CSV string."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "operation",
        "count",
        "success_count",
        "error_count",
        "tpm",
        "avg_ms",
        "std_ms",
        "min_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
    ])

    for op, data in metrics.get("operations", {}).items():
        writer.writerow([
            op,
            data["count"],
            data["success_count"],
            data["error_count"],
            data["tpm"],
            data["avg_ms"],
            data["std_ms"],
            data["min_ms"],
            data["p50_ms"],
            data["p95_ms"],
            data["p99_ms"],
            data["max_ms"],
        ])

    if "summary" in metrics:
        s = metrics["summary"]
        writer.writerow([
            "TOTAL",
            s["count"],
            "",
            "",
            s["tpm"],
            s["avg_ms"],
            s["std_ms"],
            s["min_ms"],
            s["p50_ms"],
            s["p95_ms"],
            s["p99_ms"],
            s["max_ms"],
        ])

    return output.getvalue()


# ── Main CLI ──────────────────────────────────────────────────────────


def main() -> None:
    """CLI entry point for metrics aggregation."""
    parser = argparse.ArgumentParser(
        description="Aggregate and compare SRB performance metrics from structured JSON logs."
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=[],
        help="Log files to read (default: read from stdin).",
    )
    parser.add_argument(
        "--from",
        dest="start_time",
        type=str,
        default=None,
        help="Filter logs from timestamp (ISO 8601 or epoch float).",
    )
    parser.add_argument(
        "--to",
        dest="end_time",
        type=str,
        default=None,
        help="Filter logs up to timestamp (ISO 8601 or epoch float).",
    )
    parser.add_argument(
        "--component",
        type=str,
        default=None,
        help="Filter by component name (e.g. 'gt' or 'persistence').",
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("FILE1", "FILE2"),
        default=None,
        help="Compare two log files (e.g. 1 RMC vs 3 RMC).",
    )
    parser.add_argument(
        "--labels",
        nargs=2,
        metavar=("LABEL1", "LABEL2"),
        default=["1 RMC", "3 RMC"],
        help="Labels for comparison datasets (default: '1 RMC' '3 RMC').",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="table",
        help="Output format to stdout (default: table).",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default=None,
        help="Path to write JSON output report.",
    )
    parser.add_argument(
        "--csv-out",
        type=str,
        default=None,
        help="Path to write CSV output report.",
    )

    args = parser.parse_args()

    t_start = parse_timestamp(args.start_time)
    t_end = parse_timestamp(args.end_time)

    # ── Comparison Mode ───────────────────────────────────────────────
    if args.compare:
        file1, file2 = args.compare
        label1, label2 = args.labels[0], args.labels[1]

        records1 = load_records([file1], start_time=t_start, end_time=t_end, component_filter=args.component)
        records2 = load_records([file2], start_time=t_start, end_time=t_end, component_filter=args.component)

        metrics1 = calculate_metrics(records1)
        metrics2 = calculate_metrics(records2)

        comparison_data = {
            "label1": label1,
            "metrics1": metrics1,
            "label2": label2,
            "metrics2": metrics2,
        }

        if args.format == "json":
            print(json.dumps(comparison_data, indent=2))
        elif args.format == "csv":
            print(f"# Dataset: {label1}")
            print(export_csv(metrics1))
            print(f"# Dataset: {label2}")
            print(export_csv(metrics2))
        else:
            print(format_comparison_table(metrics1, metrics2, label1, label2))

        if args.json_out:
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(comparison_data, f, indent=2)
            print(f"Comparison JSON written to {args.json_out}")

        return

    # ── Single Run Mode ───────────────────────────────────────────────
    records = load_records(args.files, start_time=t_start, end_time=t_end, component_filter=args.component)
    metrics = calculate_metrics(records)

    # Stdout output
    if args.format == "json":
        print(json.dumps(metrics, indent=2))
    elif args.format == "csv":
        print(export_csv(metrics))
    else:
        print(format_table(metrics))

    # File outputs if requested
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        print(f"JSON metrics written to {args.json_out}")

    if args.csv_out:
        with open(args.csv_out, "w", encoding="utf-8") as f:
            f.write(export_csv(metrics))
        print(f"CSV metrics written to {args.csv_out}")


if __name__ == "__main__":
    main()
