"""
Converts the raw pytest-csv output into a clean, readable summary CSV.

Usage (run from repo root after the test suite):

    python tests/performance/summarise_results.py \
        --input  performance_results.csv \
        --output tests/performance/performance_results.csv

'result' is PASS / FAIL / WARN.  WARN is emitted when a test passes but its
measured duration is within 20 % of the threshold — a useful early-warning
signal before a threshold breach occurs.
"""

import argparse
import csv
import re
import sys

THRESHOLDS = {
    # test_spark_core.py
    "test_connection_establishment_time":  10,
    "test_query_run_time":                  5,
    "test_concurrent_spark_operations":    60,
    "test_memory_concurrent_connections":  None,   # budget is RSS MB, not seconds
    "test_memory_no_leak_after_cursor_close": None,
    "test_memory_no_leak_on_exception":    None,
    "test_create_table_as_select":         30,
    "test_insert_into_select":             30,
    "test_drop_table":                     10,
    "test_show_tables":                     5,

    "test_describe_table":                  5,
    # test_thrift_server.py
    "test_query_round_trip_latency":        2,     # median < 2s (worst-case max < 5s)
    "test_concurrent_connections":         15,
    "test_concurrent_queries":             20,
    "test_large_result_set":               10,
}

LABELS = {
    "test_connection_establishment_time":     "Connection establishment time",
    "test_query_run_time":                    "COUNT(*) query on reference dataset",
    "test_concurrent_spark_operations":       "5 concurrent Spark jobs (total wall time)",
    "test_memory_concurrent_connections":     "RSS delta — 20 concurrent connections",
    "test_memory_no_leak_after_cursor_close": "RSS after 20 sequential cursor close cycles",
    "test_memory_no_leak_on_exception":       "RSS delta — 20 exception-path cursor cycles",
    "test_create_table_as_select":            "CREATE TABLE AS SELECT (CTAS)",
    "test_insert_into_select":                "INSERT INTO … SELECT",
    "test_drop_table":                        "DROP TABLE",
    "test_show_tables":                       "SHOW TABLES",
    "test_describe_table":                    "DESCRIBE TABLE",
    "test_query_round_trip_latency":          "Round-trip latency median over 10 SELECT 1s",
    "test_concurrent_connections":            "5 concurrent Thrift connections (total wall time)",
    "test_concurrent_queries":                "10 sequential queries on single connection",
    "test_large_result_set":                  "Full fetch of 10 000-row result set",
}


def _parse_class(test_id: str) -> str:
    """Extract the TestClass name from a pytest node id."""
    parts = test_id.split("::")
    if len(parts) >= 3:
        return parts[-2]
    return ""


def _result(status: str, duration: float, threshold) -> str:
    if status != "passed":
        return "FAIL"
    if threshold is None:
        return "PASS"
    warn_floor = threshold * 0.80          
    if duration >= warn_floor:
        return "WARN"
    return "PASS"


def _threshold_label(threshold) -> str:
    if threshold is None:
        return "see log"
    return f"< {threshold}s"


def summarise(input_path: str, output_path: str) -> None:
    with open(input_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    output_rows = []
    for row in rows:
        test_name = row["name"]
        status    = row["status"]
        try:
            duration = float(row["duration"])
        except (ValueError, KeyError):
            duration = 0.0

        threshold = THRESHOLDS.get(test_name)
        label     = LABELS.get(test_name, re.sub(r"^test_", "", test_name).replace("_", " ").title())

        # Truncate long error messages to keep the CSV readable
        message = row.get("message", "").replace("\n", " ").strip()
        if len(message) > 120:
            message = message[:117] + "..."

        output_rows.append({
            "class":       _parse_class(row["id"]),
            "test":        test_name,
            "description": label,
            "status":      status,
            "duration_s":  f"{duration:.3f}",
            "threshold":   _threshold_label(threshold),
            "result":      _result(status, duration, threshold),
            "notes":       message,
        })

    fieldnames = ["class", "test", "description", "status", "duration_s", "threshold", "result", "notes"]

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    # Print a quick console summary
    passed = sum(1 for r in output_rows if r["result"] in ("PASS", "WARN"))
    failed = sum(1 for r in output_rows if r["result"] == "FAIL")
    warned = sum(1 for r in output_rows if r["result"] == "WARN")
    total  = len(output_rows)
    print(f"Summary written to {output_path}")
    print(f"  {total} tests — {passed} passed ({warned} with WARN), {failed} failed")
    if failed:
        print("\nFailed tests:")
        for r in output_rows:
            if r["result"] == "FAIL":
                print(f"  {r['class']}::{r['test']}")
                if r["notes"]:
                    print(f"    {r['notes']}")
    if warned:
        print("\nTests approaching threshold (WARN):")
        for r in output_rows:
            if r["result"] == "WARN":
                print(f"  {r['class']}::{r['test']}  {r['duration_s']}s  (threshold {r['threshold']})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input",  default="performance_results.csv",
                        help="Raw pytest-csv output (default: performance_results.csv)")
    parser.add_argument("--output", default="tests/performance/performance_results.csv",
                        help="Destination for the clean summary (default: tests/performance/performance_results.csv)")
    args = parser.parse_args()
    summarise(args.input, args.output)


if __name__ == "__main__":
    main()
