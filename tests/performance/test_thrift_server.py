import queue
import statistics
import threading
import time
import unittest

import pytest
from pyhive import hive

from tests.performance.conftest import _HOST, _PORT, _USER, _AUTH

pytestmark = [
    pytest.mark.performance,
    pytest.mark.skip_profile(
        "spark_session",
        "databricks_cluster",
        "databricks_sql_endpoint",
        "databricks_http_cluster",
    ),
]


# ---------------------------------------------------------------------------
# Thrift transport layer benchmarks.
# Tests are Thrift-specific and must not run against non-Thrift profiles.
# ---------------------------------------------------------------------------
class TestThriftServerPerformance(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def _inject_fixtures(self, thrift_connection, spark_backend, reference_dataset):
        self.conn = thrift_connection
        self.backend = spark_backend
        self.ref_table = reference_dataset

    def test_query_round_trip_latency(self):
        """
        Records per-query latency (send + receive) over 10 iterations.
        Tracks median for consistency and max for worst-case behaviour.
        Asserts median < 2s and max < 5s.
        """
        cursor = self.conn.cursor()
        latencies = []

        for _ in range(10):
            start = time.perf_counter()
            cursor.execute("SELECT 1")
            cursor.fetchall()
            latencies.append(time.perf_counter() - start)

        cursor.close()
        median_latency = statistics.median(latencies)
        max_latency = max(latencies)

        print(
            f"\n[perf][{self.backend}] round-trip latency — "
            f"median: {median_latency:.3f}s, max: {max_latency:.3f}s"
        )
        self.assertLess(median_latency, 2, f"Median latency {median_latency:.3f}s exceeded 2s")
        self.assertLess(max_latency, 5, f"Max latency {max_latency:.3f}s exceeded 5s")

    def test_concurrent_connections(self):
        """
        Tests connection establishment time under concurrent load.
        Asserts all 5 connections complete within 15s with no exceptions.
        """
        results = queue.Queue()

        def connect_and_query():
            try:
                conn = hive.Connection(
                    host=_HOST,
                    port=_PORT,
                    username=_USER,
                    auth=_AUTH,
                )
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchall()
                cursor.close()
                conn.close()
                results.put(("ok", None))
            except Exception as e:
                results.put(("error", e))

        threads = [threading.Thread(target=connect_and_query) for _ in range(5)]

        start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        elapsed = time.perf_counter() - start

        # Check for hung threads before the elapsed assertion so a stuck
        # thread is reported as a hang rather than a slow run.
        hung_threads = [t for t in threads if t.is_alive()]
        self.assertFalse(hung_threads, f"{len(hung_threads)} threads did not complete within 15s")

        print(f"\n[perf][{self.backend}] 5 concurrent connections: {elapsed:.3f}s")
        self.assertLess(elapsed, 15, f"Concurrent connections took {elapsed:.3f}s, expected < 15s")

        errors = []
        while not results.empty():
            status, exc = results.get()
            if status == "error":
                errors.append(str(exc))
        self.assertFalse(errors, f"Errors in concurrent connections: {errors}")

    def test_concurrent_queries(self):
        """
        Checks whether the Thrift server slows down or accumulates latency
        over 10 sequential queries on the same session.
        Asserts total time < 20s.
        """
        cursor = self.conn.cursor()
        start = time.perf_counter()
        for _ in range(10):
            cursor.execute("SELECT 1")
            cursor.fetchall()
        elapsed = time.perf_counter() - start
        cursor.close()

        print(f"\n[perf][{self.backend}] 10 sequential queries on single connection: {elapsed:.3f}s")
        self.assertLess(elapsed, 20, f"10 sequential queries took {elapsed:.3f}s, expected < 20s")

    def test_large_result_set(self):
        """
        Tests the Thrift layer's ability to stream all rows of the reference
        dataset back to the client without dropping data or timing out.
        """
        cursor = self.conn.cursor()

        start = time.perf_counter()
        cursor.execute(f"SELECT * FROM {self.ref_table}")
        rows = cursor.fetchall()
        elapsed = time.perf_counter() - start
        cursor.close()

        print(
            f"\n[perf][{self.backend}] large result set from {self.ref_table}: "
            f"{elapsed:.3f}s, rows received: {len(rows)}"
        )
        self.assertEqual(len(rows), 10000, f"Expected 10000 rows, got {len(rows)}")
        self.assertLess(elapsed, 10, f"Large result set fetch took {elapsed:.3f}s, expected < 10s")
