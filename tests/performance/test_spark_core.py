import gc
import logging
import queue
import statistics
import threading
import time
import unittest

import psutil
import pytest
from pyhive import hive

from tests.performance.conftest import _HOST, _PORT, _USER, _AUTH
from tests.performance.utils.memory_utils import profile_memory
from tests.performance.utils.spark_metrics import get_executor_metrics

log = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.performance,
    pytest.mark.skip_profile("spark_session"),
]


@pytest.fixture(scope="module")
def cleanup_ddl_tables(thrift_connection):
    """Drops all scratch tables created by TestDDLAndWritePathPerformance."""
    yield
    cursor = thrift_connection.cursor()
    for table in ("perf_ctas_out", "perf_insert_target", "perf_drop_scratch"):
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    cursor.close()


# ---------------------------------------------------------------------------
# Benchmark latency for connecting, creating tables, and querying via Thrift.
# ---------------------------------------------------------------------------
class TestConnectionAndQueryPerformance(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def _inject_fixtures(self, thrift_connection, spark_backend, reference_dataset):
        self.conn = thrift_connection
        self.backend = spark_backend
        self.ref_table = reference_dataset

    def test_connection_establishment_time(self):
        """Measures how long a single Thrift connection takes to open."""
        start = time.perf_counter()
        conn = hive.Connection(
            host=_HOST,
            port=_PORT,
            username=_USER,
            auth=_AUTH,
        )
        elapsed = time.perf_counter() - start
        conn.close()
        log.info("[perf][%s] connection establishment: %.3fs", self.backend, elapsed)
        self.assertLess(elapsed, 10, f"Connection took {elapsed:.3f}s, expected < 10s")

    def test_query_run_time(self):
        """Measures COUNT(*) latency against the fixed reference dataset."""
        cursor = self.conn.cursor()
        start = time.perf_counter()
        cursor.execute(f"SELECT COUNT(*) FROM {self.ref_table}")
        result = cursor.fetchall()
        elapsed = time.perf_counter() - start
        cursor.close()

        log.info(
            "[perf][%s] COUNT(*) on %s: %.3fs, rows: %d",
            self.backend, self.ref_table, elapsed, result[0][0],
        )
        self.assertEqual(result[0][0], 10000, f"Expected 10000 rows, got {result[0][0]}")
        self.assertLess(elapsed, 5, f"Query took {elapsed:.3f}s, expected < 5s")
        cursor.close()


# ---------------------------------------------------------------------------
# Tests behavior under concurrent connection and query load.
# ---------------------------------------------------------------------------
class TestConcurrentOperations(unittest.TestCase):
    """Runs 5 concurrent Spark queries and checks they all complete cleanly."""

    @pytest.fixture(autouse=True)
    def _inject_fixtures(self, spark_backend, reference_dataset):
        self.backend = spark_backend
        self.ref_table = reference_dataset

    def test_concurrent_spark_operations(self):
        results = queue.Queue()

        def run_spark_job(job_id):
            try:
                conn = hive.Connection(
                    host=_HOST,
                    port=_PORT,
                    username=_USER,
                    auth=_AUTH,
                )
                cursor = conn.cursor()
                start = time.perf_counter()
                # Partition unique data to each thread to simulate jobs doing different work
                cursor.execute(
                    f"SELECT COUNT(*), SUM(value), AVG(value) "
                    f"FROM {self.ref_table} WHERE id % 5 = {job_id}"
                )
                result = cursor.fetchall()
                elapsed = time.perf_counter() - start
                cursor.close()
                conn.close()
                results.put(("ok", job_id, elapsed, result))
            except Exception as e:
                results.put(("error", job_id, str(e)))

        num_jobs = 5
        threads = [threading.Thread(target=run_spark_job, args=(i,)) for i in range(num_jobs)]

        start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        total_elapsed = time.perf_counter() - start

        hung_threads = [t for t in threads if t.is_alive()]
        self.assertFalse(hung_threads, f"{len(hung_threads)} Spark job(s) did not finish within 30s")

        errors = []
        job_times = []
        while not results.empty():
            item = results.get()
            if item[0] == "error":
                errors.append(f"Job {item[1]}: {item[2]}")
            else:
                job_times.append(item[2])

        self.assertFalse(errors, f"Concurrent Spark jobs raised exceptions: {errors}")
        # Thread could exit early due to a swallowed exception; ensure it accounts for itself
        self.assertEqual(len(job_times), num_jobs, f"Expected {num_jobs} results, got {len(job_times)}")

        log.info(
            "[perf][%s] %d concurrent Spark jobs: total=%.3fs median=%.3fs max=%.3fs",
            self.backend, num_jobs, total_elapsed,
            statistics.median(job_times), max(job_times),
        )
        self.assertLess(total_elapsed, 60, f"Concurrent Spark jobs took {total_elapsed:.3f}s, expected < 60s")


# ---------------------------------------------------------------------------
# Measure OS-level RSS memory growth across connection, cursor, and exception
# lifecycles to catch resource leaks in the adapter.
# ---------------------------------------------------------------------------
class TestMemoryUsage(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def _inject_fixtures(self, thrift_connection, spark_backend):
        self.conn = thrift_connection
        self.backend = spark_backend

    def test_memory_concurrent_connections(self):
        """Opens 20 connections concurrently and measures OS-level RSS growth."""
        gc.collect()
        rss_before = psutil.Process().memory_info().rss

        results = queue.Queue()

        def open_run_close(job_id):
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
                results.put(("ok", job_id))
            except Exception as e:
                results.put(("error", job_id, str(e)))

        num_connections = 20
        threads = [threading.Thread(target=open_run_close, args=(i,)) for i in range(num_connections)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        hung = [t for t in threads if t.is_alive()]
        self.assertFalse(hung, f"{len(hung)} connection thread(s) did not finish within 60s")

        errors = []
        while not results.empty():
            item = results.get()
            if item[0] == "error":
                errors.append(f"Thread {item[1]}: {item[2]}")
        self.assertFalse(errors, f"Connection threads raised exceptions: {errors}")

        gc.collect()
        rss_after = psutil.Process().memory_info().rss
        rss_delta_mb = (rss_after - rss_before) / (1024 ** 2)
        log.info("[mem][%s] concurrent_connections: rss_delta=%.1f MB", self.backend, rss_delta_mb)
        self.assertLess(
            rss_delta_mb, 150,
            f"RSS grew by {rss_delta_mb:.1f} MB across 20 connections, expected < 150 MB"
        )

    def test_memory_no_leak_after_cursor_close(self):
        """Opens and closes 20 cursors sequentially on the shared connection."""
        def run():
            for _ in range(20):
                cursor = self.conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchall()
                cursor.close()

        result = profile_memory(run, label="cursor_close_leak", backend=self.backend)

        # RSS after should be less than 15% of original RSS because Python's memory
        # allocator does not return freed pages immediately.  A genuine leak would
        # exceed this threshold.
        self.assertLessEqual(
            result["rss_after_mb"], result["rss_before_mb"] * 1.15,
            f"RSS grew from {result['rss_before_mb']:.1f} MB to {result['rss_after_mb']:.1f} MB "
            f"(delta {result['rss_delta_mb']:.1f} MB) after 20 cursor close cycles — possible leak"
        )

    def test_memory_no_leak_on_exception(self):
        """
        Runs 20 cursor executions against a nonexistent table and measures
        whether the exception path leaves unreclaimed memory.
        """
        def run():
            for _ in range(20):
                cursor = self.conn.cursor()
                try:
                    cursor.execute("SELECT * FROM nonexistent_table_xyz")
                except Exception:
                    pass
                finally:
                    cursor.close()

        result = profile_memory(run, label="exception_cursor_leak", backend=self.backend)
        self.assertLess(
            result["rss_delta_mb"], 30,
            f"RSS grew by {result['rss_delta_mb']:.1f} MB across 20 exception-path cursor cycles, "
            f"expected < 30 MB — possible session resource leak on exception"
        )


# ---------------------------------------------------------------------------
# Benchmark the DDL and write-path operations dbt issues during a real run.
#
# Covers: CREATE TABLE AS SELECT, INSERT INTO, DROP TABLE, SHOW TABLES,
#         and DESCRIBE TABLE — the operations that dbt's table, incremental,
#         and introspection paths rely on.
# ---------------------------------------------------------------------------
class TestDDLAndWritePathPerformance(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def _inject_fixtures(self, thrift_connection, spark_backend, reference_dataset, cleanup_ddl_tables):
        self.conn = thrift_connection
        self.backend = spark_backend
        self.ref_table = reference_dataset

    def test_create_table_as_select(self):
        """
        Benchmarks CREATE TABLE AS SELECT — the core operation of every dbt
        table materialisation.
        """
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS perf_ctas_out")

        metrics_before = get_executor_metrics()

        start = time.perf_counter()
        cursor.execute(
            f"CREATE TABLE perf_ctas_out AS SELECT * FROM {self.ref_table}"
        )
        elapsed = time.perf_counter() - start

        metrics_after = get_executor_metrics()
        cursor.close()

        heap_delta = (
            metrics_after.get("executor_heap_used_mb", 0)
            - metrics_before.get("executor_heap_used_mb", 0)
        )
        log.info(
            "[perf][%s] CTAS from %s: %.3fs | driver_heap=%.1fMB heap_delta=%.1fMB tasks=%s",
            self.backend, self.ref_table, elapsed,
            metrics_after.get("driver_heap_used_mb", 0),
            heap_delta,
            metrics_after.get("executor_total_tasks", 0),
        )
        self.assertLess(elapsed, 30, f"CTAS took {elapsed:.3f}s, expected < 30s")
        # Executor heap shouldn't blow out on a 10k row CTAS
        self.assertLess(
            metrics_after.get("executor_heap_used_mb", 0), 512,
            f"Executor heap reached {metrics_after.get('executor_heap_used_mb', 0):.1f} MB during CTAS"
        )

    def test_insert_into_select(self):
        """
        Benchmarks INSERT INTO … SELECT — the core write operation of every
        dbt incremental materialisation.
        """
        cursor = self.conn.cursor()
        # Create a schema-matched empty target table first
        cursor.execute("DROP TABLE IF EXISTS perf_insert_target")
        cursor.execute(
            "CREATE TABLE perf_insert_target (id INT, name STRING, value DOUBLE)"
        )

        start = time.perf_counter()
        cursor.execute(
            f"INSERT INTO perf_insert_target SELECT * FROM {self.ref_table}"
        )
        elapsed = time.perf_counter() - start
        cursor.close()

        log.info("[perf][%s] INSERT INTO from %s: %.3fs", self.backend, self.ref_table, elapsed)
        self.assertLess(elapsed, 30, f"INSERT INTO took {elapsed:.3f}s, expected < 30s")

    def test_drop_table(self):
        """
        Benchmarks DROP TABLE — issued by dbt before every full-refresh
        table materialisation and at the end of incremental swaps.
        """
        cursor = self.conn.cursor()
        # Create a minimal scratch table to drop
        cursor.execute("DROP TABLE IF EXISTS perf_drop_scratch")
        cursor.execute(
            "CREATE TABLE perf_drop_scratch (id INT)"
        )

        start = time.perf_counter()
        cursor.execute("DROP TABLE IF EXISTS perf_drop_scratch")
        elapsed = time.perf_counter() - start
        cursor.close()

        log.info("[perf][%s] DROP TABLE: %.3fs", self.backend, elapsed)
        self.assertLess(elapsed, 10, f"DROP TABLE took {elapsed:.3f}s, expected < 10s")

    def test_show_tables(self):
        """
        Benchmarks SHOW TABLES — issued by dbt's relation cache and
        schema-check introspection; latency compounds across every model.
        """
        cursor = self.conn.cursor()
        start = time.perf_counter()
        cursor.execute("SHOW TABLES")
        rows = cursor.fetchall()
        elapsed = time.perf_counter() - start
        cursor.close()

        log.info("[perf][%s] SHOW TABLES: %.3fs (%d table(s))", self.backend, elapsed, len(rows))
        self.assertLess(elapsed, 5, f"SHOW TABLES took {elapsed:.3f}s, expected < 5s")

    def test_describe_table(self):
        """
        Benchmarks DESCRIBE TABLE — issued by dbt's column introspection
        during --check-cols and schema drift detection.
        """
        cursor = self.conn.cursor()
        start = time.perf_counter()
        cursor.execute(f"DESCRIBE TABLE {self.ref_table}")
        rows = cursor.fetchall()
        elapsed = time.perf_counter() - start
        cursor.close()

        log.info(
            "[perf][%s] DESCRIBE TABLE %s: %.3fs (%d column(s))",
            self.backend, self.ref_table, elapsed, len(rows),
        )
        self.assertLess(elapsed, 5, f"DESCRIBE TABLE took {elapsed:.3f}s, expected < 5s")
