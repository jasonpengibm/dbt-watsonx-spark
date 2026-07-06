import time
import pytest
import psutil
from pyhive import hive
import queue
import threading
import statistics
from tests.performance.conftest import _HOST, _PORT, _USER, _AUTH
from tests.performance.utils.memory_utils import profile_memory
import unittest
import gc

pytestmark = [
    pytest.mark.performance,
    pytest.mark.skip_profile("spark_session"),
]

@pytest.fixture(scope="module", autouse=True)
def cleanup_perf_tables(thrift_connection):
    yield
    cursor = thrift_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS perf_test_1k")
    cursor.execute("DROP TABLE IF EXISTS perf_test_10k")
    cursor.close()


"""
Benchmark latency for connecting, creating tables, and
querying via Thrift.
"""
class TestConnectionAndQueryPerformance(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def _inject_fixtures(self, thrift_connection):
        self.conn = thrift_connection

    '''
    Measures how long a single Thrift connection takes to open.
    '''
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
        print(f"\n[perf] connection establishment: {elapsed:.3f}s")
        self.assertLess(elapsed, 10, f"Connection took {elapsed:.3f}s, expected < 10s")

    """
    Measures how long a 1k row table creation takes.
    """
    def test_large_table_creation_1k(self):
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS perf_test_1k")

        sql = """
            CREATE TABLE perf_test_1k
            AS SELECT
                CAST(id AS INT)                      AS id,
                CONCAT('name_', CAST(id AS STRING))  AS name,
                CAST(id AS DOUBLE) * 1.5             AS value
            FROM (SELECT explode(sequence(0, 999)) AS id) t
        """

        start = time.perf_counter()
        cursor.execute(sql)
        elapsed = time.perf_counter() - start

        print(f"\n[perf] 1k table creation: {elapsed:.3f}s")
        self.assertLess(elapsed, 30, f"1k table creation took {elapsed:.3f}s, expected < 30s")
        cursor.close()


    """
    Measures how long a 10k row table creation takes.
    """
    def test_large_table_creation_10k(self):
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS perf_test_10k")

        sql = """
            CREATE TABLE perf_test_10k
            AS SELECT
                CAST(id AS INT)                      AS id,
                CONCAT('name_', CAST(id AS STRING))  AS name,
                CAST(id AS DOUBLE) * 1.5             AS value
            FROM (SELECT explode(sequence(0, 9999)) AS id) t
        """

        start = time.perf_counter()
        cursor.execute(sql)
        elapsed = time.perf_counter() - start

        print(f"\n[perf] 10k table creation: {elapsed:.3f}s")
        self.assertLess(elapsed, 60, f"10k table creation took {elapsed:.3f}s, expected < 60s")
        cursor.close()

    '''
    Measures how long a query takes to run on 10k table
    '''
    def test_query_run_time(self):
        cursor = self.conn.cursor()
        start = time.perf_counter()
        cursor.execute("SELECT COUNT(*) FROM perf_test_10k")
        result = cursor.fetchall()
        elapsed = time.perf_counter() - start

        print(f"\n[perf] query COUNT(*) on 10k rows: {elapsed:.3f}s, result: {result[0][0]}")
        self.assertEqual(result[0][0], 10000, f"Expected 10000 rows, got {result[0][0]}")
        self.assertLess(elapsed, 5, f"Query took {elapsed:.3f}s, expected < 5s")
        cursor.close()


"""
Tests behavior under concurrent connection and query load 
by opening multiple threads running Spark jobs simultaneously.
"""
class TestConcurrentOperations(unittest.TestCase):
    """
    Runs 5 concurrent Spark queries and checks they all complete cleanly.
    """
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
                    f"FROM perf_test_10k WHERE id % 5 = {job_id}"
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
        # Thread could exit early due to a swallowed exception, make sure it accounts for itself
        self.assertEqual(len(job_times), num_jobs, f"Expected {num_jobs} results, got {len(job_times)}")

        print(
            f"\n[perf] {num_jobs} concurrent Spark jobs:\n"
            f"total: {total_elapsed:.3f}s\n"
            f"per-job median: {statistics.median(job_times):.3f}s\n"
            f"per-job max: {max(job_times):.3f}s"
        )
        self.assertLess(total_elapsed, 60, f"Concurrent Spark jobs took {total_elapsed:.3f}s, expected < 60s")


"""
Measure OS-level RSS memory growth across connection, cursor, and exception
lifecycles to catch resource leaks in the adapter.
"""
class TestMemoryUsage(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def _inject_fixtures(self, thrift_connection):
        self.conn = thrift_connection

    """
    Opens 20 connections concurrently and measures OS-level RSS growth.
    """
    def test_memory_concurrent_connections(self):

        # Force garbage collection before measuring so unrelated pending
        # garbage from prior tests doesn't affect the base RSS amount
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
        
        # Collect again post-run so RSS does not reflect garbage waiting to be cleaned
        gc.collect()

        # Sample RSS after operation, memory should return near starting point after all connections torn down 
        rss_after = psutil.Process().memory_info().rss
        rss_delta_mb = (rss_after - rss_before) / (1024 ** 2)
        print(f"\n[mem] concurrent_connections: rss_delta={rss_delta_mb:.1f} MB")
        self.assertLess(
            rss_delta_mb, 150,
            f"RSS grew by {rss_delta_mb:.1f} MB across 20 connections, expected < 150 MB"
        )

    """
    Opens and closes 20 cursors sequentially on the shared connection.
    """
    def test_memory_no_leak_after_cursor_close(self):
        def run():
            for _ in range(20):
                cursor = self.conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchall()
                cursor.close()

        result = profile_memory(run, label="cursor_close_leak")

        # RSS after should be less than 15% of original RSS because Python memory allocator does not 
        # return freed pages immediately. A genuine memory leak would be higher than this threshold 
        self.assertLessEqual(
            result["rss_after_mb"], result["rss_before_mb"] * 1.15,
            f"RSS grew from {result['rss_before_mb']:.1f} MB to {result['rss_after_mb']:.1f} MB "
            f"(delta {result['rss_delta_mb']:.1f} MB) after 20 cursor close cycles — possible leak"
        )

    """
    Runs 20 cursor executions against a nonexistent table and measures
    whether the exception path leaves unreclaimed memory.
    """
    def test_memory_no_leak_on_exception(self):
        def run():
            for _ in range(20):
                cursor = self.conn.cursor()
                try:
                    cursor.execute("SELECT * FROM nonexistent_table_xyz")
                except Exception:
                    pass
                finally:
                    cursor.close()

        result = profile_memory(run, label="exception_cursor_leak")
        self.assertLess(
            result["rss_delta_mb"], 30,
            f"RSS grew by {result['rss_delta_mb']:.1f} MB across 20 exception-path cursor cycles, "
            f"expected < 30 MB — possible session resource leak on exception"
        )