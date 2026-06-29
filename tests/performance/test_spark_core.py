import time
import pytest
import psutil
from pyhive import hive
import queue
import threading
import statistics
from tests.performance.conftest import _HOST, _PORT, _USER, _AUTH
from tests.performance.memory_utils import profile_memory

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

'''
Measures how long a single Thrift connection takes to open.
'''
def test_connection_establishment_time():
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
    assert elapsed < 10, f"Connection took {elapsed:.3f}s, expected < 10s"

'''
Measures how long a large table creation takes (1k/10k rows)
'''
def test_large_table_creation_1k(thrift_connection):
    cursor = thrift_connection.cursor()
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
    assert elapsed < 30, f"1k table creation took {elapsed:.3f}s, expected < 30s"
    cursor.close()

def test_large_table_creation_10k(thrift_connection):
    cursor = thrift_connection.cursor()
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
    assert elapsed < 60, f"10k table creation took {elapsed:.3f}s, expected < 60s"
    cursor.close()

'''
Measures how long a query takes to run on 10k table
'''
def test_query_run_time(thrift_connection):
    cursor = thrift_connection.cursor()
    start = time.perf_counter()
    cursor.execute("SELECT COUNT(*) FROM perf_test_10k")
    result = cursor.fetchall()
    elapsed = time.perf_counter() - start

    print(f"\n[perf] query COUNT(*) on 10k rows: {elapsed:.3f}s, result: {result[0][0]}")
    assert result[0][0] == 10000, f"Expected 10000 rows, got {result[0][0]}"
    assert elapsed < 5, f"Query took {elapsed:.3f}s, expected < 5s"
    cursor.close()

'''
Test concurrent operation handling for running multiple queries simultaneously
'''
def test_concurrent_spark_operations():
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
    assert not hung_threads, f"{len(hung_threads)} Spark job(s) did not finish within 30s"

    errors = []
    job_times = []
    while not results.empty():
        item = results.get()
        if item[0] == "error":
            errors.append(f"Job {item[1]}: {item[2]}")
        else:
            job_times.append(item[2])

    assert not errors, f"Concurrent Spark jobs raised exceptions: {errors}"
    # Thread could exit early due to a swallowed exception, make sure it accounts for itself
    assert len(job_times) == num_jobs, f"Expected {num_jobs} results, got {len(job_times)}"

    print(
        f"\n[perf] {num_jobs} concurrent Spark jobs:\n"
        f"total: {total_elapsed:.3f}s\n"
        f"per-job median: {statistics.median(job_times):.3f}s\n"
        f"per-job max: {max(job_times):.3f}s"
    )
    assert total_elapsed < 60, f"Concurrent Spark jobs took {total_elapsed:.3f}s, expected < 60s"


'''
Opens 20 connections concurrently and measures OS-level RSS growth.
A failure here points to a connection-lifecycle leak in the adapter.
'''
def test_memory_concurrent_connections():
    import gc
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
    assert not hung, f"{len(hung)} connection thread(s) did not finish within 60s"

    errors = []
    while not results.empty():
        item = results.get()
        if item[0] == "error":
            errors.append(f"Thread {item[1]}: {item[2]}")
    assert not errors, f"Connection threads raised exceptions: {errors}"

    gc.collect()
    rss_after = psutil.Process().memory_info().rss
    rss_delta_mb = (rss_after - rss_before) / (1024 ** 2)
    print(f"\n[mem] concurrent_connections: rss_delta={rss_delta_mb:.1f} MB")
    assert rss_delta_mb < 150, f"RSS grew by {rss_delta_mb:.1f} MB across 20 connections, expected < 150 MB"


'''
Opens and closes 20 cursors sequentially on the shared connection.
'''
def test_memory_no_leak_after_cursor_close(thrift_connection):
    def run():
        for _ in range(20):
            cursor = thrift_connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchall()
            cursor.close()

    result = profile_memory(run, label="cursor_close_leak")
    assert result["rss_after_mb"] <= result["rss_before_mb"] * 1.15, (
        f"RSS grew from {result['rss_before_mb']:.1f} MB to {result['rss_after_mb']:.1f} MB "
        f"(delta {result['rss_delta_mb']:.1f} MB) after 20 cursor close cycles — possible leak"
    )


'''
Runs 20 cursor executions against a nonexistent table
Measure if an exception raised during SQL execution leaves any unreclaimed memory (accumulation into memory leak )
Failure means adapter not succssfully cleaning up cursors. 
'''
def test_memory_no_leak_on_exception(thrift_connection):
    def run():
        for _ in range(20):
            cursor = thrift_connection.cursor()
            try:
                cursor.execute("SELECT * FROM nonexistent_table_xyz")
            except Exception:
                pass
            finally:
                cursor.close()

    result = profile_memory(run, label="exception_cursor_leak")
    assert result["rss_delta_mb"] < 30, (
        f"RSS grew by {result['rss_delta_mb']:.1f} MB across 20 exception-path cursor cycles, "
        f"expected < 30 MB — possible session resource leak on exception"
    )