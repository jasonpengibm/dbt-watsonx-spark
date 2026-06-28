import os
import time
import queue
import threading
import statistics
import pytest
from pyhive import hive

_HOST = os.environ.get("SPARK_THRIFT_HOST", "localhost")
_PORT = int(os.environ.get("SPARK_THRIFT_PORT", 10000))
_USER = os.environ.get("SPARK_THRIFT_USER", "dbt")
_AUTH = os.environ.get("SPARK_THRIFT_AUTH", "NOSASL")

pytestmark = [
    pytest.mark.performance,
    pytest.mark.skip_profile("spark_session", "databricks_cluster", "databricks_sql_endpoint", "databricks_http_cluster"),
]

'''
Test file should be runnable in isolation, so recreate table to avoid 'no table found' error
'''
@pytest.fixture(scope="module", autouse=True)
def setup_large_table(thrift_connection):
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
    cursor.execute(sql)
    cursor.close()
    yield
    cursor = thrift_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS perf_test_10k")
    cursor.close()

'''
Records per query latency (send + receive), track median for consistency and max for worst case behavior
Asserts median latency < 2s & max latency < 5s
'''
def test_query_round_trip_latency(thrift_connection):
    cursor = thrift_connection.cursor()
    latencies = []

    for _ in range(10):
        start = time.perf_counter()
        cursor.execute("SELECT 1")
        cursor.fetchall()
        latencies.append(time.perf_counter() - start)

    cursor.close()
    median_latency = statistics.median(latencies)
    max_latency = max(latencies)

    print(f"\n[perf] round-trip latency — median: {median_latency:.3f}s, max: {max_latency:.3f}s")
    assert median_latency < 2, f"Median latency {median_latency:.3f}s exceeded 2s"
    assert max_latency < 5, f"Max latency {max_latency:.3f}s exceeded 5s"

'''
Test connection establishment time under concurrent connections
Asserts all connections complete within 15s and no exceptions are raised
'''
def test_concurrent_connections():
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
    
    # open 5 concurrent connections 
    threads = [threading.Thread(target=connect_and_query) for _ in range(5)]

    start = time.perf_counter()

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    elapsed = time.perf_counter() - start

    hung_threads = [t for t in threads if t.is_alive()]
    assert not hung_threads, f"{len(hung_threads)} threads did not complete within 15s"

    print(f"\n[perf] 5 concurrent connections: {elapsed:.3f}s")
    assert elapsed < 15, f"Concurrent connections took {elapsed:.3f}s, expected < 15s"

    errors = []
    while not results.empty():
        status, exc = results.get()
        if status == "error":
            errors.append(str(exc))
    
    assert not errors, f"Errors in concurrent connections: {errors}"

'''
Checks if the Thrift server slows down or accumulates latency over 10 sequential queries on the same session
Asserts time < 20s
'''
def test_concurrent_queries(thrift_connection):
    cursor = thrift_connection.cursor()
    start = time.perf_counter()
    for _ in range(10):
        cursor.execute("SELECT 1")
        cursor.fetchall()
    elapsed = time.perf_counter() - start
    cursor.close()
    
    print(f"\n10 sequential queries on single connection: {elapsed:.3f}s")
    assert elapsed < 20, f"10 sequential queries took {elapsed:.3f}s, expected < 20s"

'''
Test the Thrift layer's ability to stream 10,000 rows back to the client without dropping data or timing out
'''
def test_large_result_set(thrift_connection):
    cursor = thrift_connection.cursor()

    start = time.perf_counter()
    cursor.execute("SELECT * FROM perf_test_10k")
    rows = cursor.fetchall()
    elapsed = time.perf_counter() - start

    cursor.close()

    print(f"\nlarge result set: {elapsed:.3f}s, rows received: {len(rows)}")
    assert len(rows) == 10000, f"Expected 10000 rows, got {len(rows)}"
    assert elapsed < 10, f"Large result set fetch took {elapsed:.3f}s, expected < 10s"


