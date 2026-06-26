import os
import time
import pytest
from pyhive import hive

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
        host=spark_db,
        port=10000,
        username="dbt",
        auth="NOSASL",
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

    values = ", ".join(f"({i}, 'name_{i}', {i * 1.5})" for i in range(1000))
    sql = f"CREATE TABLE perf_test_1k (id INT, name STRING, value DOUBLE) AS SELECT * FROM (VALUES {values}) t(id, name, value)"

    start = time.perf_counter()
    cursor.execute(sql)
    elapsed = time.perf_counter() - start

    print(f"\n[perf] 1k table creation: {elapsed:.3f}s")
    assert elapsed < 30, f"1k table creation took {elapsed:.3f}s, expected < 30s"
    cursor.close()

def test_large_table_creation_10k(thrift_connection):
    cursor = thrift_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS perf_test_10k")

    values = ", ".join(f"({i}, 'name_{i}', {i * 1.5})" for i in range(10000))
    sql = f"CREATE TABLE perf_test_10k (id INT, name STRING, value DOUBLE) AS SELECT * FROM (VALUES {values}) t(id, name, value)"

    start = time.perf_counter()
    cursor.execute(sql)
    elapsed = time.perf_counter() - start

    print(f"\n[perf] 1k table creation: {elapsed:.3f}s")
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
