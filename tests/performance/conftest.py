import os
import pathlib
import pytest
from pyhive import hive

_HOST = os.environ.get("SPARK_THRIFT_HOST", "localhost")
_PORT = int(os.environ.get("SPARK_THRIFT_PORT", 10000))
_USER = os.environ.get("SPARK_THRIFT_USER", "dbt")
_AUTH = os.environ.get("SPARK_THRIFT_AUTH", "NOSASL")

_SEEDS_DIR = pathlib.Path(__file__).parent / "seeds"
_REF_TABLE = "perf_benchmark_ref"


@pytest.fixture(scope="session")
def thrift_connection():
    conn = hive.Connection(host=_HOST, port=_PORT, username=_USER, auth=_AUTH)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def spark_backend():
    """
    Label for the Spark execution backend under test.

    Set the SPARK_BACKEND environment variable to 'gluten' when running
    against a Gluten-enabled cluster.  Defaults to 'standard'.  The value
    is injected into every [perf] / [mem] print line so the CSV artifact
    carries backend metadata for every benchmark result.
    """
    return os.environ.get("SPARK_BACKEND", "standard")


@pytest.fixture(scope="session")
def reference_dataset(thrift_connection):
    """
    Seeds the reference table once per session for benchmarking

    The table definition lives in seeds/reference_dataset.sql and is
    immutable — tests MUST NOT mutate or recreate this table.  Using
    CREATE TABLE IF NOT EXISTS makes the fixture idempotent if a prior
    run left the table behind.
    """
    sql = (_SEEDS_DIR / "reference_dataset.sql").read_text()
    cursor = thrift_connection.cursor()
    cursor.execute(sql)
    cursor.close()
    yield _REF_TABLE
    cursor = thrift_connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {_REF_TABLE}")
    cursor.close()
