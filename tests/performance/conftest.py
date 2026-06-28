import os
import pytest
from pyhive import hive

_HOST = os.environ.get("SPARK_THRIFT_HOST", "localhost")
_PORT = int(os.environ.get("SPARK_THRIFT_PORT", 10000))
_USER = os.environ.get("SPARK_THRIFT_USER", "dbt")
_AUTH = os.environ.get("SPARK_THRIFT_AUTH", "NOSASL")


@pytest.fixture(scope="session")
def thrift_connection():
    conn = hive.Connection(
        host=_HOST,
        port=_PORT,
        username=_USER,
        auth=_AUTH,
    )
    yield conn
    conn.close()
