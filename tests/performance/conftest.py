import pytest 
from pyhive import hive 

@pytest.fixture(scope="session")
def thrift_connection():
    conn = hive.Connection(
        host="spark_db",
        port=10000,
        username="dbt",
        auth="NOSASL"
    )
    yield conn
    conn.close()