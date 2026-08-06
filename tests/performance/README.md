# Performance Tests

Benchmarks that verify the dbt-watsonx-spark adapter stays within acceptable latency and memory bounds over a Spark Thrift server.

## What is covered

| Group                     | What is measured                                                                                                   | Passes when                                   |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------ | --------------------------------------------- |
| **Connection & Query**    | Time to open a Thrift connection; `COUNT(*)` on 10k rows                                                           | Connect < 10s, query < 5s                     |
| **Concurrent Operations** | 5 parallel Spark jobs running simultaneously                                                                       | All complete within 60s, no errors            |
| **Memory**                | RSS growth across 20 concurrent connections, normal cursor close cycles, and exception paths                       | No significant memory growth after cleanup    |
| **DDL & Write Path**      | `CTAS`, `INSERT INTO`, `DROP TABLE`, `SHOW TABLES`, `DESCRIBE TABLE` — the exact SQL dbt issues during a model run | Each operation within its time budget (5–30s) |
| **Thrift Server**         | Round-trip latency (10 iterations), 5 concurrent connections, 10 sequential queries, full 10k-row fetch            | Median latency < 2s, totals within budget     |

## Testing locally

```bash
docker compose up -d dbt-hive-metastore dbt-spark3-thrift && \
docker compose logs -f dbt-spark3-thrift | grep -m1 "HiveThriftServer2 started"

SPARK_THRIFT_HOST=localhost SPARK_THRIFT_PORT=10000 \
SPARK_THRIFT_USER=dbt SPARK_THRIFT_AUTH=NOSASL SPARK_BACKEND=standard \
python3 -m pytest --csv performance_results.csv -v -m performance tests/performance

# Run this for formatted/filtered results
python3 tests/performance/summarise_results.py \
  --input performance_results.csv \
  --output tests/performance/performance_results.csv
```

## Recording results

Every run **should** be recorded on the PR

**Locally:**

1. Run the two commands under _Testing locally_ above.
2. Commit the updated `tests/performance/performance_results.csv`.
3. Paste the block below into your PR description (or a new PR comment), filling in the table from the CSV:

   ````markdown
   ### Performance run — <date> / Python <version> / backend <standard|gluten>

   Command:

   ```bash
   python3 -m pytest --csv performance_results.csv -v -m performance tests/performance
   python3 tests/performance/summarise_results.py \
     --input performance_results.csv \
     --output tests/performance/performance_results.csv
   ```

   | class | test | duration_s | threshold | result |
   | ----- | ---- | ---------- | --------- | ------ |
   | …     | …    | …          | …         | …      |
   ````

**In CI:** the `Performance Tests` workflow runs `summarise_results.py` after pytest and uploads the cleaned `performance_results.csv` as an artifact (`performance_results_<python-version>-<timestamp>.csv`) — grab it from the workflow run for any push that didn't come with a local paste.

The CSV contains: `class`, `test`, `description`, `status`, `duration_s`, `threshold`, `result` (`PASS` / `WARN` / `FAIL`), `notes`.
