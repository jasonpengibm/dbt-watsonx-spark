-- Canonical reference dataset for all performance benchmarks.
--
-- Schema contract (do NOT change column names or types without bumping
-- the table name, because every benchmark test depends on this layout):
--
--   id    INT     -- sequential integer key 0..9999
--   name  STRING  -- synthetic label: 'name_0', 'name_1', …, 'name_9999'
--   value DOUBLE  -- id * 1.5  (predictable, monotonically increasing)
--
-- The expression is purely deterministic: same cluster, same data, every run.
-- Tests MUST NOT recreate or mutate this table; read it as-is.

CREATE TABLE IF NOT EXISTS perf_benchmark_ref
AS SELECT
    CAST(id AS INT)                      AS id,
    CONCAT('name_', CAST(id AS STRING))  AS name,
    CAST(id AS DOUBLE) * 1.5             AS value
FROM (SELECT explode(sequence(0, 9999)) AS id) t
