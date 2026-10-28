# Benchmarks

Method: `python benchmarks/run.py --profile <small|medium>` creates isolated `*_bench`
databases, generates and loads the synthetic source, and times each step once (search and API
latency over many requests). Results are from **one machine** (2 vCPU, 7.8 GiB RAM,
Python 3.13.17, PostgreSQL 16 native) and are not a capacity claim. Raw results:
[benchmarks/results](../benchmarks/results/).

## Small profile (seed 42, 25,283 source rows) — measured 2026-10-28T02:42:13+00:00

| Step | Result |
|---|---|
| Generate + load source | 0.059 s + 0.316 s |
| Full batch extraction (11 tables) | 25,283 rows in 1.346 s ≈ 18,784 rows/s, 0 failed |
| Re-extraction of the same window | 0 inserted, 25,283 skipped as duplicates, 1.133 s |
| dbt build, full refresh (models + tests) | exit 0, 35.418 s |
| 1% of orders changed → extract + incremental dbt build | 18 rows in 0.032 s; dbt exit 0, 7.936 s |
| Reconciliation (row counts / source↔mart / ledger↔mart) | pass 0.067 s / pass 0.032 s / pass 0.019 s |
| Metadata collection (dbt, ingestion, schema, freshness) | 0.24 s |
| Incident evaluation on healthy data | 0.031 s, 0 events |
| Runbook hybrid search (n=60) | p50 13.82 ms, p95 16.47 ms |
| API `GET /health` (n=200) | p50 9.39 ms, p95 11.91 ms |
| API `GET /overview` (n=200) | p50 59.85 ms, p95 81.96 ms |
| API `GET /incidents?all=true` (n=200) | p50 12.92 ms, p95 16.23 ms |
| API `GET /metrics` (n=200) | p50 19.04 ms, p95 22.31 ms |

## Other measured timings (from actual runs, see their reports)

- Failure scenarios: injection → incident detection 8.7–11.6 s per scenario, dominated by one
  pipeline cycle ([results/scenarios](results/scenarios/README.md)).
- Investigator, fixture mode (not AI): median 215 ms over 35 evaluation cases
  ([evaluations/results/fixture](../evaluations/results/fixture/)). Real-model latency and token
  cost: not measured yet.

## Medium profile

In progress / to be added from `benchmarks/results/medium-*.json`.
