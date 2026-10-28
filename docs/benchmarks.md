# Benchmarks

Method: `python benchmarks/run.py --profile <small|medium>` creates isolated `*_bench`
databases, generates and loads the synthetic source, and times each step once (search and API
latency over many requests; API via real uvicorn over HTTP). One machine only:
2 vCPU, 7.8 GiB RAM, Python 3.13.17, PostgreSQL 16 native, same host for
database, dbt and API. Not a capacity claim; no concurrency or load testing.
Raw results: [benchmarks/results](../benchmarks/results/).

| Step | small (25,283 source rows) | medium (505,260 source rows) |
|---|---|---|
| Generate + load source | 0.06 s + 0.306 s | 1.481 s + 6.089 s |
| Full batch extraction, 11 tables | 25,283 rows, 1.329 s ≈ 19,024 rows/s | 505,260 rows, 36.502 s ≈ 13,842 rows/s |
| Re-extraction of the same window | 0 inserted, 25,283 skipped, 1.109 s | 0 inserted, 505,260 skipped, 27.565 s |
| dbt build --full-refresh (models + tests) | exit 0, 6.549 s | exit 0, 9.547 s |
| 1% of orders changed → extract | 18 rows, 0.033 s | 372 rows, 0.051 s |
| → incremental dbt build | exit 0, 5.173 s | exit 0, 7.763 s |
| Reconciliation: row counts / source↔mart / ledger↔mart | pass 0.069 s / pass 0.033 s / pass 0.018 s | pass 1.045 s / pass 0.639 s / pass 0.02 s |
| Metadata collection (dbt, ingestion, schema, freshness) | 0.302 s | 0.629 s |
| Incident evaluation on healthy data | 0.028 s, 0 events | 0.028 s, 0 events |
| Runbook hybrid search (n=60) | p50 12.83 ms, p95 14.78 ms | p50 13.86 ms, p95 18.81 ms |
| API `GET /health` (n=200) | p50 9.87 ms, p95 12.24 ms | p50 10.26 ms, p95 13.53 ms |
| API `GET /overview` (n=200) | p50 57.52 ms, p95 75.66 ms | p50 58.2 ms, p95 73.58 ms |
| API `GET /incidents?all=true` (n=200) | p50 11.14 ms, p95 14.19 ms | p50 11.14 ms, p95 13.75 ms |
| API `GET /metrics` (n=200) | p50 14.83 ms, p95 20.39 ms | p50 15.43 ms, p95 20.77 ms |

Re-extraction is slower per row than one would expect from a no-op because every page is still
read from the source and hashed; only the insert is skipped.

## Finding: staging views did not scale

The first medium run had staging and intermediate models as **views** over the generic JSONB
landing table (`DISTINCT ON` per key). The planner estimated a few hundred rows for views that
return ~170,000, and the full-refresh build of `fct_shipment_event` (a join of two such views)
was still running after more than 20 minutes when it was stopped. Materializing staging and
intermediate models as tables with `ANALYZE` (dbt_project.yml) brought the whole medium full
build to 9.547 s. All tests and the scenario suite were re-run on the change.

## Other measured timings (from actual runs)

- Failure scenarios: injection → incident detection 10.6–12.1 s, dominated by one pipeline cycle
  ([results/scenarios](results/scenarios/README.md)).
- Investigator in fixture mode (not AI): median 215 ms over 35 evaluation cases
  ([evaluations/results/fixture](../evaluations/results/fixture/)). Real-model latency and token cost:
  not measured yet.
