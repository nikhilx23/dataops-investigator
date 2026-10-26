# Progress log

Purpose: let any engineer or agent resume without repeating work. Status words are strict:
**verified** = executed here with the result shown; **implemented** = code exists, not run
end to end; **planned** = not started; **blocked** = cannot proceed in this environment.

## Build environment (2026-10-25)

| Resource | Finding |
|---|---|
| OS / CPU / RAM | Ubuntu 24.04, 2 vCPU, 7.8 GiB RAM, no swap |
| Python | 3.13.17 (virtualenv `.venv`) |
| PostgreSQL | 16.15 native (localhost only, scram-sha-256); pgvector 0.6.0 from Ubuntu archive |
| Java | OpenJDK 21.0.12 |
| Docker | daemon runs (29.9.2, Compose v5.6.1), **but image pulls fail**: Docker Hub, quay.io, public.ecr.aws, ghcr.io return 403 at the egress proxy |
| Maven Central | 403 (blocks Spark package resolution of Delta/Kafka connector jars) |
| Package indexes | PyPI, npm, Ubuntu archive, GitHub reachable |

Consequence: the lightweight profile is verified on native PostgreSQL. Anything that needs
container images or Maven artifacts (Compose runtime, Kafka, Debezium, Spark+Delta jars,
Airflow image) can be written and statically validated here, but must be run on a machine
with registry access (e.g. the GitHub Actions workflow, or a developer laptop).

## Milestone 1: environment and vertical slice — **complete**

### Implemented and verified
- Dependency baseline resolved and pinned: [docs/versions.md](versions.md).
- Business rules and metric definitions written before code: [docs/business_rules.md](business_rules.md).
- Database bootstrap (`infra/postgres/bootstrap.sql`): 4 databases, 8 least-privilege roles,
  `PUBLIC` revoked, secrets read with `\getenv` (never in argv/files). Idempotent (run 3×).
- Forward-only migrations with checksums (`dataops db migrate`); idempotent.
- Deterministic generator (`tiny`/`small`/`medium`/`large` profiles) + expected-results ledger.
- Batch ingestion (windowed, keyset pagination, overlap, retries, manifests, checksums,
  atomic publication, single-writer lock, lease expiry for crashed batches).
- dbt: 4 staging views, `mart_daily_revenue`, 35 data tests (schema + 3 singular business tests).
- Reconciliation: source↔raw counts, source↔mart, ledger↔mart; persisted to
  `reliability.reconciliation_results`.
- Health/readiness CLI for every role.
- Scripts for Unix shell and PowerShell (bootstrap, demo, health, up, down).

### Commands executed and results (this environment)

| Command | Result |
|---|---|
| `bash scripts/bootstrap_db.sh` (3 runs) | success; second and third runs apply 0 migrations |
| `python -m dataops generate --seed 42 --profile small --load` | 25,203 source rows; checksum `c7ca92be…86f2` |
| `ingest --until 2026-03-02T00:00:00+00:00` then `ingest --start 2026-03-02… --end 2026-04-01… --step 1d` (fresh DBs) | 310 batches committed, 25,400 rows extracted, 25,203 inserted, 197 overlap re-reads skipped, 0 failed |
| re-ingest of 2 days already loaded | 0 inserted, 1,723 skipped (idempotent) |
| `dbt build` | PASS=40 WARN=0 ERROR=0 |
| `dataops reconcile --ledger var/ledger/small-seed42.json` | 3/3 pass; e.g. booked revenue 55,906,035 ¢ on all three sides |
| `bash scripts/demo_m1.sh` | 3/3 reconciliation pass, ~28 s wall clock |
| `pwsh scripts/bootstrap_db.ps1` + `pwsh scripts/demo_m1.ps1` on fresh `*_fresh` databases | 3/3 pass (PowerShell 7.4.6 on Linux) |
| `pytest -q` | 29 passed (21 unit, 8 integration) |
| `ruff check .`, `ruff format --check .`, `mypy` | clean |
| `shellcheck -x scripts/*.sh infra/postgres/init/*.sh` | clean |
| PowerShell parser over `scripts/*.ps1` | all parse |
| `docker compose --profile lightweight config --quiet` | valid |

Integration tests (real PostgreSQL, isolated `*_test` databases) cover: end-to-end
reconciliation, persistence of results, loader refusing to overwrite, idempotent re-ingest,
**atomic publication under a simulated mid-batch crash** (no rows leaked, watermark
unchanged, rerun publishes once), **overlapping writer rejected**, **source drift detected
with the exact delta then corrected by an incremental run**, and role privilege boundaries.

### Bugs found by verification and fixed
- Migration runner wrapped migrations in savepoints of an implicit transaction that was never
  committed → now uses autocommit + explicit transactions.
- `REVOKE ALL ... FROM PUBLIC` also removed `TEMPORARY`; loader and dbt need it → granted
  explicitly to `wh_loader` and `wh_transform` only.
- `.env` loaders in the shell scripts overrode variables already set in the environment
  (unlike the Python loader) → now consistent: the existing environment wins.
- PowerShell `ConvertFrom-Json` converted ISO timestamps to local time → converted back to UTC.

### Known limitations
- Batch cannot observe deletes or intermediate versions (see guarantees doc).
- Changing the generator seed against an existing warehouse requires a raw-zone reset
  (no reset command yet; deliberate, because it is destructive).
- Compose runtime, app image build and the CI workflow are **not verified** (registry blocked).
- Image tags are pinned but not digest-pinned.

## Milestone 2: data engineering depth — **complete**

### 2a. Warehouse modeling on the batch path — **verified**
- Data model defined first: [docs/data_model.md](data_model.md) (grains, keys, measures,
  update/deletion policy, double-counting rules).
- Source migration 0002: ordered product change log `shop.product_versions` (trigger-maintained).
  Migration 0003: `sim.clock` for the time simulator. Rationale: [ADR 0003](adr/0003-source-change-log-and-time-simulation.md).
- Time simulator (`dataops simulate`): replays every row's state history; replay-to-end equals
  the final dataset (unit-tested, `tiny` and `small`).
- dbt: 11 staging views, 2 intermediate views, `dim_date`, `dim_customer`, `dim_warehouse`,
  `dim_product_history` (SCD2), incremental facts `fct_order_item` (point-in-time product
  version), `fct_payment_transaction`, `fct_inventory_movement`, `fct_shipment_event`, marts
  `mart_order_fulfillment`, `mart_inventory_position`, `mart_payment_reconciliation`
  (+ existing `mart_daily_revenue`). Singular tests: SCD2 intervals, point-in-time price,
  facts vs mart consistency, inventory non-negative, payment reconciliation rules.
- Fingerprints (`dataops fingerprint`) and inventory ledger reconciliation.
- Explicit destructive reset of the synthetic environment (`dataops reset --confirm-synthetic-reset`).

| Command | Result |
|---|---|
| `dbt build` (final-state data) | PASS=96 WARN=0 ERROR=0 |
| `python scripts/demo_m2.py --reset` | 30 simulated days, 31,604 raw versions (daily source changes = rows inserted every day), 6 incremental dbt builds; full-refresh fingerprints **equal** for all 9 models; 4/4 reconciliations pass; 89 s wall clock |
| `pytest -q` | 40 passed (27 unit, 13 integration) |
| `bash scripts/demo_m1.sh` after the changes | 4/4 reconciliations pass |

Integration tests added: incremental absorbs a late order-status update; full refresh equals
incremental for all fingerprinted models; marts reconcile with source and ledger; SCD2 keeps
two intermediate price changes made between extractions; no-op updates create no version.

Found by verification: day-by-day runs against a one-shot final-state source produced 13
orphan payments (orders became visible only at their final update). Fixed by the simulator
(ADR 0003), not by weakening the relationship tests.

### 2b. Full profile: CDC, Kafka, Spark + Delta, publication, Airflow — **verified** (2026-10-26)

Network: the user allowed Docker Hub, Quay and Maven Central for this environment. Docker Hub
then returned HTTP 429 (anonymous pull rate limit) for most pulls, so Kafka runs from
Debezium's Quay image. Everything below ran on the Compose stack (`DATAOPS_PG_PORT=5433`).

Implemented: CDC role + publication (source migration 0004), Compose `kafka` + `connect`
(digest-pinned), connector registration with the secret resolved in the worker
(`dataops cdc register`), CDC materializer + column-level reconciliation, source contract
`contracts/source/shop.v1.yaml`, Spark Bronze/Silver/event-time jobs (`dataops stream`),
explicit replay, Silver publication to `cdc_pub` (warehouse migration 0002), dbt
`source_path` switch, path-aware row-count reconciliation, 5 Airflow DAGs, full-profile
scripts for both shells. Design: [ADR 0004](adr/0004-cdc-and-lakehouse.md), guarantees:
[architecture/guarantees.md](architecture/guarantees.md), operations: [full_profile.md](full_profile.md).

| Command | Result |
|---|---|
| `docker compose --profile full up -d --wait postgres kafka connect` | 3/3 healthy in 34 s |
| `python scripts/verify_cdc.py` | 6/6 PASS: snapshot 2,539/2,539 rows column-equal; probe `c,u,u,d,tombstone` in LSN order; 936 changes made while Connect was stopped delivered after restart, 0 mismatches, 0 transport duplicates |
| `python scripts/verify_streaming.py` | 6/6 PASS (A) Bronze 5,533 rows = distinct offsets = Kafka end offsets; Silver = source (5,034). (B) SIGKILL of the continuous job after 75 s with 1,239 changes outstanding, restart: Bronze 6,772 = offsets, Silver = source (6,013). (C) replay digest identical. (D) 10 events dropped by the 6 h watermark in incremental runs; correction recomputes from Silver |
| `python -m dataops publish` | 6,013 rows, one transaction, 72 s (incl. Spark start) |
| `dbt build --vars '{source_path: cdc}'` | PASS=96 WARN=0 ERROR=0 |
| `python -m dataops reconcile --source-path cdc` | source vs cdc_pub 6,013 = 6,013; source vs mart 0 mismatches (booked 13,966,994 ¢) |
| Airflow 3.3.2 standalone, real scheduler | 7 DAG runs, 19 task instances, all `success`: scheduled + manual publication (serialized by `max_active_runs=1`), asset-triggered dbt build and reconciliation (4 reconciliation rows `pass`), batch extract, backfill from params (3 windows each for orders, payments) |
| `pytest -q` (native PostgreSQL) | 49 passed (30 unit, 13 integration, 6 Spark) |
| ruff, mypy, shellcheck, PowerShell parser (11 scripts), `scripts/airflow.ps1 dags list-import-errors` | clean |

Measured memory (2 vCPU / 7.8 GiB VM): Kafka 390–450 MiB, Connect 415–430 MiB, PostgreSQL
56–117 MiB, Airflow (4 processes) 833 MiB RSS, Spark driver up to 1.2 GiB RSS while
processing. Peak total ≈ 3 GiB; whole stack fits in 8 GiB when run in stages as here.
Spark local mode spends ≈ 40 s starting per run; a Bronze+Silver+events pass over one
simulated day takes 2–3 minutes on 2 vCPUs.

Bugs found by verification and fixed:
- Delete events were quarantined: under REPLICA IDENTITY DEFAULT Debezium fills non-key
  columns of a delete image with placeholders ("" and epoch). Deletes are now validated on
  the key only, and a soft delete keeps the row's last real values. Recovered with an
  explicit replay from Bronze; reconciliation then passed.
- `airflow standalone` failed because it spawns `airflow` from PATH: wrapper now exports it.
- Airflow 3 Postgres asset URIs need host and port; fixed.
- Kafka 3.9 rejected `0.0.0.0` in advertised listeners for the controller; listeners now bind
  without a host.

Not verified here (blocked or deferred):
- App container image build (`python:3.12.11-slim-bookworm` from Docker Hub: HTTP 429 on
  every attempt). The CLI ran from the host virtualenv instead; CI builds the image.
- Continuous streaming as a supervised long-running service: verified only as a process run
  for 75 s and killed; background processes do not survive between turns in this sandbox.
- Grafana/Prometheus monitoring (planned with Milestone 3 observability).
- Throughput/latency benchmarks (Milestone 6).

## Blockers
- Docker Hub anonymous pull rate limit (HTTP 429) from the build network blocks the app image
  build. Workarounds: retry later, authenticate `docker login` with a free Docker account, or
  rely on the CI build.

## Next: Milestone 3 (reliability platform)
Collectors for Airflow runs, dbt artifacts, Spark batch logs and Kafka lag; full reliability
metadata model; contract and freshness checks; deterministic incident detection; lineage from
the dbt manifest plus declared ingestion/publication/dashboard edges; impact analysis.
