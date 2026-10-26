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

## Milestone 2: data engineering depth — **in progress**

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

### 2b. Full-profile streaming and CDC — **not started; runtime blocked here**
Kafka (KRaft), Debezium PostgreSQL connector, Spark 4.2 + Delta 4.4.1 Bronze/Silver jobs,
Silver → warehouse publisher, Airflow DAGs. These can be written and statically validated in
this environment but not run (container registries and Maven Central are blocked).

### Next actions
1. Write full-profile Compose services with pinned versions and compatibility notes.
2. Implement Spark Bronze/Silver jobs with unit tests that run on local PySpark where possible
   (Delta itself needs Maven artifacts: verification deferred to a machine with access).
3. Airflow DAGs for the batch path already verified here (extraction, backfill, dbt, reconcile).

## Blockers
- Container registries and Maven Central are not reachable from the build environment.
  To unblock: allow those hosts for this environment (claude.ai → Settings → Capabilities on
  a paid individual plan, or the organization admin), or run the full profile on a machine
  with registry access.
