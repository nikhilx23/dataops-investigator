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

## Milestone 3: reliability platform — **complete** (2026-10-27)

Implemented: reliability migrations 0002 (metadata model) and 0003 (current-state views),
collectors for dbt, ingestion/publication manifests, Airflow REST API v2, Spark progress,
Kafka lag + Connect status, freshness and schema; versioned lineage with impact analysis;
deterministic incident engine with correlation, deduplication, evidence and history;
reversible failure scenario 1 (`dataops.scenarios.schema_change`); cross-process lakehouse
lock; CLI `collect`, `lineage`, `incidents`; DAGs `dataops_metadata_collection` and
`dataops_incident_evaluation`. Details: [reliability_platform.md](reliability_platform.md).

| Command / check | Result |
|---|---|
| `python scripts/demo_m3.py` (lightweight DBs) | healthy: 0 events; after injection: dbt build exit 1, reconciliation fail (30 dates), 1 sev1 incident rooted at `shop.payments` with 3 dashboards; re-evaluation: 0 new events; revert ok |
| Full profile `dataops collect` | 7/7 collectors success (dbt 492, ingestion 9, schema 11, freshness 24, Airflow 43, Spark 13, Kafka 12 records) |
| Consumer-lag scenario (full profile) | 1 incident, 16 correlated signals; recovery clears all signals |
| Airflow: metadata collection -> (asset) incident evaluation | 2 + 2 runs, all `success`; Airflow collector authenticated with the generated admin password |
| `pytest -q` | 68 passed (40 unit, 22 integration, 6 Spark) |
| ruff, mypy, shellcheck; DAG import check (7 DAGs) | clean |

Bugs and design gaps found by verification and fixed:
- Old failed/skipped dbt tasks kept firing after a later successful run -> signals are now
  superseded by later success of the same task.
- Freshness judged the inactive input path, and its stale readings stayed "latest" forever
  -> only the active path (`DATAOPS_SOURCE_PATH`) is judged, and only observations repeated by
  the latest collection count. Three false-positive incidents were closed with that reason.
- One stalled input path opened 7 incidents (unrelated tables) -> signals now carry a group
  (consumer, input path) used for correlation. The 7 were closed as superseded and
  re-evaluated into one incident.
- A CLI stream run and an Airflow stream task overlapped; Spark failed the second safely
  (`CONCURRENT_STREAM_LOG_UPDATE`) -> added a cross-process lakehouse lock used by the
  streaming runner and the publisher (which also makes publications a consistent cut).
- An Airflow task's first attempt failed because the standalone API server was still
  starting; the bounded retry recovered it (no code change).
- dbt skips models downstream of a failed test, so no "failed task" appeared for stale marts
  -> new signal `downstream_model_skipped`.

Known limitations:
- Root cause is inferred from lineage and signal order; it is never "confirmed" here.
- Thresholds (lookback 24 h, correlation window 180 min, lag 15 min) are environment settings,
  not learned.
- Prometheus/Grafana are not set up; the views above are the metrics source for M5.
- The Airflow collector needs API credentials; in standalone mode they come from the
  generated password file at start-up.

## Milestone 4: AI investigator — **implemented; verified in fixture mode; real-model evaluation blocked** (2026-10-27)

Implemented: migration `reliability/0004` (runbook documents/chunks with pgvector HNSW and
tsvector indexes; investigation audit fields); seven original runbooks; incremental,
fingerprinted, section-aware indexing with update/delete propagation; hybrid retrieval;
11 allowlisted read-only tools; evidence resolver; structured output schema; validator
(citations, numeric grounding, forbidden operations, numeric confidence, forced abstention);
bounded loop with audit record; Anthropic provider; labelled fixture provider; 35-case
evaluation suite with harness and report; CLI `runbooks`, `investigate`, `eval`.
Details: [investigator.md](investigator.md), [../evaluations/README.md](../evaluations/README.md).

| Command / check | Result |
|---|---|
| `dataops db migrate` | `reliability/0004_investigator.sql` applied |
| `dataops runbooks index` (twice) | 7 documents, 28 chunks; second run: 7 unchanged |
| `dataops investigate <M3 schema incident> --mode fixture` | completed; 7 tool calls; 26/26 citations valid; cause `schema_change` at `shop.payments`; backfill/rebuild preconditions marked **unmet** (schema still breaking) |
| `dataops eval --mode fixture` (35 cases, `*_eval` databases) | every case opened exactly one incident; see `evaluations/results/fixture/` — **fixture harness, not AI; not a model-quality metric** |
| Retrieval benchmark (hashing, k=4) | hit@4 35/35, MRR@4 0.91 (queries written by the runbook author: optimistic) |
| `pytest tests/unit` / investigator integration tests | 62 passed / 4 passed; full non-Spark suite 88 passed earlier in the milestone |
| ruff, ruff format, mypy | clean |

Not done / blocked:
- **Real-model evaluation: blocked** until `ANTHROPIC_API_KEY` is in the local `.env`
  (api.anthropic.com is reachable: 401 without a key). The Anthropic code path is
  implemented but has not made a single real call yet.
- **Neural embeddings: blocked** (Hugging Face unreachable from this network); the default
  `hashing-v1-384` embedder is lexical, labelled as such in every chunk and result.
- No human-rubric scoring yet (needs real-model outputs).

Found by verification and fixed:
- `get_incident` results exceeded the context budget and the trimming dropped the decisive
  schema signal -> signals are ordered root-first/upstream-first and a per-rule count is
  always included; budget raised to 12k characters.
- A failed Debezium connector opened a separate incident from the stale published tables it
  caused -> connector signals now share the CDC input-path group.
- Two seeds listed models as skipped that are not downstream in lineage, so the engine
  (correctly) split them into two incidents -> seeds checked against the lineage graph.

## Milestone 5: recovery and demonstration — **complete** (2026-10-28)

Implemented: reliability migrations 0005 (recovery audit) and 0006 (evidence key per
incident); recovery service with five predefined actions, typed scopes, live preconditions,
dry runs, named-human approval, single-runner lock, verification that re-evaluates the
incident; failure scenarios 2-8 with a runner; FastAPI (reads, investigations, recovery,
`/metrics`); Streamlit dashboard (API only); Prometheus + Grafana (monitoring profile, alert
rules, provisioned dashboard); app image (now builds) with `api` and `dashboard` Compose
services. Guides: [recovery.md](recovery.md), [failure_scenarios.md](failure_scenarios.md).

| Command / check | Result |
|---|---|
| Recovery of the open M3 schema-change incident (demo DB) | premature rebuild **rejected** with reason; approval by a service identity refused; `bounded_backfill` payments (79 rows) → verification_failed (1 signal left, correct); `rebuild_models` stg_payments+ (PASS=29) → **verified**; incident `resolved`, cause `confirmed` |
| `dataops scenario run <2,3,5,6,7,8>` | every scenario: one sev1 incident 8.7–11.6 s after injection; recovery verified; cleanup reconciles — [results](results/scenarios/README.md) |
| Scenario 2 through the dashboard (Playwright driving the UI) | escalate → owner fix → rebuild proposed, approved, executed and verified from the UI; incident resolved/confirmed (screenshots 05–07) |
| API (TestClient + live) | `/health`, `/overview`, `/incidents`, `/impact`, `/metrics` (95 series lines), writes refused without token (401/403), unknown action 409, model mode without key 409 |
| Compose: `postgres`, `app db migrate`, `api`, `dashboard` containers | migrations 0004–0006 applied; API healthy; dashboard pages render with 0 exceptions |
| Compose monitoring profile | Prometheus target `up`; Grafana dashboard renders live metrics (screenshot 11) |
| `pytest -m "not spark"` | 115 passed (85 unit, 30 integration) |
| ruff, ruff format, mypy | clean |

Found by verification and fixed:
- Evidence ids were a global primary key, so a second incident citing the same dashboard path
  crashed the engine -> primary key is now (incident, evidence id).
- Row-count reconciliation signals were attributed to `warehouse.raw.source_records`, which
  is outside lineage, so they opened a separate incident -> attributed to
  `warehouse.raw.<table>`; scenarios 6 and 7 now produce one incident rooted at `raw.orders`.
- Revoking SELECT made `information_schema` hide all columns -> the schema collector reported
  a false breaking change; unreadable tables are now skipped.
- Back-dated source corrections older than the loaded version can never win latest-version
  ordering; scenarios 3 and 8 first produced unfixable states (verification correctly failed;
  incidents closed with that reason) -> scenarios now write 1 s after the previous version.
- Scenario cleanups that move data backwards or delete rows are invisible to incremental facts
  -> cleanups end with a full refresh.
- Scenario 6's first recovery plan omitted `stg_payments`; dbt's consistency test failed and
  the action was recorded `failed` -> plan fixed.
- The runner crashed when a proposal was refused (scope over 31 days) -> refusals are recorded.
- The app image build failed on `apt-get` (deb.debian.org unreachable) -> no OS packages
  needed; built through the proxy with an optional CA build secret.

Not done / limits:
- Real-model investigation and evaluation: still blocked (no API key).
- The M3 consumer-lag incident in the Compose (full-profile) database stays open: recovering
  it needs the Kafka/Spark stack running; not repeated in this milestone.
- Scenario detected→verified times are machine time: the runner approves immediately.
- Monitoring uses host networking (Linux); Docker Desktop needs the documented change.
- Freshness `data_lag` can be negative after scenario cleanups (warehouse holds a version the
  source no longer has); shown as observed, not clamped.

## GitHub and CI — **verified** (2026-10-28)
Pushed to https://github.com/nikhilx23/dataops-investigator. First CI run: integration,
spark-and-dags and compose-and-scripts passed; lint-and-unit failed (mypy: type-ignore comments
unused when the optional Anthropic SDK is absent) and secret-scan failed (deliberately fake test
credentials). Fixed (cast instead of ignores; `.gitleaks.toml` allowlisting those exact fake
values). Second run (40621685613): **all 5 jobs passed**.

## Next: Milestone 6 (benchmarks and portfolio)
Benchmarks, remaining docs (architecture diagram,
data dictionary, security, cost, demo script, troubleshooting, interview guide, resume
bullets from verified results), optional Terraform; real-model evaluation when the key arrives.

## Blockers
- Real-model evaluation needs `ANTHROPIC_API_KEY` in the local `.env`.
- deb.debian.org and huggingface.co are not reachable from the build network.
