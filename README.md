# DataOps Investigator

**AI Data Reliability Platform with Pipeline Impact Analysis and Evidence-Based Incident Investigation**

A portfolio project that builds an operational data platform for a *fictional* commerce
company, then a reliability and AI investigation layer around it. **All data is synthetic**;
this is a simulated environment, not a real company's production platform.

> **Status: Milestones 1 and 2 complete (batch path, warehouse models, CDC, Spark + Delta
> lakehouse, Airflow), all verified; reliability platform, AI investigator, recovery and
> dashboard are not built yet.** See [docs/PROGRESS.md](docs/PROGRESS.md)
> for exactly what is implemented, verified, planned, and blocked. Nothing below claims more.

## The problem

When a pipeline breaks, engineers spend most of the time answering three questions: *what
actually failed, what is affected downstream, and is it fixed?* This project builds the
evidence trail (run metadata, schema observations, quality and reconciliation results,
lineage) needed to answer them, and later an AI investigator that may only cite that evidence.

## What works today (Milestone 1)

```
deterministic generator ──► PostgreSQL source (shop.*)
        │                          │  batch extraction: windowed, keyset-paginated,
        │                          ▼  overlap re-reads, content-deduplicated, atomic publish
        │                  warehouse raw.source_records + raw.ingestion_batches (manifest)
        │                          │  dbt (committed batches only)
        │                          ▼
        │                  staging views ──► marts.mart_daily_revenue (+ 35 data tests)
        ▼                          │
 expected-results ledger ──► reconciliation (source↔raw counts, source↔mart, ledger↔mart)
                                   ▼
                     reliability.reconciliation_results
```

- Seeded generator: customers, products with price history, orders, payments (pending,
  failed, succeeded, refunded), partial refunds, late captures, inventory movements,
  shipments with late and out-of-order events. Same seed → byte-identical data.
- Business rules defined first: [docs/business_rules.md](docs/business_rules.md).
- Separate databases and least-privilege roles for source, warehouse, reliability, Airflow.
- Boundary guarantees, including what is *not* guaranteed: [docs/architecture/guarantees.md](docs/architecture/guarantees.md).

## Quick start (lightweight, against any PostgreSQL 16 with pgvector)

Requirements: Python 3.12+, PostgreSQL 16 server you administer, `psql` ≥ 15.

```bash
python -m venv .venv && . .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1
pip install -e ".[dbt,dev]"
python scripts/init_env.py                             # writes .env with generated dev passwords
# set DATAOPS_PG_HOST/PORT and DATAOPS_PG_ADMIN_PASSWORD in .env to match your server
bash scripts/bootstrap_db.sh                           # PowerShell: scripts/bootstrap_db.ps1
bash scripts/demo_m1.sh                                # PowerShell: scripts/demo_m1.ps1
bash scripts/health.sh                                 # PowerShell: scripts/health.ps1
```

Expected end of the demo (default seed 42, `small` profile):

```
ingestion.row_counts.source_vs_raw       pass  rows=10 mismatches=0
revenue.daily.source_vs_mart             pass  rows=30 mismatches=0
revenue.daily.ledger_vs_mart             pass  rows=30 mismatches=0
```

### With Docker Compose (the app image build is not yet verified — see PROGRESS)

```bash
python scripts/init_env.py
bash scripts/up.sh lightweight                         # PowerShell: scripts/up.ps1
DATAOPS_RUNTIME=docker bash scripts/demo_m1.sh         # PowerShell: scripts/demo_m1.ps1 -Docker
bash scripts/down.sh                                   # keeps volumes; --delete-volumes removes them
```

## Milestone 2 demo: a simulated month of daily operations

```bash
python scripts/demo_m2.py --reset      # DESTRUCTIVE for the synthetic demo data only
```

The source advances one simulated day at a time; each day is extracted, dbt runs
incrementally every 5 days, then the incremental facts are compared with a full refresh and
reconciled against the source and the generator ledger.

## Full profile: CDC, streaming, lakehouse, Airflow

Kafka + Debezium capture every source change; Spark Structured Streaming lands them in Delta
Bronze, validates them against a versioned contract into Silver (bad records quarantined with
reasons), and a controlled publisher loads Silver into the warehouse in one transaction. dbt
runs unchanged on either path (`--vars '{source_path: cdc}'`). Airflow 3 schedules the
bounded work. Details, commands and measured memory: [docs/full_profile.md](docs/full_profile.md).

```bash
bash scripts/full_up.sh        # PowerShell: scripts/full_up.ps1
bash scripts/demo_full.sh      # PowerShell: scripts/demo_full.ps1
bash scripts/airflow.sh standalone
```

## Tests

```bash
pytest tests/unit                 # no services needed
pytest tests/integration          # needs PostgreSQL + .env; uses isolated *_test databases
ruff check . && ruff format --check . && mypy
```

## Limitations (current)

- Batch extraction cannot see hard deletes or intermediate versions between runs; the full
  profile's CDC path does.
- The app container image has not been built here (Docker Hub rate limit); the full-profile
  services ran in Compose and the CLI ran from the host virtualenv. The CI workflow has not
  run yet (nothing has been pushed).
- Reloading the source with a *different* seed requires resetting the warehouse raw zone.
- Not built yet: the reliability platform, the AI investigator, recovery and the dashboard.
