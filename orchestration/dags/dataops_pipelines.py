"""DataOps Investigator DAGs (Airflow 3.x).

Every task shells out to the project CLI (`python -m dataops ...`) in the project's own
virtualenv (DATAOPS_PYTHON), so Airflow's dependency set never has to coexist with dbt or
Spark. Long-running streaming consumers are NOT scheduled here: the Silver publication DAG
runs the streaming jobs with an available-now trigger (process what exists, then stop),
which is bounded work; continuous streaming runs as its own service.

Writers never overlap: each writing DAG has max_active_runs=1 and its tasks share the
`warehouse_writer` pool (1 slot), in addition to the database-level single-writer locks.

DAGs
  dataops_batch_extract      @daily, one bounded window per data interval, all tables
  dataops_backfill           manual, explicit [start, end) and tables, chunked by day
  dataops_silver_publication every 30 min (full profile): Bronze/Silver/events -> publish
  dataops_dbt_build          triggered by the warehouse-input assets above
  dataops_reconciliation     triggered after a successful dbt build
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset, Param

PROJECT = os.environ.get("DATAOPS_PROJECT_DIR", "/opt/dataops")
PYTHON = os.environ.get("DATAOPS_PYTHON", f"{PROJECT}/.venv/bin/python")
DBT = os.environ.get("DATAOPS_DBT", f"{PROJECT}/.venv/bin/dbt")
SOURCE_PATH = os.environ.get("DATAOPS_SOURCE_PATH", "batch")  # batch | cdc
CLI = f"cd {PROJECT} && {PYTHON} -m dataops --log-level INFO"

# Asset URIs follow the postgres://host:port/database/schema/table scheme.
RAW_LANDED = Asset("postgres://postgres:5432/warehouse/raw/source_records")
SILVER_PUBLISHED = Asset("postgres://postgres:5432/warehouse/cdc_pub/records")
MARTS_BUILT = Asset("postgres://postgres:5432/warehouse/marts/mart_daily_revenue")

DEFAULTS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
}
TABLES = [
    "warehouses",
    "customers",
    "products",
    "product_versions",
    "orders",
    "order_items",
    "payments",
    "refunds",
    "inventory_movements",
    "shipments",
    "shipment_events",
]

with DAG(
    dag_id="dataops_batch_extract",
    description="Incremental extraction of one data interval into warehouse raw.",
    schedule="@daily",
    start_date=datetime(2026, 3, 2),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["dataops", "ingestion"],
) as batch_extract:
    for table in TABLES:
        BashOperator(
            task_id=f"ingest_{table}",
            pool="warehouse_writer",
            bash_command=(
                f"{CLI} ingest --tables {table} --start '{{{{ data_interval_start }}}}' "
                f"--end '{{{{ data_interval_end }}}}' --step 1d --run-id '{{{{ run_id }}}}'"
            ),
            outlets=[RAW_LANDED],
        )

with DAG(
    dag_id="dataops_backfill",
    description="Bounded historical backfill of explicit windows (manual trigger only).",
    schedule=None,
    start_date=datetime(2026, 3, 2),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULTS, "execution_timeout": timedelta(hours=2)},
    params={
        "start": Param("2026-03-02T00:00:00+00:00", type="string"),
        "end": Param("2026-03-03T00:00:00+00:00", type="string"),
        "tables": Param("all", type="string", description="comma-separated table names, or 'all'"),
    },
    tags=["dataops", "ingestion", "backfill"],
) as backfill:
    BashOperator(
        task_id="backfill_windows",
        pool="warehouse_writer",
        bash_command=(
            f"{CLI} ingest --tables '{{{{ params.tables }}}}' --start '{{{{ params.start }}}}' "
            f"--end '{{{{ params.end }}}}' --step 1d --overlap 0m --run-id '{{{{ run_id }}}}'"
        ),
        outlets=[RAW_LANDED],
    )

with DAG(
    dag_id="dataops_silver_publication",
    description="Process available CDC data into Bronze/Silver, then publish Silver.",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 3, 2),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULTS, "execution_timeout": timedelta(minutes=30)},
    tags=["dataops", "lakehouse", "full-profile"],
) as silver_publication:
    process = BashOperator(
        task_id="process_available_cdc",
        bash_command=f"{CLI} stream run --jobs bronze silver events",
    )
    publish = BashOperator(
        task_id="publish_silver",
        pool="warehouse_writer",
        bash_command=f"{CLI} publish --correlation-id '{{{{ run_id }}}}'",
        outlets=[SILVER_PUBLISHED],
    )
    process >> publish

with DAG(
    dag_id="dataops_dbt_build",
    description="dbt build of staging, dimensions, facts and marts.",
    schedule=[SILVER_PUBLISHED] if SOURCE_PATH == "cdc" else [RAW_LANDED],
    start_date=datetime(2026, 3, 2),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["dataops", "dbt"],
) as dbt_build:
    BashOperator(
        task_id="dbt_build",
        pool="warehouse_writer",
        bash_command=(
            f"cd {PROJECT}/dbt && {DBT} build --profiles-dir . "
            f"--vars '{{source_path: {SOURCE_PATH}}}' "
            f"--target-path target/{{{{ run_id | replace(':', '_') }}}}"
        ),
        outlets=[MARTS_BUILT],
    )

with DAG(
    dag_id="dataops_reconciliation",
    description="Source vs raw counts and source vs mart revenue reconciliation.",
    schedule=[MARTS_BUILT],
    start_date=datetime(2026, 3, 2),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULTS, "retries": 1},
    tags=["dataops", "quality"],
) as reconciliation:
    BashOperator(
        task_id="reconcile_revenue",
        bash_command=(
            f"{CLI} reconcile --source-path {SOURCE_PATH} --correlation-id '{{{{ run_id }}}}'"
        ),
    )
