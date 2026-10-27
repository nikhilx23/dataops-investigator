"""Isolated evaluation databases (*_eval). Nothing here touches the demo databases."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from dataops.config import REPO_ROOT, Role, connection_spec, load_dotenv
from dataops.db import connect

EVAL_DBS = {
    "DATAOPS_SOURCE_DB": "commerce_source_eval",
    "DATAOPS_WAREHOUSE_DB": "warehouse_eval",
    "DATAOPS_RELIABILITY_DB": "reliability_eval",
    "DATAOPS_AIRFLOW_DB": "airflow_eval",
}
PIPELINES = [
    ("batch_ingestion", "loader", "critical"),
    ("dbt", "dbt", "critical"),
    ("silver_publication", "loader", "critical"),
    ("debezium", "debezium", "critical"),
    ("spark:silver", "spark", "critical"),
    ("spark:bronze", "spark", "standard"),
]


def use_eval_databases() -> None:
    load_dotenv()
    os.environ.update(EVAL_DBS)


def prepare(rebuild: bool = False) -> dict[str, str]:
    """Create (or reuse) the eval databases, migrate, store lineage and register pipelines."""
    use_eval_databases()
    import psycopg

    admin = connection_spec(Role.ADMIN, "dataops-eval")
    with psycopg.connect(
        host=admin.host,
        port=admin.port,
        user=admin.user,
        password=admin.password,
        dbname="postgres",
        autocommit=True,
    ) as conn:
        exists = conn.execute(
            "select 1 from pg_database where datname=%s", (EVAL_DBS["DATAOPS_RELIABILITY_DB"],)
        ).fetchone()
        if rebuild or not exists:
            for name in EVAL_DBS.values():
                assert name.endswith("_eval"), name
                conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            exists = None
    if not exists:
        subprocess.run(
            [
                "psql",
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                admin.host,
                "-p",
                str(admin.port),
                "-U",
                admin.user,
                "-d",
                "postgres",
                "-f",
                str(REPO_ROOT / "infra/postgres/bootstrap.sql"),
            ],
            check=True,
            env=dict(os.environ, PGPASSWORD=admin.password),
            capture_output=True,
        )
    from dataops.migrate import migrate

    migrate()
    from dataops.reliability import lineage

    dbt = shutil.which("dbt")
    if dbt is None:
        raise RuntimeError("dbt is needed to parse the project for the lineage graph")
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            [
                dbt,
                "parse",
                "--project-dir",
                str(REPO_ROOT / "dbt"),
                "--profiles-dir",
                str(REPO_ROOT / "dbt"),
                "--target-path",
                tmp,
                "--log-path",
                tmp,
                "--quiet",
            ],
            check=True,
            env=dict(os.environ),
            capture_output=True,
        )
        version = lineage.persist(lineage.build_graph(Path(tmp) / "manifest.json"))
    with connect(Role.REL_APP, application_name="dataops-eval") as conn, conn.transaction():
        for pid, system, crit in PIPELINES:
            conn.execute(
                "insert into reliability.pipeline_definitions (pipeline_id, system,"
                " criticality) values (%s,%s,%s) on conflict (pipeline_id) do update set"
                " criticality=excluded.criticality",
                (pid, system, crit),
            )
    return {"lineage_version": version, **EVAL_DBS}


CASE_TABLES = [
    "incidents",
    "incident_events",
    "pipeline_runs",
    "quality_check_results",
    "freshness_observations",
    "schema_observations",
    "reconciliation_results",
    "stream_observations",
]


def clear_case_state() -> None:
    assert os.environ.get("DATAOPS_RELIABILITY_DB", "").endswith("_eval")
    with connect(Role.REL_APP, application_name="dataops-eval") as conn, conn.transaction():
        conn.execute("truncate " + ", ".join(f"reliability.{t}" for t in CASE_TABLES) + " cascade")
