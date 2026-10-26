"""Integration fixtures: isolated *_test databases on the configured PostgreSQL server.

The suite never touches the demo databases. It recreates commerce_source_test,
warehouse_test, reliability_test and airflow_test (names are checked to end in _test before
anything is dropped), bootstraps roles/grants with infra/postgres/bootstrap.sql and applies
migrations. Requires DATAOPS_PG_ADMIN_PASSWORD and the role passwords (from .env), plus psql.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import psycopg
import pytest

from dataops.config import REPO_ROOT, Role, connection_spec, load_dotenv

TEST_DBS = {
    "DATAOPS_SOURCE_DB": "commerce_source_test",
    "DATAOPS_WAREHOUSE_DB": "warehouse_test",
    "DATAOPS_RELIABILITY_DB": "reliability_test",
    "DATAOPS_AIRFLOW_DB": "airflow_test",
}


def pytest_collection_modifyitems(items):
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="module")
def test_databases():
    """Fresh, isolated databases for each test module."""
    load_dotenv()
    if not os.environ.get("DATAOPS_PG_ADMIN_PASSWORD"):
        pytest.skip("DATAOPS_PG_ADMIN_PASSWORD not set; integration tests need PostgreSQL")
    if shutil.which("psql") is None:
        pytest.skip("psql not on PATH")
    os.environ.update(TEST_DBS)
    admin = connection_spec(Role.ADMIN, "dataops-tests")
    try:
        conn = psycopg.connect(**admin.kwargs(), autocommit=True)  # type: ignore[arg-type]
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    with conn:
        for name in TEST_DBS.values():
            assert name.endswith("_test"), name
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    env = dict(os.environ, PGPASSWORD=admin.password)
    subprocess.run(
        [
            "psql",
            "-X",
            "-q",
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
        env=env,
        capture_output=True,
    )
    from dataops.migrate import migrate

    migrate()
    return dict(TEST_DBS)


def run_dbt(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    dbt = shutil.which("dbt")
    if dbt is None:
        pytest.skip("dbt not installed")
    return subprocess.run(
        [
            dbt,
            *args,
            "--project-dir",
            str(REPO_ROOT / "dbt"),
            "--profiles-dir",
            str(REPO_ROOT / "dbt"),
            "--target-path",
            str(tmp_path / "dbt-target"),
            "--log-path",
            str(tmp_path / "dbt-logs"),
        ],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        check=False,
    )
