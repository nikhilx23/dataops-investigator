"""Milestone 3 end to end on real PostgreSQL: a breaking source schema change is detected,
correlated into ONE incident whose inferred root is the source table, with impact on the
revenue dashboard and evidence records that resolve to stored observations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dataops.config import Role
from dataops.db import connect
from dataops.generator.generate import generate
from dataops.generator.loader import load_source
from dataops.ingestion import batch
from dataops.reconciliation import revenue
from dataops.reliability import collectors, incidents
from dataops.scenarios import schema_change

from .conftest import run_dbt

HISTORY = datetime(2020, 1, 1, tzinfo=UTC)


def _ingest_until(when: datetime) -> None:
    for table in batch.TABLE_ORDER:
        start = batch.watermark(table) or HISTORY
        if start < when:
            batch.ingest_table(table, start, when)


@pytest.fixture(scope="module")
def healthy(test_databases, tmp_path_factory, monkeypatch_module):
    tmp = tmp_path_factory.mktemp("dbt")
    monkeypatch_module.setenv("DATAOPS_DBT_TARGET_ROOT", str(tmp))
    ds = generate(11, "tiny")
    load_source(ds)
    _ingest_until(ds.end)
    assert run_dbt(tmp, "build").returncode == 0
    revenue.reconcile_source_vs_mart("it-m3-healthy")
    out = collectors.collect(["dbt", "ingestion", "schema", "freshness"])
    assert all(v["status"] == "success" for v in out.values()), out
    return {"ds": ds, "tmp": tmp}


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def test_healthy_pipeline_raises_no_incidents(healthy):
    result = incidents.evaluate()
    assert result["new_events"] == 0 and result["created"] == []


def test_collectors_are_idempotent(healthy):
    with connect(Role.REL_READER) as conn:

        def counts():
            return conn.execute(
                "select (select count(*) from reliability.pipeline_runs),"
                " (select count(*) from reliability.task_runs),"
                " (select count(*) from reliability.quality_check_results),"
                " (select count(*) from reliability.schema_observations)"
            ).fetchone()

        before = counts()
    collectors.collect(["dbt", "ingestion", "schema"])
    with connect(Role.REL_READER) as conn:
        assert counts() == before


@pytest.fixture(scope="module")
def broken(healthy):
    injected = schema_change.inject()
    assert injected["status"] == "injected" and injected["touched_payments"] > 0
    _ingest_until(datetime.now(UTC) + timedelta(seconds=1))
    build = run_dbt(healthy["tmp"], "build")
    assert build.returncode != 0  # the change must break dbt visibly
    revenue.reconcile_source_vs_mart("it-m3-broken")
    collectors.collect(["dbt", "ingestion", "schema", "freshness"])
    first = incidents.evaluate()
    second = incidents.evaluate()
    return {"first": first, "second": second, "build": build}


def test_one_incident_rooted_at_the_source_table(broken):
    assert len(broken["first"]["created"]) == 1
    inc = incidents.show(broken["first"]["created"][0])
    assert inc["root_dataset_id"] == "commerce_source.shop.payments"
    assert inc["root_cause_status"] == "inferred"
    rules = {e["rule_id"] for e in inc["events"]}
    assert "breaking_schema_change" in rules
    assert rules & {"failed_critical_task", "quality_check_failed", "reconciliation_failed"}
    assert inc["severity"] == "sev1"  # financial reconciliation / critical dashboard


def test_impact_reaches_revenue_dashboard_with_paths(broken):
    inc = incidents.show(broken["first"]["created"][0])
    assert "bi.dashboard.revenue_overview" in inc["impacted_datasets"]
    paths = [e for e in inc["evidence"] if e["evidence_type"] == "lineage_path"]
    rev = [p for p in paths if p["dataset_id"] == "bi.dashboard.revenue_overview"]
    assert rev and rev[0]["summary"].startswith("commerce_source.shop.payments -> ")


def test_every_evidence_reference_resolves(broken):
    inc = incidents.show(broken["first"]["created"][0])
    tables = {
        "task_runs": "task_run_id",
        "quality_check_results": "result_id",
        "schema_observations": "observation_id::text",
        "reconciliation_results": "reconciliation_id::text",
        "freshness_observations": "observation_id::text",
    }
    with connect(Role.REL_READER) as conn:
        rows = conn.execute(
            "select source_table, source_id from reliability.incident_evidence"
            " where incident_id = %s",
            (inc["incident_id"],),
        ).fetchall()
        assert rows
        for table, sid in rows:
            if table == "lineage_edges":
                version, pair = sid.split(":", 1)
                root, dash = pair.split("->")
                assert conn.execute(
                    "select 1 from reliability.lineage_edges where"
                    " lineage_version = %s and downstream_id = %s",
                    (version, dash),
                ).fetchone()
                continue
            key = tables[table]
            found = conn.execute(
                f"select 1 from reliability.{table} where {key} = %s", (sid,)
            ).fetchone()
            assert found, (table, sid)


def test_reevaluation_does_not_duplicate(broken):
    assert broken["second"]["created"] == [] and broken["second"]["new_events"] == 0
    with connect(Role.REL_READER) as conn:
        active = conn.execute(
            "select count(*) from reliability.incidents where status = 'open'"
        ).fetchone()[0]
    assert active == 1


def test_schema_observation_describes_the_change(broken):
    with connect(Role.REL_READER) as conn:
        comp, diffs = conn.execute(
            "select compatibility, differences from reliability.schema_observations"
            " where dataset_id = 'commerce_source.shop.payments'"
            " order by observed_at desc limit 1"
        ).fetchone()
    assert comp == "breaking"
    assert {
        "column": "amount_cents",
        "change": "type_changed",
        "expected": "int",
        "observed": "numeric",
        "breaking": True,
    } in diffs


def test_revert_restores_contract(broken):
    assert schema_change.revert()["status"] == "reverted"
    collectors.collect(["schema"])
    with connect(Role.REL_READER) as conn:
        comp = conn.execute(
            "select compatibility from reliability.schema_observations"
            " where dataset_id = 'commerce_source.shop.payments'"
            " order by observed_at desc limit 1"
        ).fetchone()[0]
    assert comp == "matches_contract"


def test_recovered_pipeline_raises_no_new_signals(broken, healthy):
    """After the revert, a successful rebuild supersedes the failed run: no new events."""
    _ingest_until(datetime.now(UTC) + timedelta(seconds=1))
    assert run_dbt(healthy["tmp"], "build").returncode == 0
    assert revenue.reconcile_source_vs_mart("it-m3-recovered").status == "pass"
    collectors.collect(["dbt", "ingestion", "schema", "freshness"])
    after = incidents.evaluate()
    assert after["new_events"] == 0 and after["created"] == []
