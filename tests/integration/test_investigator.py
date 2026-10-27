"""Investigator against real PostgreSQL (isolated *_test databases), fixture provider only.
Fixture mode is a deterministic harness; these tests check plumbing, not AI quality."""

from __future__ import annotations

import os
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataops.config import REPO_ROOT, Role
from dataops.db import connect
from dataops.evaluation import environment, harness, seeds
from dataops.investigator import engine, evidence, runbooks


@pytest.fixture(scope="module")
def prepared(test_databases, tmp_path_factory):
    from dataops.reliability import lineage

    dbt = shutil.which("dbt")
    if dbt is None:
        pytest.skip("dbt not installed")
    tmp = tmp_path_factory.mktemp("parse")
    subprocess.run(
        [
            dbt,
            "parse",
            "--project-dir",
            str(REPO_ROOT / "dbt"),
            "--profiles-dir",
            str(REPO_ROOT / "dbt"),
            "--target-path",
            str(tmp),
            "--log-path",
            str(tmp),
        ],
        check=True,
        env=dict(os.environ),
        capture_output=True,
    )
    lineage.persist(lineage.build_graph(tmp / "manifest.json"))
    with connect(Role.REL_APP) as conn, conn.transaction():
        for pid, system, crit in environment.PIPELINES:
            conn.execute(
                "insert into reliability.pipeline_definitions (pipeline_id, system, criticality)"
                " values (%s,%s,%s) on conflict do nothing",
                (pid, system, crit),
            )
    return test_databases


def test_incremental_indexing_propagates_updates_and_deletes(prepared, tmp_path: Path):
    d = tmp_path / "runbooks"
    shutil.copytree(runbooks.RUNBOOK_DIR, d)
    first = runbooks.index(d)
    assert len(first["added"]) >= 7 and first["chunks_written"] > 0
    assert set(runbooks.index(d)["unchanged"]) == set(first["added"])
    p = d / "stale-data.md"
    p.write_text(
        p.read_text().replace("batch extraction runs daily", "batch extraction runs twice a day")
    )
    (d / "consumer-lag.md").unlink()
    second = runbooks.index(d)
    assert second["updated"] == ["RB-002"] and second["deleted"] == ["RB-003"]
    hits = runbooks.search("batch extraction runs twice a day load age", k=2)
    assert hits[0]["runbook_id"] == "RB-002"
    with connect(Role.REL_READER) as conn:
        assert (
            conn.execute(
                "select count(*) from reliability.runbook_chunks where doc_id='RB-003'"
            ).fetchone()[0]
            == 0
        )
    runbooks.index(runbooks.RUNBOOK_DIR)


def test_search_returns_resolvable_citations(prepared):
    runbooks.index(runbooks.RUNBOOK_DIR)
    hits = runbooks.search("source column type changed from integer cents to decimal", k=3)
    assert hits[0]["runbook_id"] == "RB-001"
    assert all(evidence.resolve([h["evidence_id"]])[h["evidence_id"]] for h in hits)


def test_fixture_investigation_end_to_end_is_read_only_and_audited(prepared):
    runbooks.index(runbooks.RUNBOOK_DIR)
    case = next(c for c in harness.load_suite()["cases"] if c["id"] == "PI-01")
    now = datetime.now(UTC)
    with connect(Role.REL_APP) as conn, conn.transaction():
        seeds.TEMPLATES[case["template"]](seeds.Seeder(conn, "it-pi01", now), case["params"])
    from dataops.reliability import incidents

    incidents.evaluate(now=now)
    with connect(Role.REL_READER) as conn:
        inc_id, before = conn.execute(
            "select incident_id, updated_at from reliability.incidents where root_dataset_id ="
            " 'commerce_source.shop.payments'"
        ).fetchone()
    res = engine.investigate(str(inc_id), mode="fixture", now=now)
    assert res["status"] == "completed" and res["mode"] == "fixture"
    out = res["output"]
    assert out["likely_causes"][0]["category"] == "schema_change"
    refs = out["evidence_references"]
    assert refs and all(evidence.resolve(refs).values())
    assert "ev:task_runs:maintenance-approved" not in refs
    with connect(Role.REL_READER) as conn:
        row = conn.execute(
            "select mode, status, prompt_version, tool_call_count, embedding_model from"
            " reliability.investigation_runs where investigation_id=%s",
            (res["investigation_id"],),
        ).fetchone()
        after = conn.execute(
            "select updated_at from reliability.incidents where incident_id=%s", (inc_id,)
        ).fetchone()[0]
    assert row[0] == "fixture" and row[1] == "completed" and row[2] == engine.PROMPT_VERSION
    assert row[3] == len(res["tool_calls"]) > 3 and row[4] == "hashing-v1-384"
    assert after == before  # the investigator never changes incident state


def test_investigator_role_cannot_write(prepared):
    import psycopg

    with connect(Role.REL_READER) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("delete from reliability.incidents")
