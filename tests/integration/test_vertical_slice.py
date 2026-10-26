"""Milestone 1 vertical slice against a real PostgreSQL server:
source -> batch ingestion -> dbt revenue mart -> reconciliation.

Tests run in file order and share one pipeline state (module-scoped fixture).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from dataops.config import Role
from dataops.db import connect
from dataops.generator.generate import generate
from dataops.generator.ledger import build_ledger
from dataops.generator.loader import SourceNotEmptyError, load_source
from dataops.ingestion import batch
from dataops.reconciliation import revenue

from .conftest import run_dbt

HISTORY = datetime(2020, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def pipeline(test_databases, tmp_path_factory):
    ds = generate(3, "tiny")
    load_source(ds)
    for table in batch.TABLE_ORDER:
        batch.ingest_table(table, HISTORY, ds.start)
    results = batch.ingest_range(batch.TABLE_ORDER, ds.start, ds.end, step=timedelta(days=1))
    assert all(r.status == "committed" for r in results)
    tmp = tmp_path_factory.mktemp("dbt")
    build = run_dbt(tmp, "build")
    assert build.returncode == 0, build.stdout[-3000:]
    return {"ds": ds, "ledger": build_ledger(ds), "tmp": tmp}


def test_end_to_end_reconciles(pipeline):
    rows = revenue.reconcile_row_counts("it-e2e")
    src = revenue.reconcile_source_vs_mart("it-e2e")
    led = revenue.reconcile_ledger_vs_mart(pipeline["ledger"], "it-e2e")
    assert (rows.status, src.status, led.status) == ("pass", "pass", "pass"), (
        rows.mismatches,
        src.mismatches,
        led.mismatches,
    )
    assert led.right_totals == pipeline["ledger"]["revenue_totals"]


def test_results_are_persisted(pipeline):
    with connect(Role.REL_READER) as conn:
        n = conn.execute(
            "select count(*) from reliability.reconciliation_results"
            " where correlation_id = 'it-e2e' and status = 'pass'"
        ).fetchone()[0]
    assert n == 3


def test_loader_refuses_to_overwrite_without_replace(pipeline):
    with pytest.raises(SourceNotEmptyError):
        load_source(pipeline["ds"])


def test_reingesting_a_window_is_idempotent(pipeline):
    ds = pipeline["ds"]
    results = batch.ingest_range(batch.TABLE_ORDER, ds.start, ds.start + timedelta(days=2))
    assert sum(r.inserted_rows for r in results) == 0
    assert sum(r.duplicate_rows for r in results) > 0


def test_failed_publication_is_atomic(pipeline, monkeypatch):
    ds = pipeline["ds"]
    before = batch.watermark("orders")
    calls = {"n": 0}
    real = batch.payload_checksum

    def flaky(payload):
        calls["n"] += 1
        if calls["n"] > 3:
            raise RuntimeError("simulated crash mid-batch")
        return real(payload)

    monkeypatch.setattr(batch, "payload_checksum", flaky)
    with connect(Role.SRC_APP) as conn, conn.transaction():  # create new versions to publish
        conn.execute("update shop.orders set updated_at = updated_at + interval '1 second'")
    with pytest.raises(RuntimeError, match="simulated crash"):
        batch.ingest_table("orders", ds.start, datetime.now(UTC), overlap=timedelta(0))
    with connect(Role.WH_READER) as conn:
        failed = conn.execute(
            "select batch_id, status, error_message from raw.ingestion_batches"
            " where source_table='orders' order by started_at desc limit 1"
        ).fetchone()
        leaked = conn.execute(
            "select count(*) from raw.source_records where batch_id=%s", (failed[0],)
        ).fetchone()[0]
    assert failed[1] == "failed" and "simulated crash" in failed[2]
    assert leaked == 0
    assert batch.watermark("orders") == before
    monkeypatch.undo()
    # Restart: a fresh batch for the same window publishes the new versions exactly once.
    ok = batch.ingest_table("orders", ds.start, datetime.now(UTC), overlap=timedelta(0))
    assert ok.status == "committed" and ok.inserted_rows == len(ds.tables["orders"])


def test_overlapping_writer_is_rejected(pipeline):
    ds = pipeline["ds"]
    batch_id = batch._register_batch("refunds", ds.start, ds.end, timedelta(0), None, "it-lock")
    try:
        with pytest.raises(batch.OverlappingWriterError):
            batch.ingest_table("refunds", ds.start, ds.end)
    finally:
        batch._mark_failed(batch_id, "test cleanup")


def test_source_change_detected_then_corrected(pipeline):
    """A refund amount changes after ingestion: reconciliation fails with the exact delta,
    then an incremental run picks up the new version and reconciliation passes again."""
    ds = pipeline["ds"]
    with connect(Role.SRC_APP) as conn, conn.transaction():
        refund_id, refunded_at = conn.execute(
            "update shop.refunds set amount_cents = amount_cents - 100"
            " where refund_id = (select min(refund_id) from shop.refunds)"
            " returning refund_id, refunded_at"
        ).fetchone()
    failing = revenue.reconcile_source_vs_mart("it-drift")
    assert failing.status == "fail"
    (mismatch,) = failing.mismatches
    assert mismatch["key"] == refunded_at.date().isoformat()
    assert mismatch["metrics"]["refunded_cents"]["delta"] == 100  # mart still has old amount

    now = datetime.now(UTC) + timedelta(seconds=1)
    res = batch.ingest_table("refunds", batch.watermark("refunds") or ds.end, now)
    assert res.inserted_rows == 1
    assert run_dbt(pipeline["tmp"], "build").returncode == 0
    assert revenue.reconcile_source_vs_mart("it-drift-fixed").status == "pass"
    assert refund_id


def test_least_privilege(pipeline):
    with connect(Role.WH_READER) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("delete from raw.source_records")
    with connect(Role.SRC_READER) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("update shop.orders set status = 'paid'")
    with connect(Role.REL_READER) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("delete from reliability.reconciliation_results")
    with connect(Role.WH_READER) as conn:  # readers can read dbt output
        assert conn.execute("select count(*) from marts.mart_daily_revenue").fetchone()[0] > 0
