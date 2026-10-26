"""Milestone 2 warehouse modeling on the batch path:
SCD2 from the ordered change log, point-in-time joins, incremental facts that absorb late
updates, and full-refresh vs incremental equivalence.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dataops.config import Role
from dataops.db import connect
from dataops.generator.generate import generate
from dataops.generator.ledger import build_ledger
from dataops.generator.simulator import simulate
from dataops.ingestion import batch
from dataops.reconciliation import revenue
from dataops.reconciliation.fingerprint import FINGERPRINTABLE, fingerprint_all

from .conftest import run_dbt

HISTORY = datetime(2020, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def incremental_state(test_databases, tmp_path_factory):
    """Load day by day and run dbt incrementally after every day; include a late update to
    an already-processed order between runs."""
    tmp = tmp_path_factory.mktemp("dbt")
    ds = generate(5, "tiny")
    simulate(ds, ds.start)  # history before the simulation window
    for table in batch.TABLE_ORDER:
        batch.ingest_table(table, HISTORY, ds.start)
    windows = batch.split_windows(ds.start, ds.end, timedelta(days=1))
    for i, (w_start, w_end) in enumerate(windows):
        simulate(ds, w_end)  # the source evolves day by day, like an operational database
        for table in batch.TABLE_ORDER:
            assert batch.ingest_table(table, w_start, w_end).status == "committed"
        build = run_dbt(tmp, "build")
        assert build.returncode == 0, build.stdout[-3000:]
        if i == 1:
            # Late status change on an order whose lines were processed in an earlier run.
            with connect(Role.SRC_APP) as conn, conn.transaction():
                changed = conn.execute(
                    "update shop.orders set status = 'delivered' where order_id ="
                    " (select min(order_id) from shop.orders where status = 'shipped')"
                    " returning order_id"
                ).fetchone()
    now = datetime.now(UTC) + timedelta(seconds=1)
    for table in batch.TABLE_ORDER:  # pick up the late change (updated_at = wall clock)
        batch.ingest_table(table, ds.end, now)
    build = run_dbt(tmp, "build")
    assert build.returncode == 0, build.stdout[-3000:]
    return {"ds": ds, "tmp": tmp, "late_order_id": changed[0] if changed else None}


def test_incremental_absorbed_late_update(incremental_state):
    order_id = incremental_state["late_order_id"]
    assert order_id is not None
    with connect(Role.WH_READER) as conn:
        statuses = conn.execute(
            "select distinct order_status from marts.fct_order_item where order_id = %s",
            (order_id,),
        ).fetchall()
    assert statuses == [("delivered",)]


def test_full_refresh_equals_incremental(incremental_state):
    incremental = fingerprint_all()
    rebuilt = run_dbt(incremental_state["tmp"], "build", "--full-refresh")
    assert rebuilt.returncode == 0, rebuilt.stdout[-3000:]
    full = fingerprint_all()
    assert set(incremental) == set(FINGERPRINTABLE)
    for model in FINGERPRINTABLE:
        assert incremental[model].rows > 0, model
        assert incremental[model] == full[model], model


def test_marts_reconcile_with_source_ledger(incremental_state):
    # The late update changes status only, so revenue and inventory still match the ledger.
    ledger = build_ledger(incremental_state["ds"])
    assert revenue.reconcile_source_vs_mart("it-m2").status == "pass"
    assert revenue.reconcile_ledger_vs_mart(ledger, "it-m2").status == "pass"
    assert revenue.reconcile_inventory_ledger_vs_mart(ledger, "it-m2").status == "pass"


def test_scd2_keeps_every_intermediate_version(incremental_state):
    """Two price changes between extractions must both land as versions (the batch path
    reads the ordered change log instead of snapshotting shop.products)."""
    product_id = 1
    with connect(Role.SRC_APP) as conn:
        for price in (111_00 - 1, 222_00 - 1):
            with conn.transaction():
                conn.execute(
                    "update shop.products set list_price_cents = %s where product_id = %s",
                    (price, product_id),
                )
    since = batch.watermark("product_versions")
    res = batch.ingest_table("product_versions", since, datetime.now(UTC) + timedelta(seconds=1))
    assert res.inserted_rows == 2
    assert (
        run_dbt(incremental_state["tmp"], "build", "--select", "+dim_product_history+").returncode
        == 0
    )
    with connect(Role.WH_READER) as conn:
        rows = conn.execute(
            "select version, list_price_cents, is_current from marts.dim_product_history"
            " where product_id = %s order by version",
            (product_id,),
        ).fetchall()
    assert [r[1] for r in rows[-2:]] == [11_099, 22_199]
    assert [r[2] for r in rows] == [False] * (len(rows) - 1) + [True]


def test_unchanged_update_creates_no_version(incremental_state):
    with connect(Role.SRC_APP) as conn, conn.transaction():
        before = conn.execute("select count(*) from shop.product_versions").fetchone()[0]
        conn.execute("update shop.products set name = name where product_id = 2")
        after = conn.execute("select count(*) from shop.product_versions").fetchone()[0]
    assert before == after
