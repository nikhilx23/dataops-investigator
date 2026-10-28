"""Failure scenarios 2-8 (scenario 1 lives in schema_change.py). Deterministic, reversible,
synthetic data only.

Each scenario has: an injection, a state check, a cleanup, the dataset scope and its ground
truth (expected detection, impact, known cause, recovery plan, verification). Ground truth is
used by the scenario runner and the docs; it is never stored where the investigator's tools
can read it.

Cleanups that remove rows the scenario *inserted* also remove those rows' raw landing
records: batch extraction cannot observe deletes (documented limitation), so this harness
step stands in for "the source owner deleted the bad rows and the CDC path propagated it".
It is a scenario reset, never a recovery action.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from dataops.config import Role
from dataops.db import connect

STATE_TABLE = "sim.scenario_state"  # created on demand in the source DB (sim schema, src_app)
BASE_ID = 900_000_000  # ids of rows inserted by scenarios start here


@dataclass
class Scenario:
    id: str
    number: int
    title: str
    scope: dict[str, Any]
    ground_truth: dict[str, Any]
    recovery_plan: list[dict[str, Any]] = field(default_factory=list)


def _src() -> Any:
    return connect(Role.SRC_APP, application_name="dataops-scenario")


def _ensure_state(conn: Any) -> None:
    conn.execute(
        "create table if not exists sim.scenario_state (scenario_id text primary key,"
        " injected_at timestamptz not null, details jsonb not null)"
    )


def _save(sid: str, details: dict[str, Any]) -> None:
    with _src() as conn, conn.transaction():
        _ensure_state(conn)
        conn.execute(
            "insert into sim.scenario_state values (%s, clock_timestamp(), %s)"
            " on conflict (scenario_id) do update set injected_at=excluded.injected_at,"
            " details=excluded.details",
            (sid, json.dumps(details, default=str)),
        )


def _load(sid: str) -> dict[str, Any] | None:
    with _src() as conn:
        _ensure_state(conn)
        conn.commit()
        row = conn.execute(
            "select details from sim.scenario_state where scenario_id=%s", (sid,)
        ).fetchone()
    return row[0] if row else None


def _forget(sid: str) -> None:
    with _src() as conn, conn.transaction():
        _ensure_state(conn)
        conn.execute("delete from sim.scenario_state where scenario_id=%s", (sid,))


def state(sid: str) -> str:
    return "injected" if _load(sid) is not None else "clean"


def _remove_raw(table: str, pks: list[int]) -> int:
    with connect(Role.WH_LOADER, application_name="dataops-scenario") as conn, conn.transaction():
        return conn.execute(
            "delete from raw.source_records where source_system='commerce_source' and"
            " source_table=%s"
            " and source_pk = any(%s)",
            (table, [str(p) for p in pks]),
        ).rowcount


def _new_orders(conn: Any, n: int, offset: int) -> dict[str, list[int]]:
    """Insert n complete, valid orders (1 item each, captured payment) with updated_at = now."""
    cust, prod, price = conn.execute(
        "select c.customer_id, p.product_id, p.list_price_cents from shop.customers c,"
        " shop.products p"
        " where p.is_active order by c.customer_id, p.product_id limit 1"
    ).fetchone()
    ids: dict[str, list[int]] = {"orders": [], "order_items": [], "payments": []}
    for i in range(n):
        oid = BASE_ID + offset + i
        conn.execute(
            "insert into shop.orders (order_id, customer_id, status, placed_at, updated_at)"
            " values (%s,%s,'paid',clock_timestamp(),clock_timestamp())",
            (oid, cust),
        )
        conn.execute(
            "insert into shop.order_items (order_item_id, order_id, line_number, product_id,"
            " quantity, unit_price_cents, created_at, updated_at) values"
            " (%s,%s,1,%s,1,%s,clock_timestamp(),clock_timestamp())",
            (oid, oid, prod, price),
        )
        conn.execute(
            "insert into shop.payments (payment_id, order_id, attempt_number, status, method,"
            " amount_cents, created_at, captured_at, updated_at) values"
            " (%s,%s,1,'succeeded','card',%s,"
            " clock_timestamp(),clock_timestamp(),clock_timestamp())",
            (oid, oid, price),
        )
        for k in ids:
            ids[k].append(oid)
    return ids


def _delete_inserted(ids: dict[str, list[int]]) -> dict[str, int]:
    removed: dict[str, int] = {}
    with _src() as conn, conn.transaction():
        for table, key in (
            ("refunds", "refund_id"),
            ("payments", "payment_id"),
            ("order_items", "order_item_id"),
            ("orders", "order_id"),
        ):
            if ids.get(table):
                conn.execute(f"delete from shop.{table} where {key} = any(%s)", (ids[table],))  # noqa: S608
    for table, pks in ids.items():
        if pks:
            removed[table] = _remove_raw(table, pks)
    return removed


# --------------------------------------------------------------- 2 duplicate business events
def inject_duplicates(n: int = 5) -> dict[str, Any]:
    with _src() as conn, conn.transaction():
        rows = conn.execute(
            "select refund_id, payment_id, amount_cents, reason, refunded_at from shop.refunds"
            " order by refund_id desc limit %s",
            (n,),
        ).fetchall()
        new = []
        for i, (_rid, pid, amt, reason, at) in enumerate(rows):
            nid = BASE_ID + 2000 + i
            conn.execute(
                "insert into shop.refunds (refund_id, payment_id, amount_cents, reason,"
                " refunded_at,"
                " updated_at) values (%s,%s,%s,%s,%s,clock_timestamp())",
                (nid, pid, amt, reason, at),
            )
            new.append(nid)
    details = {"refunds": new, "duplicated_from": [r[0] for r in rows]}
    _save("duplicate_business_events", details)
    return details


# --------------------------------------------------------------- 3 late-arriving payments
def _behind_watermark(table: str) -> datetime:
    """Latest updated_at that watermark-based extraction will not re-read (watermark minus
    the 15-minute overlap, minus a margin)."""
    from dataops.ingestion.batch import DEFAULT_OVERLAP, watermark

    wm = watermark(table)
    if wm is None:
        raise RuntimeError(f"{table} has never been extracted; run a pipeline cycle first")
    return wm - DEFAULT_OVERLAP - timedelta(minutes=2)


def inject_late_payments(n: int = 6) -> dict[str, Any]:
    """Pending payments are captured, but the payments service commits the change (and the
    order's move to 'paid') with a timestamp just after the previous version, behind the
    loader watermark, so watermark-based extraction never reads it."""
    limit = min(_behind_watermark("payments"), _behind_watermark("orders"))
    with _src() as conn, conn.transaction():
        rows = conn.execute(
            "select p.payment_id, p.created_at, greatest(p.updated_at, o.updated_at), o.order_id,"
            " o.status from shop.payments p join shop.orders o using (order_id)"
            " where p.status='pending' and greatest(p.updated_at, o.updated_at) + interval '1"
            " second' < %s"
            " order by p.payment_id limit %s",
            (limit, n),
        ).fetchall()
        if not rows:
            raise RuntimeError(
                "no pending payment is behind the watermark; run a pipeline cycle"
                " (or wait 20 minutes after the last cleanup)"
            )
        for pid, created, prev, oid, _ost in rows:
            ts = prev + timedelta(seconds=1)
            conn.execute(
                "update shop.payments set status='succeeded', captured_at=%s, updated_at=%s"
                " where payment_id=%s",
                (created + timedelta(minutes=7), ts, pid),
            )
            conn.execute(
                "update shop.orders set status='paid', updated_at=%s where order_id=%s", (ts, oid)
            )
    stamps = [r[2] + timedelta(seconds=1) for r in rows]
    details = {
        "payments": [{"payment_id": r[0], "order_id": r[3], "order_status": r[4]} for r in rows],
        "window": [min(stamps) - timedelta(minutes=1), max(stamps) + timedelta(minutes=1)],
    }
    if details["window"][1] - details["window"][0] > timedelta(days=31):
        raise RuntimeError("candidates span more than the 31-day backfill bound")
    _save("late_arriving_payments", details)
    return details


def cleanup_late_payments() -> dict[str, Any]:
    d = _load("late_arriving_payments")
    if not d:
        return {"status": "already_clean"}
    with _src() as conn, conn.transaction():
        for p in d["payments"]:
            conn.execute(
                "update shop.payments set status='pending', captured_at=null,"
                " updated_at=clock_timestamp() where payment_id=%s",
                (p["payment_id"],),
            )
            conn.execute(
                "update shop.orders set status=%s, updated_at=clock_timestamp() where order_id=%s",
                (p["order_status"], p["order_id"]),
            )
    _forget("late_arriving_payments")
    # The reversal moves payments backwards (captured -> pending), which the business never
    # does; incremental facts do not retract rows, so the cleanup ends with a full refresh.
    from dataops.ingestion.batch import ingest_table, watermark

    now = datetime.now(UTC) + timedelta(seconds=1)
    for t in ("orders", "payments"):
        ingest_table(t, watermark(t) or now - timedelta(days=1), now, run_id="scenario:cleanup")
    rc = _full_refresh(["stg_orders+", "stg_payments+"])
    return {"status": "reverted", "payments": len(d["payments"]), "full_refresh_exit": rc}


# --------------------------------------------------------------- 4 streaming interruption
def streaming_note() -> dict[str, Any]:
    return {
        "status": "full_profile_only",
        "inject": "stop the Spark consumer (docker compose stop or kill the `dataops stream run`"
        " process) while the time simulator produces changes",
        "verified_in": "Milestone 3 consumer-lag scenario (docs/reliability_platform.md)",
    }


# --------------------------------------------------------------- 5 stale upstream source
def inject_source_access_revoked(table: str = "orders", touch: int = 25) -> dict[str, Any]:
    with _src() as conn, conn.transaction():
        conn.execute(
            f"update shop.{table} set updated_at=clock_timestamp() where ctid in"  # noqa: S608
            f" (select ctid from shop.{table} order by updated_at desc limit %s)",
            (touch,),
        )
        conn.execute(f"revoke select on shop.{table} from src_reader")  # noqa: S608 - fixed table
    details = {"table": table, "touched": touch}
    _save("stale_upstream_source", details)
    return details


def cleanup_source_access() -> dict[str, Any]:
    d = _load("stale_upstream_source") or {"table": "orders"}
    with _src() as conn, conn.transaction():
        conn.execute(f"grant select on shop.{d['table']} to src_reader")  # noqa: S608
    _forget("stale_upstream_source")
    return {"status": "access_restored", "table": d["table"]}


# --------------------------------------------------------------- 6 orphan foreign key
def inject_orphans(n: int = 4) -> dict[str, Any]:
    """New orders arrive, but only order_items is extracted for the window (the orders task
    of that run did not happen), so the warehouse has items without parents."""
    from dataops.ingestion.batch import ingest_table, watermark

    with _src() as conn, conn.transaction():
        ids = _new_orders(conn, n, 6000)
    now = datetime.now(UTC) + timedelta(seconds=1)
    r = ingest_table(
        "order_items",
        watermark("order_items") or now - timedelta(days=1),
        now,
        run_id="scenario:orphan_foreign_key",
    )
    details = {"ids": ids, "items_batch": r.as_dict()}
    _save("orphan_foreign_key", details)
    return details


# --------------------------------------------------------------- 7 incomplete publication
def inject_crashed_load(n: int = 30) -> dict[str, Any]:
    """New orders arrive; the loader process for orders is killed mid-batch (SIGKILL after the
    first page). Nothing is published (atomic batches); the batch stays 'running'."""
    with _src() as conn, conn.transaction():
        ids = _new_orders(conn, n, 7000)
    child = subprocess.Popen(  # noqa: S603 - fixed module entry point
        [sys.executable, "-m", "dataops.scenarios.library", "--crashing-load"],
        env=dict(os.environ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out, err = child.communicate(timeout=300)
    details = {
        "ids": ids,
        "child_exit": child.returncode,
        "child_signal": -child.returncode if child.returncode < 0 else None,
    }
    _save("incomplete_publication", details)
    return details


def _crashing_load() -> None:
    from dataops.ingestion import batch

    original = batch.payload_checksum
    seen = {"n": 0}

    def crash_after_some(payload: dict[str, Any]) -> str:
        seen["n"] += 1
        if seen["n"] > 10:
            os.kill(os.getpid(), signal.SIGKILL)
        return original(payload)

    batch.payload_checksum = crash_after_some
    now = datetime.now(UTC) + timedelta(seconds=1)
    batch.ingest_table(
        "orders",
        batch.watermark("orders") or now - timedelta(days=1),
        now,
        page_size=5,
        run_id="scenario:incomplete_publication",
    )


# --------------------------------------------------------------- 8 refund revenue discrepancy
def inject_refund_corrections(n: int = 5) -> dict[str, Any]:
    """The refunds service corrects amounts (halves them) and writes the correction with a
    timestamp just after the previous version, far behind the extraction watermark, so
    watermark-based extraction never sees the new versions. Refunds behind the watermark are
    used, consecutive in updated_at so the recovery window stays small."""
    with _src() as conn, conn.transaction():
        rows = conn.execute(
            "select refund_id, amount_cents, refunded_at, updated_at from shop.refunds"
            " where amount_cents >= 200 and refund_id < %s"
            " and updated_at + interval '1 second' < %s"
            " order by updated_at desc limit %s",
            (BASE_ID, _behind_watermark("refunds"), n),
        ).fetchall()
        for rid, amt, _at, upd in rows:
            # the source bumps updated_at automatically unless the writer sets it; the faulty
            # writer sets it to one second after the previous version (far behind the watermark)
            conn.execute(
                "update shop.refunds set amount_cents=%s, updated_at=%s where refund_id=%s",
                (amt // 2, upd + timedelta(seconds=1), rid),
            )
    details = {
        "refunds": [{"refund_id": r[0], "amount_cents": r[1], "updated_at": r[3]} for r in rows],
        "window": [
            min(r[3] for r in rows) - timedelta(minutes=1),
            max(r[3] for r in rows) + timedelta(minutes=1),
        ],
    }
    _save("refund_revenue_discrepancy", details)
    return details


def cleanup_refund_corrections() -> dict[str, Any]:
    d = _load("refund_revenue_discrepancy")
    if not d:
        return {"status": "already_clean"}
    with _src() as conn, conn.transaction():
        for r in d["refunds"]:
            conn.execute(
                "update shop.refunds set amount_cents=%s, updated_at=clock_timestamp()"
                " where refund_id=%s",
                (r["amount_cents"], r["refund_id"]),
            )
    _forget("refund_revenue_discrepancy")
    return {"status": "reverted", "refunds": len(d["refunds"])}


def _full_refresh(selectors: list[str]) -> int:
    import shutil

    from dataops.config import REPO_ROOT

    target = REPO_ROOT / "dbt" / "target" / "scenarios" / "cleanup"
    return subprocess.run(  # noqa: S603 - fixed command and selectors
        [
            shutil.which("dbt") or "dbt",
            "build",
            "--full-refresh",
            "--select",
            *selectors,
            "--project-dir",
            str(REPO_ROOT / "dbt"),
            "--profiles-dir",
            str(REPO_ROOT / "dbt"),
            "--target-path",
            str(target),
            "--log-path",
            str(target / "logs"),
            "--quiet",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=1800,
    ).returncode


def cleanup_inserted(sid: str) -> dict[str, Any]:
    d = _load(sid)
    if not d:
        return {"status": "already_clean"}
    ids = d.get("ids") or {"refunds": d.get("refunds", [])}
    removed = _delete_inserted(ids)
    if sid == "incomplete_publication":
        with (
            connect(Role.WH_LOADER, application_name="dataops-scenario") as conn,
            conn.transaction(),
        ):
            conn.execute(
                "update raw.ingestion_batches set status='abandoned',"
                " finished_at=clock_timestamp(),"
                " error_message='scenario cleanup: crashed loader' where status='running'"
                " and run_id='scenario:incomplete_publication'"
            )
    _forget(sid)
    # Removed rows cannot be retracted by incremental facts either: rebuild them fully.
    rc = _full_refresh(["stg_orders+", "stg_order_items+", "stg_payments+", "stg_refunds+"])
    return {"status": "removed", "raw_records_removed": removed, "full_refresh_exit": rc}


def _iso(d: Any) -> str:
    return d if isinstance(d, str) else d.isoformat()


SCENARIOS: dict[str, Scenario] = {
    s.id: s
    for s in [
        Scenario(
            "duplicate_business_events",
            2,
            "Duplicate refund events from the refunds service",
            {"tables": ["refunds"], "rows": 5},
            {
                "root_dataset": "commerce_source.shop.refunds",
                "cause": "the refunds service submitted 5 refunds twice (new refund_id, same"
                " payment,"
                " amount and timestamp)",
                "expected_detection": [
                    "dbt test assert_no_duplicate_refund_events fails",
                    "refunds exceed captured amount (assert_refunds_never_exceed_capture)",
                    "downstream payment models skipped",
                ],
                "expected_impact": [
                    "warehouse.marts.fct_payment_transaction",
                    "warehouse.marts.mart_daily_revenue",
                    "bi.dashboard.revenue_overview",
                    "bi.dashboard.finance_reconciliation",
                ],
            },
            [
                {
                    "action": "escalate_to_owner",
                    "scope": {
                        "dataset_id": "commerce_source.shop.refunds",
                        "message": "Duplicate refund events (same payment, amount, timestamp)."
                        " Please remove the duplicates.",
                    },
                },
                {"cleanup_first": True},
                {
                    "action": "rebuild_models",
                    "scope": {"models": ["stg_refunds"], "full_refresh": True},
                },
            ],
        ),
        Scenario(
            "late_arriving_payments",
            3,
            "Captured payments committed behind the extraction watermark",
            {"tables": ["payments"], "rows": 6},
            {
                "root_dataset": "commerce_source.shop.payments",
                "cause": "captures committed with updated_at far behind the extraction"
                " watermark, so"
                " watermark-based extraction never read them",
                "expected_detection": [
                    "revenue.daily.source_vs_mart fails on the capture dates",
                    "ingestion.row_counts passes (same rows, different versions)",
                ],
                "expected_impact": [
                    "warehouse.marts.mart_daily_revenue",
                    "bi.dashboard.revenue_overview",
                ],
            },
            [
                {
                    "action": "bounded_backfill",
                    "scope": {"tables": ["orders", "payments"], "window": "from_injection"},
                },
                {
                    "action": "rebuild_models",
                    "scope": {"models": ["stg_orders", "stg_payments"], "full_refresh": True},
                },
            ],
        ),
        Scenario(
            "streaming_interruption",
            4,
            "Streaming consumer stopped while changes arrive (full profile)",
            {"profile": "full"},
            {
                "root_dataset": None,
                "cause": "Spark Bronze consumer not running",
                "expected_detection": ["consumer_lag on CDC topics", "stale cdc_pub tables"],
            },
        ),
        Scenario(
            "stale_upstream_source",
            5,
            "Extraction blocked: source read access revoked for orders",
            {"tables": ["orders"]},
            {
                "root_dataset": "commerce_source.shop.orders",
                "cause": "SELECT on shop.orders revoked from the extraction role; every orders"
                " batch fails",
                "expected_detection": [
                    "failed_critical_task batch_ingestion ingest orders (permission denied)",
                    "freshness stale for warehouse.raw.orders once the lag passes 26 h",
                ],
                "expected_impact": [
                    "warehouse.staging.stg_orders",
                    "warehouse.marts.mart_order_fulfillment",
                ],
            },
            [
                {
                    "action": "escalate_to_owner",
                    "scope": {
                        "dataset_id": "commerce_source.shop.orders",
                        "message": "Extraction role lost SELECT on shop.orders; please restore"
                        " the grant.",
                    },
                },
                {"cleanup_first": True},
                {"action": "retry_failed_batch", "scope": {"batch_id": "latest_failed:orders"}},
                {"action": "rebuild_models", "scope": {"models": ["stg_orders"]}},
            ],
        ),
        Scenario(
            "orphan_foreign_key",
            6,
            "Order items loaded without their orders",
            {
                "tables": ["orders", "order_items"],
                "rows": 4,
                "skip_extract": ["orders", "payments"],
            },
            {
                "root_dataset": "warehouse.raw.orders",
                "cause": "the orders extraction for the window did not run while order_items did",
                "expected_detection": [
                    "relationships test stg_order_items.order_id -> stg_orders fails"
                ],
                "expected_impact": [
                    "warehouse.marts.fct_order_item",
                    "warehouse.marts.mart_daily_revenue",
                ],
            },
            [
                {
                    "action": "bounded_backfill",
                    "scope": {"tables": ["orders", "payments"], "window": "last_day"},
                },
                {
                    "action": "rebuild_models",
                    "scope": {"models": ["stg_orders", "stg_order_items", "stg_payments"]},
                },
            ],
        ),
        Scenario(
            "incomplete_publication",
            7,
            "Loader killed mid-batch (nothing published)",
            {"tables": ["orders"], "rows": 30},
            {
                "root_dataset": "warehouse.raw.orders",
                "cause": "the orders loader process was killed during a batch; the batch never"
                " committed",
                "expected_detection": [
                    "ingestion.row_counts.source_vs_raw fails for orders",
                    "orphan payments / items in staging tests",
                ],
                "expected_impact": ["warehouse.staging.stg_orders"],
            },
            [
                {"action": "retry_failed_batch", "scope": {"batch_id": "latest_failed:orders"}},
                {
                    "action": "rebuild_models",
                    "scope": {"models": ["stg_orders", "stg_order_items", "stg_payments"]},
                },
            ],
        ),
        Scenario(
            "refund_revenue_discrepancy",
            8,
            "Refund corrections written behind the extraction watermark",
            {"tables": ["refunds"], "rows": 5},
            {
                "root_dataset": "commerce_source.shop.refunds",
                "cause": "refund amounts corrected in the source with back-dated updated_at"
                " (behind the"
                " extraction watermark); extraction missed the new versions",
                "expected_detection": [
                    "revenue.daily.source_vs_mart fails on refunded_cents for the refund dates"
                ],
                "expected_impact": [
                    "warehouse.marts.mart_daily_revenue",
                    "bi.dashboard.finance_reconciliation",
                ],
            },
            [
                {
                    "action": "bounded_backfill",
                    "scope": {"tables": ["refunds"], "window": "from_injection"},
                },
                {
                    "action": "rebuild_models",
                    "scope": {"models": ["stg_refunds"], "full_refresh": True},
                },
            ],
        ),
    ]
}

INJECT: dict[str, Callable[[], dict[str, Any]]] = {
    "duplicate_business_events": inject_duplicates,
    "late_arriving_payments": inject_late_payments,
    "stale_upstream_source": inject_source_access_revoked,
    "orphan_foreign_key": inject_orphans,
    "incomplete_publication": inject_crashed_load,
    "refund_revenue_discrepancy": inject_refund_corrections,
    "streaming_interruption": streaming_note,
}
CLEANUP: dict[str, Callable[[], dict[str, Any]]] = {
    "duplicate_business_events": lambda: cleanup_inserted("duplicate_business_events"),
    "late_arriving_payments": cleanup_late_payments,
    "stale_upstream_source": cleanup_source_access,
    "orphan_foreign_key": lambda: cleanup_inserted("orphan_foreign_key"),
    "incomplete_publication": lambda: cleanup_inserted("incomplete_publication"),
    "refund_revenue_discrepancy": cleanup_refund_corrections,
    "streaming_interruption": streaming_note,
}


if __name__ == "__main__" and "--crashing-load" in sys.argv:
    from dataops.config import load_dotenv

    load_dotenv()
    _crashing_load()
