"""Replay a generated dataset into the source database through simulated time.

`load_source` writes the *final* state in one shot, which is enough for reconciliation but
unrealistic for incremental pipelines: an order that was placed on day 1 and delivered on
day 5 would first become visible on day 5. The simulator instead derives every row's state
history from the generated data and applies, for each step, all changes effective up to the
simulated clock. The source therefore evolves the way an operational database does, which
is what incremental extraction (and CDC in the full profile) needs.

Invariant (tested): replaying to the simulation end produces exactly the final dataset.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from dataops.config import Role
from dataops.db import session
from dataops.generator.generate import _EVENT_SEQUENCE, PRIMARY_KEYS, TABLE_ORDER, Dataset, Row

log = logging.getLogger(__name__)

Change = tuple[datetime, int, str, Row]  # (effective_at, table rank, table, row snapshot)

_INSERT_ONLY_EFFECTIVE = {
    "warehouses": "created_at",
    "customers": "created_at",
    "product_versions": "recorded_at",
    "order_items": "created_at",
    "refunds": "refunded_at",
    "inventory_movements": "occurred_at",
    "shipment_events": "received_at",
}
_RANK = {t: i for i, t in enumerate(TABLE_ORDER)}


class SimulationStateError(RuntimeError):
    pass


def _order_changes(ds: Dataset) -> Iterable[tuple[datetime, Row]]:
    t = ds.tables
    payments_by_order: dict[int, list[Row]] = defaultdict(list)
    for p in t["payments"]:
        payments_by_order[p["order_id"]].append(p)
    refunds_by_payment: dict[int, list[Row]] = defaultdict(list)
    for r in t["refunds"]:
        refunds_by_payment[r["payment_id"]].append(r)
    shipment_by_order = {s["order_id"]: s for s in t["shipments"]}
    events_by_shipment: dict[int, list[Row]] = defaultdict(list)
    for e in t["shipment_events"]:
        events_by_shipment[e["shipment_id"]].append(e)

    for final in t["orders"]:
        oid = final["order_id"]
        base = dict(final, status="placed", cancelled_at=None, updated_at=final["placed_at"])
        yield final["placed_at"], base
        if final["status"] == "cancelled":
            yield final["cancelled_at"], dict(final)
            continue
        captured = [p for p in payments_by_order[oid] if p["captured_at"] is not None]
        if not captured:
            continue
        payment = captured[0]
        state = dict(base, status="paid", updated_at=payment["captured_at"])
        yield state["updated_at"], dict(state)
        refund_at = None
        if payment["status"] == "refunded":
            refund_at = refunds_by_payment[payment["payment_id"]][0]["refunded_at"]
        shipment = shipment_by_order.get(oid)
        receipts = sorted(
            events_by_shipment[shipment["shipment_id"]] if shipment else [],
            key=lambda e: e["received_at"],
        )
        delivered = False
        for ev in receipts:
            if refund_at is not None and ev["received_at"] >= refund_at:
                break  # refunded orders ignore later shipment updates (generator rule)
            delivered = delivered or ev["event_type"] == "delivered"
            state = dict(
                state,
                status="delivered" if delivered else "shipped",
                updated_at=max(state["updated_at"], ev["received_at"]),
            )
            yield ev["received_at"], dict(state)
        if refund_at is not None:
            yield refund_at, dict(state, status="refunded", updated_at=refund_at)


def _payment_changes(ds: Dataset) -> Iterable[tuple[datetime, Row]]:
    refunds_by_payment: dict[int, list[Row]] = defaultdict(list)
    for r in ds.tables["refunds"]:
        refunds_by_payment[r["payment_id"]].append(r)
    for final in ds.tables["payments"]:
        pending = dict(
            final,
            status="pending",
            captured_at=None,
            failure_reason=None,
            updated_at=final["created_at"],
        )
        yield final["created_at"], pending
        if final["status"] == "pending":
            continue
        if final["status"] == "failed":
            yield final["updated_at"], dict(final)
            continue
        captured = dict(final, status="succeeded", updated_at=final["captured_at"])
        yield final["captured_at"], captured
        if final["status"] == "refunded":
            yield final["updated_at"], dict(final)


def _shipment_changes(ds: Dataset) -> Iterable[tuple[datetime, Row]]:
    events_by_shipment: dict[int, list[Row]] = defaultdict(list)
    for e in ds.tables["shipment_events"]:
        events_by_shipment[e["shipment_id"]].append(e)
    for final in ds.tables["shipments"]:
        state = dict(final, status="label_created", updated_at=final["created_at"])
        yield final["created_at"], dict(state)
        seen: list[str] = []
        for ev in sorted(events_by_shipment[final["shipment_id"]], key=lambda e: e["received_at"]):
            seen.append(ev["event_type"])
            state = dict(
                state,
                status=max(seen, key=_EVENT_SEQUENCE.index),
                updated_at=max(state["updated_at"], ev["received_at"]),
            )
            yield ev["received_at"], dict(state)


def _product_changes(ds: Dataset) -> Iterable[tuple[datetime, Row]]:
    versions: dict[int, list[Row]] = defaultdict(list)
    for v in ds.tables["product_versions"]:
        versions[v["product_id"]].append(v)
    for final in ds.tables["products"]:
        for v in sorted(versions[final["product_id"]], key=lambda v: v["version"]):
            yield (
                v["valid_from"],
                dict(final, list_price_cents=v["list_price_cents"], updated_at=v["valid_from"]),
            )


def changes(ds: Dataset) -> list[Change]:
    """Every row state of the dataset with the time it took effect, in apply order."""
    out: list[Change] = []
    for table, column in _INSERT_ONLY_EFFECTIVE.items():
        out.extend((r[column], _RANK[table], table, r) for r in ds.tables[table])
    for table, gen in (
        ("orders", _order_changes),
        ("payments", _payment_changes),
        ("shipments", _shipment_changes),
        ("products", _product_changes),
    ):
        out.extend((when, _RANK[table], table, row) for when, row in gen(ds))
    # Stable sort: within one instant, parents (lower rank) before children, and the
    # generator's own order preserved for successive states of the same row.
    out.sort(key=lambda c: (c[0], c[1]))
    return out


def snapshot_at(ds: Dataset, until: datetime) -> dict[str, list[Row]]:
    """Source state at `until` (latest state of every row effective at or before it)."""
    state: dict[str, dict[Any, Row]] = {t: {} for t in TABLE_ORDER}
    for when, _rank, table, row in changes(ds):
        if when > until:
            break
        state[table][row[PRIMARY_KEYS[table]]] = row
    return {t: sorted(rows.values(), key=lambda r: r[PRIMARY_KEYS[t]]) for t, rows in state.items()}


def _upsert(conn: Any, table: str, rows: list[Row]) -> None:
    if not rows:
        return
    pk = PRIMARY_KEYS[table]
    cols = list(rows[0].keys())
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != pk)
    sql = (
        f"INSERT INTO shop.{table} ({', '.join(cols)}) "  # noqa: S608 - allowlisted identifiers
        f"VALUES ({', '.join(['%s'] * len(cols))}) "
        f"ON CONFLICT ({pk}) DO UPDATE SET {updates}"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, [[r[c] for c in cols] for r in rows])


def simulate(ds: Dataset, until: datetime, *, reset: bool = False) -> dict[str, Any]:
    """Advance the source to `until`. Idempotent; refuses to run against a source that was
    produced from a different dataset, or to move the clock backwards."""
    checksum = ds.checksum()
    with (
        session(
            Role.SRC_APP, application_name="dataops-simulator", statement_timeout_s=600
        ) as conn,
        conn.transaction(),
    ):
        if reset:
            conn.execute(
                "TRUNCATE "
                + ", ".join(f"shop.{t}" for t in reversed(TABLE_ORDER))
                + " RESTART IDENTITY"
            )
            conn.execute("DELETE FROM sim.clock")
        clock = conn.execute(
            "SELECT dataset_checksum, simulated_until FROM sim.clock FOR UPDATE"
        ).fetchone()
        if clock is None:
            existing = conn.execute("SELECT count(*) FROM shop.orders").fetchone()
            if existing and existing[0]:
                raise SimulationStateError(
                    "source holds data that was not produced by the simulator; "
                    "use --reset to start a simulation from an empty source"
                )
            since = None
        else:
            if clock[0] != checksum:
                raise SimulationStateError(
                    "source was simulated from a different dataset (seed/profile); "
                    "use --reset to restart"
                )
            since = clock[1]
            if until < since:
                raise SimulationStateError(
                    f"clock is at {since.isoformat()}; cannot move back to {until.isoformat()}"
                )
        pending: dict[str, dict[Any, Row]] = defaultdict(dict)
        for when, _rank, table, row in changes(ds):
            if when > until:
                break
            if since is not None and when <= since:
                continue
            pending[table][row[PRIMARY_KEYS[table]]] = row  # latest state wins
        # Product versions are written explicitly from the generated change log.
        conn.execute("ALTER TABLE shop.products DISABLE TRIGGER products_log_version")
        counts = {}
        for table in TABLE_ORDER:
            rows = list(pending[table].values())
            _upsert(conn, table, rows)
            counts[table] = len(rows)
        conn.execute("ALTER TABLE shop.products ENABLE TRIGGER products_log_version")
        conn.execute(
            "SELECT setval(pg_get_serial_sequence('shop.product_versions',"
            " 'product_version_id'), (SELECT COALESCE(MAX(product_version_id), 0) + 1"
            " FROM shop.product_versions), false)"
        )
        conn.execute(
            "INSERT INTO sim.clock (id, seed, profile, dataset_checksum, simulated_until)"
            " VALUES (1, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET"
            " simulated_until = EXCLUDED.simulated_until, updated_at = clock_timestamp()",
            (ds.seed, ds.profile.name, checksum, until),
        )
    result = {
        "simulated_from": since.isoformat() if since else None,
        "simulated_until": until.isoformat(),
        "changed_rows": counts,
    }
    log.info("simulation advanced: %s", result)
    return result
