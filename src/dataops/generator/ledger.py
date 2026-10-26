"""Expected-results ledger: metrics computed in Python directly from the generated dataset.

This is an independent implementation of docs/business_rules.md. Reconciliation compares
the warehouse marts against it, so the SQL path (ingestion + dbt) is checked against code
that never touches the database.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from dataops.generator.generate import Dataset, line_amount

CAPTURED_STATUSES = {"succeeded", "refunded"}


def daily_revenue(ds: Dataset) -> dict[str, dict[str, int]]:
    """Return {iso_date: metrics} for every UTC date in the simulation window."""
    days: dict[str, dict[str, int]] = {}
    d: date = ds.start.date()
    while d < ds.end.date():
        days[d.isoformat()] = {
            "orders_placed": 0,
            "orders_cancelled": 0,
            "booked_revenue_cents": 0,
            "captured_cents": 0,
            "refunded_cents": 0,
            "net_collected_cents": 0,
        }
        d += timedelta(days=1)

    order_total: dict[int, int] = defaultdict(int)
    for item in ds.tables["order_items"]:
        order_total[item["order_id"]] += line_amount(item)

    for order in ds.tables["orders"]:
        row = days[order["placed_at"].date().isoformat()]
        row["orders_placed"] += 1
        if order["status"] == "cancelled":
            row["orders_cancelled"] += 1
        else:
            row["booked_revenue_cents"] += order_total[order["order_id"]]

    for p in ds.tables["payments"]:
        if p["status"] in CAPTURED_STATUSES:
            days[p["captured_at"].date().isoformat()]["captured_cents"] += p["amount_cents"]

    for r in ds.tables["refunds"]:
        days[r["refunded_at"].date().isoformat()]["refunded_cents"] += r["amount_cents"]

    for row in days.values():
        row["net_collected_cents"] = row["captured_cents"] - row["refunded_cents"]
    return days


def inventory_positions(ds: Dataset) -> dict[str, dict[str, int]]:
    pos: dict[str, dict[str, int]] = {}
    for m in ds.tables["inventory_movements"]:
        key = f"{m['product_id']}:{m['warehouse_id']}"
        p = pos.setdefault(key, {"on_hand": 0, "allocated": 0})
        p["on_hand"] += m["on_hand_delta"]
        p["allocated"] += m["allocated_delta"]
    for p in pos.values():
        p["available"] = p["on_hand"] - p["allocated"]
    return dict(sorted(pos.items()))


def build_ledger(ds: Dataset) -> dict[str, Any]:
    daily = daily_revenue(ds)
    totals = {k: sum(r[k] for r in daily.values()) for k in next(iter(daily.values()))}
    return {
        "ledger_version": "1",
        "synthetic": True,
        "seed": ds.seed,
        "profile": ds.profile.name,
        "simulation_start": ds.start.isoformat(),
        "simulation_end": ds.end.isoformat(),
        "dataset_checksum": ds.checksum(),
        "row_counts": ds.row_counts(),
        "daily_revenue": daily,
        "revenue_totals": totals,
        "inventory_positions": inventory_positions(ds),
    }


def write_ledger(ledger: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
