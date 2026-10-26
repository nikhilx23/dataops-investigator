from collections import defaultdict

import pytest

from dataops.generator.generate import TABLE_ORDER, generate, line_amount
from dataops.generator.ledger import build_ledger, daily_revenue, inventory_positions


@pytest.fixture(scope="module")
def ds():
    return generate(7, "tiny")


@pytest.fixture(scope="module")
def ds_small():
    return generate(42, "small")


def test_same_seed_is_byte_identical():
    assert generate(11, "tiny").checksum() == generate(11, "tiny").checksum()


def test_different_seed_differs():
    assert generate(11, "tiny").checksum() != generate(12, "tiny").checksum()


def test_profile_changes_volume():
    assert len(generate(1, "small").tables["orders"]) > len(generate(1, "tiny").tables["orders"])


def test_primary_keys_unique(ds):
    from dataops.generator.generate import PRIMARY_KEYS

    for table in TABLE_ORDER:
        keys = [r[PRIMARY_KEYS[table]] for r in ds.tables[table]]
        assert len(keys) == len(set(keys)), table


def test_foreign_keys_resolve(ds_small):
    t = ds_small.tables
    orders = {o["order_id"] for o in t["orders"]}
    payments = {p["payment_id"] for p in t["payments"]}
    products = {p["product_id"] for p in t["products"]}
    customers = {c["customer_id"] for c in t["customers"]}
    assert all(i["order_id"] in orders and i["product_id"] in products for i in t["order_items"])
    assert all(o["customer_id"] in customers for o in t["orders"])
    assert all(p["order_id"] in orders for p in t["payments"])
    assert all(r["payment_id"] in payments for r in t["refunds"])


def test_timestamps_are_utc_and_within_simulation(ds_small):
    for o in ds_small.tables["orders"]:
        assert o["placed_at"].utcoffset().total_seconds() == 0
        assert ds_small.start <= o["placed_at"] < ds_small.end
    for e in ds_small.tables["shipment_events"]:
        assert e["event_at"] <= e["received_at"] <= ds_small.end


def test_payment_rules(ds_small):
    t = ds_small.tables
    order_total = defaultdict(int)
    for i in t["order_items"]:
        assert 0 <= i["discount_cents"] <= i["quantity"] * i["unit_price_cents"]
        order_total[i["order_id"]] += line_amount(i)
    captures_per_order = defaultdict(int)
    for p in t["payments"]:
        assert (p["captured_at"] is not None) == (p["status"] in {"succeeded", "refunded"})
        assert p["amount_cents"] == order_total[p["order_id"]]
        if p["captured_at"] is not None:
            captures_per_order[p["order_id"]] += 1
    assert max(captures_per_order.values()) == 1, "at most one capture per order"
    refunded = defaultdict(int)
    for r in t["refunds"]:
        refunded[r["payment_id"]] += r["amount_cents"]
    by_id = {p["payment_id"]: p for p in t["payments"]}
    for pid, amount in refunded.items():
        assert by_id[pid]["status"] in {"succeeded", "refunded"}
        assert amount <= by_id[pid]["amount_cents"]
        if by_id[pid]["status"] == "refunded":
            assert amount == by_id[pid]["amount_cents"]


def test_scenario_coverage(ds_small):
    """The default dataset exercises the cases the pipeline must handle."""
    t = ds_small.tables
    statuses = {p["status"] for p in t["payments"]}
    assert statuses == {"pending", "succeeded", "failed", "refunded"}
    by_id = {p["payment_id"]: p for p in t["payments"]}
    partial = [r for r in t["refunds"] if by_id[r["payment_id"]]["status"] == "succeeded"]
    assert partial, "partial refunds present"
    placed = {o["order_id"]: o["placed_at"] for o in t["orders"]}
    late = [
        p
        for p in t["payments"]
        if p["captured_at"] and p["captured_at"].date() > placed[p["order_id"]].date()
    ]
    assert late, "captures that land on a later day than the order"
    assert any(
        (e["received_at"] - e["event_at"]).total_seconds() > 6 * 3600 for e in t["shipment_events"]
    ), "late-arriving shipment events"
    assert any(
        len([v for v in ds_small.tables["product_versions"] if v["product_id"] == pid]) > 1
        for pid in {v["product_id"] for v in ds_small.tables["product_versions"]}
    ), "price changes"


def test_out_of_order_arrival(ds_small):
    events = ds_small.tables["shipment_events"]  # sorted by arrival
    inversions = sum(1 for a, b in zip(events, events[1:]) if b["event_at"] < a["event_at"])
    assert inversions > 0


def test_inventory_never_negative(ds_small):
    for pos in inventory_positions(ds_small).values():
        assert pos["allocated"] >= 0
        assert pos["available"] >= 0
        assert pos["on_hand"] >= 0


def test_ledger_definitions(ds):
    daily = daily_revenue(ds)
    total_lines = sum(line_amount(i) for i in ds.tables["order_items"])
    cancelled = {o["order_id"] for o in ds.tables["orders"] if o["status"] == "cancelled"}
    cancelled_lines = sum(
        line_amount(i) for i in ds.tables["order_items"] if i["order_id"] in cancelled
    )
    assert sum(d["booked_revenue_cents"] for d in daily.values()) == total_lines - cancelled_lines
    for d in daily.values():
        assert d["net_collected_cents"] == d["captured_cents"] - d["refunded_cents"]
    ledger = build_ledger(ds)
    assert ledger["synthetic"] is True
    assert ledger["row_counts"]["orders"] == len(ds.tables["orders"])
