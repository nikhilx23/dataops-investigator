"""Deterministic synthetic commerce data generator.

Same (seed, profile, start) -> byte-identical dataset. Determinism comes from a single
random.Random seeded with a string (stable across processes, unaffected by PYTHONHASHSEED),
sequential identifiers, and iteration only over ordered sequences.

Business rules implemented here are specified in docs/business_rules.md.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from dataops.generator.profiles import DEFAULT_START, PROFILES, VolumeProfile

Row = dict[str, Any]

TABLE_ORDER = [
    "warehouses",
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "refunds",
    "inventory_movements",
    "shipments",
    "shipment_events",
]
PRIMARY_KEYS = {
    "warehouses": "warehouse_id",
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "refunds": "refund_id",
    "inventory_movements": "movement_id",
    "shipments": "shipment_id",
    "shipment_events": "shipment_event_id",
}

_WAREHOUSES = [
    ("CHI1", "Chicago Fulfillment", "us-central"),
    ("NJ1", "Newark Fulfillment", "us-east"),
    ("RNO1", "Reno Fulfillment", "us-west"),
    ("DAL1", "Dallas Fulfillment", "us-south"),
    ("ATL1", "Atlanta Fulfillment", "us-southeast"),
    ("SEA1", "Seattle Fulfillment", "us-northwest"),
]
_CATEGORIES = ["kitchen", "outdoor", "electronics", "home", "toys", "apparel", "garden", "office"]
_ADJECTIVES = ["Compact", "Classic", "Pro", "Everyday", "Deluxe", "Eco", "Travel", "Smart"]
_NOUNS = [
    "Kettle",
    "Lamp",
    "Backpack",
    "Speaker",
    "Planter",
    "Mug",
    "Desk Organizer",
    "Jacket",
    "Blender",
    "Tent",
    "Headphones",
    "Puzzle",
    "Notebook",
    "Chair Pad",
    "Water Bottle",
]
_FIRST = [
    "Ava",
    "Liam",
    "Maya",
    "Noah",
    "Zoe",
    "Ethan",
    "Iris",
    "Omar",
    "Lena",
    "Ravi",
    "Sofia",
    "Theo",
    "Nina",
    "Jonah",
    "Priya",
    "Marco",
    "Hana",
    "Caleb",
    "Elena",
    "Diego",
]
_LAST = [
    "Nguyen",
    "Smith",
    "Garcia",
    "Okafor",
    "Kim",
    "Patel",
    "Rossi",
    "Silva",
    "Cohen",
    "Moreau",
    "Tanaka",
    "Brown",
    "Novak",
    "Haddad",
    "Lopez",
    "Fischer",
]
_COUNTRIES = ["US"] * 8 + ["CA", "MX"]
_CARRIERS = ["ParcelCo", "SwiftShip", "RouteOne"]
_EVENT_SEQUENCE = ["label_created", "picked_up", "in_transit", "out_for_delivery", "delivered"]
_HOUR_WEIGHTS = [1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 7, 8, 7, 6, 6, 7, 8, 10, 11, 10, 8, 5, 2]


@dataclass
class Dataset:
    seed: int
    profile: VolumeProfile
    start: datetime
    end: datetime
    tables: dict[str, list[Row]]
    # Not loaded into the source: complete product price history (for SCD expectations).
    product_versions: list[Row] = field(default_factory=list)

    def checksum(self) -> str:
        return hashlib.sha256(
            canonical_json(
                {"tables": self.tables, "product_versions": self.product_versions}
            ).encode()
        ).hexdigest()

    def row_counts(self) -> dict[str, int]:
        return {t: len(self.tables[t]) for t in TABLE_ORDER}


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"not JSON serializable: {type(value)!r}")


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=_json_default)


def line_amount(item: Row) -> int:
    return int(item["quantity"]) * int(item["unit_price_cents"]) - int(item["discount_cents"])


def _ts(base: datetime, rng: random.Random, lo_s: int, hi_s: int) -> datetime:
    return base + timedelta(seconds=rng.randint(lo_s, hi_s))


def generate(
    seed: int, profile: VolumeProfile | str = "small", start: datetime = DEFAULT_START
) -> Dataset:
    prof = PROFILES[profile] if isinstance(profile, str) else profile
    rng = random.Random(f"dataops-investigator:{seed}:{prof.name}")
    end = start + timedelta(days=prof.days)
    pre = start - timedelta(days=1)

    # ------------------------------------------------------------- dimensions
    warehouses: list[Row] = [
        {
            "warehouse_id": i + 1,
            "code": c,
            "name": n,
            "region": r,
            "created_at": pre - timedelta(days=400),
            "updated_at": pre - timedelta(days=400),
        }
        for i, (c, n, r) in enumerate(_WAREHOUSES[: prof.warehouses])
    ]

    customers: list[Row] = []
    for cid in range(1, prof.customers + 1):
        created = start - timedelta(days=rng.randint(2, 720), seconds=rng.randint(0, 86_399))
        first, last = rng.choice(_FIRST), rng.choice(_LAST)
        customers.append(
            {
                "customer_id": cid,
                "email": f"customer{cid:06d}@example.test",
                "full_name": f"{first} {last}",
                "country_code": rng.choice(_COUNTRIES),
                "created_at": created,
                "updated_at": created,
            }
        )

    products: list[Row] = []
    product_versions: list[Row] = []
    price_timeline: dict[int, list[tuple[datetime, int]]] = {}
    for pid in range(1, prof.products + 1):
        created = start - timedelta(days=rng.randint(30, 900))
        price = rng.randrange(499, 25_000, 100) - 1  # e.g. 1299, 4999
        name = f"{rng.choice(_ADJECTIVES)} {rng.choice(_NOUNS)} {pid:04d}"
        category = rng.choice(_CATEGORIES)
        timeline = [(created, price)]
        if rng.random() < 0.2:  # some products change price during the simulation
            for _ in range(rng.randint(1, 2)):
                when = _ts(start, rng, 3_600, prof.days * 86_400 - 3_600)
                factor = rng.choice([0.85, 0.9, 1.1, 1.15])
                timeline.append((when, int(round(price * factor / 100.0)) * 100 - 1))
            timeline.sort()
        price_timeline[pid] = timeline
        for idx, (valid_from, p) in enumerate(timeline):
            valid_to = timeline[idx + 1][0] if idx + 1 < len(timeline) else None
            product_versions.append(
                {
                    "product_id": pid,
                    "version": idx + 1,
                    "list_price_cents": p,
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                }
            )
        products.append(
            {
                "product_id": pid,
                "sku": f"SKU-{pid:05d}",
                "name": name,
                "category": category,
                "list_price_cents": timeline[-1][1],
                "is_active": True,
                "created_at": created,
                "updated_at": timeline[-1][0],
            }
        )

    def price_at(pid: int, when: datetime) -> int:
        current = price_timeline[pid][0][1]
        for valid_from, p in price_timeline[pid]:
            if valid_from <= when:
                current = p
        return current

    # ------------------------------------------------------- orders & payments
    orders: list[Row] = []
    items: list[Row] = []
    payments: list[Row] = []
    refunds: list[Row] = []
    shipments: list[Row] = []
    events: list[Row] = []
    # inventory work queue: (time, seq, kind, order_id)
    inv_queue: list[tuple[datetime, int, str, int]] = []
    order_lines: dict[int, list[Row]] = {}
    order_by_id: dict[int, Row] = {}
    shipment_plan: dict[int, Row] = {}

    oid, iid, pay_id, ref_id = 100_000, 0, 500_000, 900_000
    seq = 0
    for day in range(prof.days):
        day_start = start + timedelta(days=day)
        weekend = day_start.weekday() >= 5
        mean = prof.orders_per_day * (1.2 if weekend else 1.0)
        n_orders = max(0, int(round(rng.gauss(mean, mean * 0.12))))
        placed_times = sorted(
            day_start
            + timedelta(
                hours=rng.choices(range(24), _HOUR_WEIGHTS)[0], seconds=rng.randint(0, 3_599)
            )
            for _ in range(n_orders)
        )
        for placed in placed_times:
            oid += 1
            customer = customers[rng.randrange(len(customers))]
            n_lines = rng.choices([1, 2, 3, 4, 5], [45, 30, 15, 7, 3])[0]
            chosen = rng.sample(range(1, prof.products + 1), n_lines)
            lines: list[Row] = []
            for ln, pid in enumerate(chosen, start=1):
                iid += 1
                qty = rng.choices([1, 2, 3], [70, 22, 8])[0]
                unit = price_at(pid, placed)
                discount = (qty * unit) // 10 if rng.random() < 0.15 else 0
                lines.append(
                    {
                        "order_item_id": iid,
                        "order_id": oid,
                        "line_number": ln,
                        "product_id": pid,
                        "quantity": qty,
                        "unit_price_cents": unit,
                        "discount_cents": discount,
                        "created_at": placed,
                        "updated_at": placed,
                    }
                )
            items.extend(lines)
            order_lines[oid] = lines
            total = sum(line_amount(x) for x in lines)
            order: Row = {
                "order_id": oid,
                "customer_id": customer["customer_id"],
                "status": "placed",
                "currency": "USD",
                "placed_at": placed,
                "cancelled_at": None,
                "updated_at": placed,
            }
            orders.append(order)
            order_by_id[oid] = order

            fate = rng.random()
            attempt_time = _ts(placed, rng, 5, 120)
            attempt = 0
            if fate < 0.04:  # cancelled before any capture
                for _ in range(rng.randint(1, 2)):
                    attempt += 1
                    pay_id += 1
                    payments.append(
                        {
                            "payment_id": pay_id,
                            "order_id": oid,
                            "attempt_number": attempt,
                            "status": "failed",
                            "method": "card",
                            "amount_cents": total,
                            "created_at": attempt_time,
                            "captured_at": None,
                            "failure_reason": rng.choice(["card_declined", "insufficient_funds"]),
                            "updated_at": attempt_time + timedelta(seconds=3),
                        }
                    )
                    attempt_time = _ts(attempt_time, rng, 60, 600)
                cancelled_at = _ts(placed, rng, 900, 7_200)
                if cancelled_at <= end:
                    order["status"], order["cancelled_at"] = "cancelled", cancelled_at
                    order["updated_at"] = cancelled_at
                continue

            if rng.random() < 0.07:  # a failed first attempt, retried
                attempt += 1
                pay_id += 1
                payments.append(
                    {
                        "payment_id": pay_id,
                        "order_id": oid,
                        "attempt_number": attempt,
                        "status": "failed",
                        "method": "card",
                        "amount_cents": total,
                        "created_at": attempt_time,
                        "captured_at": None,
                        "failure_reason": "card_declined",
                        "updated_at": attempt_time + timedelta(seconds=3),
                    }
                )
                attempt_time = _ts(attempt_time, rng, 60, 900)

            late = rng.random() < 0.07
            method = "bank_transfer" if late else rng.choice(["card", "card", "wallet"])
            capture_at = (
                _ts(attempt_time, rng, 20 * 3_600, 60 * 3_600)
                if late
                else _ts(attempt_time, rng, 10, 1_800)
            )
            attempt += 1
            pay_id += 1
            payment: Row = {
                "payment_id": pay_id,
                "order_id": oid,
                "attempt_number": attempt,
                "status": "pending",
                "method": method,
                "amount_cents": total,
                "created_at": attempt_time,
                "captured_at": None,
                "failure_reason": None,
                "updated_at": attempt_time,
            }
            payments.append(payment)
            if capture_at > end:
                continue  # still pending at simulation end
            payment.update(status="succeeded", captured_at=capture_at, updated_at=capture_at)
            order.update(status="paid", updated_at=capture_at)
            seq += 1
            inv_queue.append((capture_at, seq, "allocate", oid))

            ship_at = _ts(capture_at, rng, 4 * 3_600, 30 * 3_600)
            full_refund_at: datetime | None = None
            if rng.random() < 0.04:
                full_refund_at = _ts(capture_at, rng, 2 * 3_600, 120 * 3_600)
                if full_refund_at > end:
                    full_refund_at = None
            refunded_before_ship = full_refund_at is not None and full_refund_at < ship_at

            if refunded_before_ship:
                assert full_refund_at is not None
                seq += 1
                inv_queue.append((full_refund_at, seq, "release", oid))
            elif ship_at <= end:
                seq += 1
                inv_queue.append((ship_at, seq, "ship", oid))
                shipment_plan[oid] = {"created_at": ship_at}

            if full_refund_at is not None:
                ref_id += 1
                refunds.append(
                    {
                        "refund_id": ref_id,
                        "payment_id": pay_id,
                        "amount_cents": total,
                        "reason": "customer_request",
                        "refunded_at": full_refund_at,
                        "updated_at": full_refund_at,
                    }
                )
                payment.update(status="refunded", updated_at=full_refund_at)
                order.update(status="refunded", updated_at=full_refund_at)
            elif rng.random() < 0.06:  # partial refund of one line
                refund_at = _ts(capture_at, rng, 24 * 3_600, 8 * 86_400)
                line = rng.choice(lines)
                amount = line_amount(line) if rng.random() < 0.6 else line_amount(line) // 2
                if refund_at <= end and amount > 0:
                    ref_id += 1
                    refunds.append(
                        {
                            "refund_id": ref_id,
                            "payment_id": pay_id,
                            "amount_cents": amount,
                            "reason": rng.choice(
                                ["damaged_item", "late_delivery", "price_adjustment"]
                            ),
                            "refunded_at": refund_at,
                            "updated_at": refund_at,
                        }
                    )

    # ------------------------------------------------- inventory (chronological)
    movements: list[Row] = []
    mov_id = 0
    on_hand: dict[tuple[int, int], int] = {}
    allocated: dict[tuple[int, int], int] = {}
    order_wh: dict[int, int] = {}

    def move(
        pid: int, wid: int, kind: str, d_on: int, d_alloc: int, when: datetime, ref: int | None
    ) -> None:
        nonlocal mov_id
        mov_id += 1
        on_hand[(pid, wid)] = on_hand.get((pid, wid), 0) + d_on
        allocated[(pid, wid)] = allocated.get((pid, wid), 0) + d_alloc
        movements.append(
            {
                "movement_id": mov_id,
                "product_id": pid,
                "warehouse_id": wid,
                "movement_type": kind,
                "on_hand_delta": d_on,
                "allocated_delta": d_alloc,
                "reference_order_id": ref,
                "occurred_at": when,
                "updated_at": when,
            }
        )

    initial_qty = max(20, prof.orders_per_day * prof.days // (prof.products * 2))
    for wh in warehouses:
        for prod in products:
            move(
                prod["product_id"],
                wh["warehouse_id"],
                "restock",
                initial_qty + rng.randint(0, initial_qty),
                0,
                pre,
                None,
            )

    def available(pid: int, wid: int) -> int:
        return on_hand.get((pid, wid), 0) - allocated.get((pid, wid), 0)

    for when, _seq, kind, order_id in sorted(inv_queue):
        lines = order_lines[order_id]
        if kind == "allocate":
            wid = next(
                (
                    w["warehouse_id"]
                    for w in warehouses
                    if all(
                        available(x["product_id"], w["warehouse_id"]) >= x["quantity"]
                        for x in lines
                    )
                ),
                None,
            )
            if wid is None:  # replenish the order's home warehouse just before allocation
                wid = warehouses[order_id % len(warehouses)]["warehouse_id"]
                for x in lines:
                    short = x["quantity"] - available(x["product_id"], wid)
                    if short > 0:
                        move(
                            x["product_id"],
                            wid,
                            "restock",
                            short + initial_qty,
                            0,
                            when - timedelta(minutes=30),
                            None,
                        )
            order_wh[order_id] = wid
            for x in lines:
                move(x["product_id"], wid, "allocation", 0, x["quantity"], when, order_id)
        elif kind == "release":
            for x in lines:
                move(
                    x["product_id"],
                    order_wh[order_id],
                    "allocation_release",
                    0,
                    -x["quantity"],
                    when,
                    order_id,
                )
        else:  # ship
            for x in lines:
                move(
                    x["product_id"],
                    order_wh[order_id],
                    "shipment",
                    -x["quantity"],
                    -x["quantity"],
                    when,
                    order_id,
                )
    movements.sort(key=lambda m: (m["occurred_at"], m["movement_id"]))

    # --------------------------------------------------- shipments & events
    ship_id, ev_id = 700_000, 0
    for order_id in sorted(shipment_plan):
        created = shipment_plan[order_id]["created_at"]
        ship_id += 1
        t = created
        offsets = [
            (0, 0),
            (2 * 3_600, 10 * 3_600),
            (3_600, 6 * 3_600),
            (24 * 3_600, 72 * 3_600),
            (2 * 3_600, 8 * 3_600),
        ]
        received_types: list[str] = []
        last_received = created
        for etype, (lo, hi) in zip(_EVENT_SEQUENCE, offsets, strict=True):
            t = t + timedelta(seconds=rng.randint(lo, hi)) if lo or hi else t
            if t > end:
                break
            lag = (
                rng.randint(6 * 3_600, 48 * 3_600)
                if (etype != "label_created" and rng.random() < 0.08)
                else rng.randint(30, 1_200)
            )
            received = t + timedelta(seconds=lag)
            if received > end:
                continue  # happened, but the source has not learned of it yet
            ev_id += 1
            events.append(
                {
                    "shipment_event_id": ev_id,
                    "shipment_id": ship_id,
                    "event_type": etype,
                    "event_at": t,
                    "received_at": received,
                    "updated_at": received,
                }
            )
            received_types.append(etype)
            last_received = max(last_received, received)
        status = (
            max(received_types, key=_EVENT_SEQUENCE.index) if received_types else "label_created"
        )
        shipments.append(
            {
                "shipment_id": ship_id,
                "order_id": order_id,
                "warehouse_id": order_wh[order_id],
                "carrier": rng.choice(_CARRIERS),
                "tracking_number": f"TRK{ship_id}{rng.randint(1000, 9999)}",
                "status": status,
                "created_at": created,
                "updated_at": last_received,
            }
        )
        order = order_by_id[order_id]
        if order["status"] != "refunded":
            order["status"] = "delivered" if status == "delivered" else "shipped"
            order["updated_at"] = max(order["updated_at"], last_received)

    events.sort(key=lambda e: (e["received_at"], e["shipment_event_id"]))
    for o in orders:  # updated_at must reflect the latest change visible in the source
        o["updated_at"] = max(o["updated_at"], o["placed_at"])

    tables = {
        "warehouses": warehouses,
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": items,
        "payments": payments,
        "refunds": refunds,
        "inventory_movements": movements,
        "shipments": shipments,
        "shipment_events": events,
    }
    return Dataset(seed, prof, start, end, tables, product_versions)
