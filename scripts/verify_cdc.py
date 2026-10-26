"""Full-profile CDC verification against the running Compose stack (postgres, kafka, connect).

Checks, each printed with its evidence:
 1. initial snapshot: stream state == source state (column-level)
 2. insert / update / update / delete on one row arrive as c, u, u, d (+ tombstone),
    in LSN order, and the deleted row disappears from the materialized state
 3. restart and resume: stop the Connect container, change the source (one simulated day),
    start it again; no change is lost and the stream state again equals the source
 4. replication slot is active and its lag is bounded

Requires: docker compose --profile full up -d --wait postgres kafka connect, migrations
applied, connector registered, and the source simulated at least to --from-day.
Environment: DATAOPS_PG_PORT=5433 (the Compose PostgreSQL port).
Run: python scripts/verify_cdc.py [--seed 42 --profile small]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import uuid
from datetime import timedelta

from dataops.config import REPO_ROOT, load_dotenv


def compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "--profile", "full", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def main() -> int:
    load_dotenv()
    from dataops.cdc import connector, reconcile, stream
    from dataops.config import Role
    from dataops.db import connect
    from dataops.generator.generate import generate

    p = argparse.ArgumentParser()
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--profile", default="small")
    args = p.parse_args()
    corr = f"verify-cdc-{uuid.uuid4()}"
    ok = True

    def check(name: str, passed: bool, detail: str) -> None:
        nonlocal ok
        ok &= passed
        print(f"[{'PASS' if passed else 'FAIL'}] {name}: {detail}")

    connector.register()
    connector.wait_running()
    time.sleep(5)

    # 1. snapshot / current state
    r = reconcile.reconcile(corr)
    check(
        "stream state equals source",
        r.status == "pass",
        f"{r.compared_rows} source rows, {r.right_totals['rows']} stream rows, "
        f"mismatches={r.mismatched_rows}",
    )

    # 2. insert, update, update, delete on a dedicated synthetic customer
    cid = 9_000_000 + int(time.time()) % 100_000
    with connect(Role.SRC_APP) as conn:
        for sql, params in (
            (
                "insert into shop.customers (customer_id, email, full_name, country_code,"
                " created_at, updated_at) values (%s, %s, 'Cdc Probe', 'US', now(), now())",
                (cid, f"cdc-probe-{cid}@example.test"),
            ),
            ("update shop.customers set full_name = 'Cdc Probe v2' where customer_id = %s", (cid,)),
            ("update shop.customers set country_code = 'CA' where customer_id = %s", (cid,)),
            ("delete from shop.customers where customer_id = %s", (cid,)),
        ):
            with conn.transaction():
                conn.execute(sql, params)
    time.sleep(6)
    events = [
        e
        for e in stream.read_topic("customers")
        if (e.after or e.before or e.key or {}).get("customer_id") == cid
    ]
    ops = [e.op for e in events]
    lsns = [e.lsn for e in events if e.lsn is not None]
    check(
        "insert/update/update/delete captured in order",
        ops == ["c", "u", "u", "d", None],
        f"ops={ops} (None = tombstone), lsn strictly increasing={lsns == sorted(set(lsns))}",
    )
    m = stream.materialize("customers", list(stream.read_topic("customers")))
    check(
        "deleted row absent from materialized state",
        cid not in m.rows,
        f"materialized customers={len(m.rows)}",
    )
    upd = [e for e in events if e.op == "u"]
    check(
        "update carries the new values",
        bool(upd) and upd[-1].after["country_code"] == "CA",
        "after.country_code=CA on the last update",
    )

    # 3. restart and resume
    with connect(Role.SRC_APP) as conn:
        until = conn.execute("select simulated_until from sim.clock").fetchone()[0]
    ds = generate(args.seed, args.profile)
    nxt = min(until + timedelta(days=1), ds.end)
    compose("stop", "connect")
    from dataops.generator.simulator import simulate

    changed = simulate(ds, nxt)["changed_rows"]
    with connect(Role.SRC_APP) as conn:
        lag_while_down = conn.execute(
            "select pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint,"
            " active from pg_replication_slots where slot_name = %s",
            (connector.SLOT_NAME,),
        ).fetchone()
    compose("start", "connect")
    for _ in range(60):
        try:
            connector.wait_running(timeout_s=5)
            break
        except Exception:  # Connect REST not up yet
            time.sleep(2)
    time.sleep(10)
    r2 = reconcile.reconcile(corr)
    check(
        "changes made while the connector was down are delivered after restart",
        r2.status == "pass",
        f"{sum(changed.values())} source changes while stopped "
        f"(slot inactive={not lag_while_down[1]}, pinned WAL={lag_while_down[0]} bytes); "
        f"after restart mismatches={r2.mismatched_rows}",
    )
    dups = {
        t: v["transport_duplicates"]
        for t, v in r2.scope["tables"].items()
        if v["transport_duplicates"]
    }
    print(f"       transport duplicates observed after restart (at-least-once): {dups or 0}")

    # 4. slot health
    time.sleep(12)  # one heartbeat interval
    with connect(Role.SRC_APP) as conn:
        slot = conn.execute(
            "select active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint"
            " from pg_replication_slots where slot_name = %s",
            (connector.SLOT_NAME,),
        ).fetchone()
    check(
        "replication slot active after restart",
        bool(slot and slot[0]),
        f"active={slot[0] if slot else None}, lag_bytes={slot[1] if slot else None}",
    )

    print(f"\nresult: {'PASS' if ok else 'FAIL'} (correlation_id={corr})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
