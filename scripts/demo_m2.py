"""Milestone 2 (lightweight profile) demonstration: a simulated month of daily operations.

For each simulated day: advance the source clock, extract that day's window, and run dbt
incrementally every N days. Then prove that the incremental facts equal a full refresh and
reconcile revenue and inventory against the generator's ledger.

Usage: python scripts/demo_m2.py [--seed 42] [--profile small] [--dbt-every 5]
Environment: same .env as the CLI.
DESTRUCTIVE for the synthetic demo only: with --reset it empties the source, the raw zone and
the dbt schemas first (python -m dataops reset). Without --reset it refuses to run against a
non-empty environment.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta

from dataops.config import REPO_ROOT, load_dotenv


def dbt(*args: str) -> None:
    proc = subprocess.run(
        [
            "dbt",
            *args,
            "--project-dir",
            str(REPO_ROOT / "dbt"),
            "--profiles-dir",
            str(REPO_ROOT / "dbt"),
            "--quiet",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        print(proc.stdout[-4000:], proc.stderr[-2000:], sep="\n")
        raise SystemExit(f"dbt {' '.join(args)} failed")


def main() -> int:
    load_dotenv()
    from dataops.generator.generate import TABLE_ORDER, generate
    from dataops.generator.ledger import build_ledger
    from dataops.generator.simulator import simulate
    from dataops.ingestion.batch import ingest_table, split_windows, watermark
    from dataops.reconciliation import revenue
    from dataops.reconciliation.fingerprint import fingerprint_all

    p = argparse.ArgumentParser()
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--profile", default="small")
    p.add_argument("--dbt-every", type=int, default=5)
    p.add_argument("--reset", action="store_true", help="reset the synthetic environment first")
    args = p.parse_args()

    if args.reset:
        from dataops.admin import reset_synthetic_environment

        reset_synthetic_environment()
    elif any(watermark(t) for t in TABLE_ORDER):
        raise SystemExit("warehouse raw zone is not empty; re-run with --reset (destructive)")

    ds = generate(args.seed, args.profile)
    corr = f"demo-m2-{uuid.uuid4()}"
    print(
        f"Synthetic dataset seed={args.seed} profile={args.profile} "
        f"{ds.start.date()}..{ds.end.date()} checksum={ds.checksum()[:12]}"
    )

    t0 = time.perf_counter()
    simulate(ds, ds.start)
    history = datetime(2020, 1, 1, tzinfo=UTC)
    for table in TABLE_ORDER:
        ingest_table(table, watermark(table) or history, ds.start, correlation_id=corr)
    windows = split_windows(ds.start, ds.end, timedelta(days=1))
    inserted = 0
    for day, (w_start, w_end) in enumerate(windows, start=1):
        changed = simulate(ds, w_end)["changed_rows"]
        results = [ingest_table(t, w_start, w_end, correlation_id=corr) for t in TABLE_ORDER]
        inserted += sum(r.inserted_rows for r in results)
        line = (
            f"day {day:2d} {w_start.date()} source changes={sum(changed.values()):5d} "
            f"raw inserted={sum(r.inserted_rows for r in results):5d}"
        )
        if day % args.dbt_every == 0 or day == len(windows):
            dbt("build")
            line += "  dbt build (incremental) ok"
        print(line)
    print(f"daily loop: {len(windows)} days, {inserted} raw rows, {time.perf_counter() - t0:.1f}s")

    print("\nFull-refresh vs incremental equivalence")
    incremental = fingerprint_all()
    dbt("build", "--full-refresh")
    full = fingerprint_all()
    equal = True
    for model, fp in incremental.items():
        same = fp == full[model]
        equal &= same
        print(f"  {model:36} rows={fp.rows:6d} {'equal' if same else 'DIFFERENT'}")

    print("\nReconciliation")
    ledger = build_ledger(ds)
    checks = [
        revenue.reconcile_row_counts(corr),
        revenue.reconcile_source_vs_mart(corr),
        revenue.reconcile_ledger_vs_mart(ledger, corr),
        revenue.reconcile_inventory_ledger_vs_mart(ledger, corr),
    ]
    for c in checks:
        print(
            f"  {c.check_id:40} {c.status:5} rows={c.compared_rows} mismatches={c.mismatched_rows}"
        )
    ok = equal and all(c.status == "pass" for c in checks)
    print(f"\nresult: {'PASS' if ok else 'FAIL'} (correlation_id={corr})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
