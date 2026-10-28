"""Benchmarks on isolated *_bench databases (never the demo databases).

Measures, on the machine it runs on, wall-clock time of: synthetic generation + source load,
full batch extraction (rows/s), an idempotent re-extraction, a full dbt build, an incremental
dbt build after a 1% source change, reconciliation, metadata collection, incident evaluation,
hybrid runbook search and API latency (real uvicorn over HTTP). Results go to
benchmarks/results/<profile>-<timestamp>.json; docs/benchmarks.md explains the method.

    python benchmarks/run.py --profile small
    python benchmarks/run.py --profile medium
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BENCH_DBS = {
    "DATAOPS_SOURCE_DB": "commerce_source_bench",
    "DATAOPS_WAREHOUSE_DB": "warehouse_bench",
    "DATAOPS_RELIABILITY_DB": "reliability_bench",
    "DATAOPS_AIRFLOW_DB": "airflow_bench",
}


def timed(fn: Any, *a: Any, **k: Any) -> tuple[Any, float]:
    t = time.perf_counter()
    out = fn(*a, **k)
    return out, round(time.perf_counter() - t, 3)


def pct(values: list[float], p: float) -> float:
    s = sorted(values)
    return round(s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))], 2)


def fresh_databases() -> None:
    import psycopg

    from dataops.config import Role, connection_spec

    admin = connection_spec(Role.ADMIN, "dataops-bench")
    with psycopg.connect(
        host=admin.host,
        port=admin.port,
        user=admin.user,
        password=admin.password,
        dbname="postgres",
        autocommit=True,
    ) as conn:
        for name in BENCH_DBS.values():
            assert name.endswith("_bench"), name
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    subprocess.run(
        [
            "psql",
            "-X",
            "-q",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            admin.host,
            "-p",
            str(admin.port),
            "-U",
            admin.user,
            "-d",
            "postgres",
            "-f",
            str(ROOT / "infra/postgres/bootstrap.sql"),
        ],
        check=True,
        env=dict(os.environ, PGPASSWORD=admin.password),
        capture_output=True,
    )
    from dataops.migrate import migrate

    migrate()


def dbt(target: Path, *args: str) -> tuple[int, float]:
    t = time.perf_counter()
    r = subprocess.run(
        [
            shutil.which("dbt") or "dbt",
            *args,
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--log-path",
            str(target / "logs"),
            "--quiet",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=dict(os.environ),
    )
    return r.returncode, round(time.perf_counter() - t, 3)


def api_latency(n: int) -> dict[str, Any]:
    import httpx

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "dataops.api.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8765",
            "--log-level",
            "warning",
        ],
        env=dict(os.environ),
    )
    try:
        for _ in range(60):
            try:
                if httpx.get("http://127.0.0.1:8765/health", timeout=2).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.5)
        out: dict[str, Any] = {}
        with httpx.Client(base_url="http://127.0.0.1:8765", timeout=30) as c:
            for path in ("/health", "/overview", "/incidents?all=true", "/metrics"):
                c.get(path)  # warm-up
                lat = []
                for _ in range(n):
                    t = time.perf_counter()
                    assert c.get(path).status_code == 200
                    lat.append((time.perf_counter() - t) * 1000)
                out[path] = {
                    "n": n,
                    "p50_ms": pct(lat, 50),
                    "p95_ms": pct(lat, 95),
                    "max_ms": round(max(lat), 2),
                }
        return out
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="small", choices=["tiny", "small", "medium", "large"])
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--api-requests", type=int, default=200)
    a = ap.parse_args()

    from dataops.config import load_dotenv

    load_dotenv()
    os.environ.update(BENCH_DBS)
    os.environ["DATAOPS_DBT_TARGET_ROOT"] = str(ROOT / "dbt" / "target" / "bench")
    started = datetime.now(UTC)
    res: dict[str, Any] = {
        "profile": a.profile,
        "seed": a.seed,
        "started_at": started.isoformat(timespec="seconds"),
        "machine": {
            "python": platform.python_version(),
            "cpus": os.cpu_count(),
            "platform": platform.platform(),
            "memory_gib": round(
                os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1
            ),
        },
        "note": "single run per step unless n is given; numbers are from this machine only",
    }
    fresh_databases()

    from dataops.generator.generate import TABLE_ORDER, generate
    from dataops.generator.ledger import build_ledger
    from dataops.generator.loader import load_source

    ds, t_gen = timed(generate, a.seed, a.profile)
    loaded, t_load = timed(load_source, ds)
    total_rows = sum(ds.row_counts().values())
    res["generate_and_load"] = {"rows": total_rows, "generate_s": t_gen, "load_s": t_load}

    from dataops.cli import DEFAULT_HISTORY_START
    from dataops.ingestion.batch import ingest_range

    start = datetime.fromisoformat(DEFAULT_HISTORY_START)
    end = datetime.now(UTC) + timedelta(seconds=1)
    rs, t_ing = timed(ingest_range, list(TABLE_ORDER), start, end, step=end - start)
    inserted = sum(r.inserted_rows or 0 for r in rs)
    res["batch_extraction_full"] = {
        "tables": len(rs),
        "inserted_rows": inserted,
        "seconds": t_ing,
        "rows_per_s": round(inserted / t_ing),
        "failed": sum(r.status != "committed" for r in rs),
    }
    rs2, t_re = timed(ingest_range, list(TABLE_ORDER), start, end, step=end - start)
    res["batch_extraction_rerun_idempotent"] = {
        "inserted_rows": sum(r.inserted_rows or 0 for r in rs2),
        "duplicate_rows": sum(r.duplicate_rows or 0 for r in rs2),
        "seconds": t_re,
    }

    target = ROOT / "dbt" / "target" / "bench" / a.profile
    rc, t_full = dbt(target, "build", "--full-refresh")
    res["dbt_build_full"] = {"exit": rc, "seconds": t_full}

    from dataops.config import Role
    from dataops.db import connect

    with connect(Role.SRC_APP) as conn, conn.transaction():
        n_orders = conn.execute("select count(*) from shop.orders").fetchone()[0]
        touched = conn.execute(
            "update shop.orders set updated_at = clock_timestamp() where order_id in"
            " (select order_id from shop.orders order by order_id limit %s)",
            (max(1, n_orders // 100),),
        ).rowcount
    from dataops.ingestion.batch import ingest_table, watermark

    end2 = datetime.now(UTC) + timedelta(seconds=1)
    r3, t_inc_ing = timed(ingest_table, "orders", watermark("orders") or end, end2)
    rs3 = [r3]
    rc2, t_inc = dbt(target, "build")
    res["incremental_after_1pct_change"] = {
        "orders_touched": touched,
        "extract_s": t_inc_ing,
        "inserted_rows": sum(r.inserted_rows or 0 for r in rs3),
        "dbt_build_exit": rc2,
        "dbt_build_s": t_inc,
    }

    from dataops.reconciliation import revenue

    ledger = build_ledger(ds)
    rc_rows, t1 = timed(revenue.reconcile_row_counts, "bench", True, "batch")
    rc_mart, t2 = timed(revenue.reconcile_source_vs_mart, "bench", True)
    rc_led, t3 = timed(revenue.reconcile_ledger_vs_mart, ledger, "bench", True)
    res["reconciliation"] = {
        "row_counts": [rc_rows.status, t1],
        "source_vs_mart": [rc_mart.status, t2],
        "ledger_vs_mart": [rc_led.status, t3],
    }

    from dataops.reliability import collectors, incidents, lineage

    lin, t_lin = timed(lineage.persist, lineage.build_graph(target / "manifest.json"))
    col, t_col = timed(collectors.collect, ["dbt", "ingestion", "schema", "freshness"])
    ev, t_ev = timed(incidents.evaluate)
    res["reliability"] = {
        "lineage_persist_s": t_lin,
        "collect_s": t_col,
        "collectors": {k: v.get("status") for k, v in col.items()},
        "incident_evaluation_s": t_ev,
        "new_events_on_healthy_data": ev.get("new_events"),
    }

    from dataops.investigator import runbooks

    runbooks.index()
    qs = [
        "payments amount type changed",
        "consumer lag on many topics",
        "duplicate business keys",
        "reconciliation failed refunds",
        "stale raw tables batch failed",
        "connector failed slot",
    ]
    lat = []
    for _ in range(10):
        for q in qs:
            t = time.perf_counter()
            runbooks.search(q, k=4)
            lat.append((time.perf_counter() - t) * 1000)
    res["runbook_search"] = {"n": len(lat), "p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95)}

    res["api_latency"] = api_latency(a.api_requests)
    res["finished_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    out = ROOT / "benchmarks" / "results" / f"{a.profile}-{started:%Y%m%dT%H%M%SZ}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    print(json.dumps(res, indent=2, default=str))
    print(f"wrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
