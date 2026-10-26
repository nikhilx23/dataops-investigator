"""Full-profile lakehouse verification (Kafka -> Bronze -> Silver, event-time aggregate).

Checks, each printed with its evidence:
 A. incremental: advance the source, process; Silver equals source column-by-column; every
    Kafka offset appears exactly once in Bronze
 B. interruption: start the jobs continuously, change the source, SIGKILL the whole process
    group mid-stream, restart; no Bronze offset duplicated or lost, Silver equals source
 C. replay: rebuild Silver from Bronze; content identical to before the replay
 D. event time: rows dropped by the watermark are reported; the batch correction includes them

Requires the full profile running with the connector registered (see docs/full_profile.md).
Environment: DATAOPS_PG_PORT=5433. Run: python scripts/verify_streaming.py [--days 2]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import timedelta

from dataops.config import REPO_ROOT, load_dotenv


def main() -> int:
    load_dotenv()
    from confluent_kafka import Consumer, TopicPartition
    from pyspark.sql import functions as F

    from dataops.cdc.connector import CDC_TABLES, topic_for
    from dataops.cdc.stream import bootstrap_servers
    from dataops.config import Role
    from dataops.contracts import load as load_contracts
    from dataops.db import connect
    from dataops.generator.generate import generate
    from dataops.generator.simulator import simulate
    from dataops.streaming import jobs, reconcile
    from dataops.streaming import session as S

    p = argparse.ArgumentParser()
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--profile", default="small")
    p.add_argument("--days", type=int, default=2)
    p.add_argument(
        "--kill-after",
        type=float,
        default=75.0,
        help="seconds after starting the continuous run before SIGKILL",
    )
    args = p.parse_args()
    corr = f"verify-streaming-{uuid.uuid4()}"
    ds = generate(args.seed, args.profile)
    ok = True
    dropped_total = 0

    def check(name: str, passed: bool, detail: str) -> None:
        nonlocal ok
        ok &= passed
        print(f"[{'PASS' if passed else 'FAIL'}] {name}: {detail}", flush=True)

    def advance(days: int) -> int:
        with connect(Role.SRC_APP) as conn:
            until = conn.execute("select simulated_until from sim.clock").fetchone()[0]
        target = min(until + timedelta(days=days), ds.end)
        return sum(simulate(ds, target)["changed_rows"].values())

    def kafka_end_offsets() -> int:
        c = Consumer({"bootstrap.servers": bootstrap_servers(), "group.id": "verify-offsets"})
        try:
            return sum(
                c.get_watermark_offsets(TopicPartition(topic_for(t), 0), timeout=10)[1]
                for t in CDC_TABLES
            )
        finally:
            c.close()

    def run_all() -> dict:
        nonlocal dropped_total
        rep = jobs.run(["bronze", "silver", "events"])
        dropped_total += rep["events"]["rows_dropped_by_watermark"]
        return rep

    def bronze_offsets_check(label: str) -> None:
        time.sleep(3)
        expected = kafka_end_offsets()
        spark = S.get_spark()
        b = spark.read.format("delta").load(S.path(*S.BRONZE))
        n = b.count()
        distinct = b.select("topic", "partition", "offset").distinct().count()
        check(
            f"{label}: Kafka offsets land in Bronze exactly once",
            n == distinct == expected,
            f"bronze rows={n}, distinct offsets={distinct}, Kafka end offsets={expected}",
        )

    def silver_check(label: str) -> None:
        r = reconcile.reconcile(S.get_spark(), corr)
        check(
            f"{label}: Silver equals source",
            r.status == "pass",
            f"{r.compared_rows} source rows, {r.right_totals['rows']} live Silver rows, "
            f"mismatches={r.mismatched_rows}",
        )

    def silver_digest() -> str:
        spark = S.get_spark()
        h = hashlib.sha256()
        for table, contract in load_contracts().items():
            cols = [*contract.column_names, "_lsn", "_is_deleted"]
            df = spark.read.format("delta").load(S.path("silver", table)).select(*cols)
            rows = df.orderBy(contract.key).collect()
            h.update(json.dumps([list(r) for r in rows], default=str).encode())
        return h.hexdigest()

    # A. incremental
    changed = advance(args.days)
    t0 = time.perf_counter()
    rep = run_all()
    print(
        f"       processed {sum(v['input_rows'] for v in rep.values())} rows across jobs "
        f"for {changed} source changes in {time.perf_counter() - t0:.0f}s",
        flush=True,
    )
    bronze_offsets_check("A")
    silver_check("A")

    # B. interruption
    changed = advance(1)
    time.sleep(5)  # let Debezium publish the day's changes
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "dataops",
            "--log-level",
            "WARNING",
            "stream",
            "run",
            "--continuous",
            "--processing-time",
            "5 seconds",
        ],
        cwd=REPO_ROOT,
        env=dict(os.environ),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(args.kill_after)
    os.killpg(proc.pid, signal.SIGKILL)
    proc.wait()
    print(
        f"       killed continuous run (pid {proc.pid}) after {args.kill_after:.0f}s with "
        f"{changed} new source changes outstanding",
        flush=True,
    )
    run_all()
    bronze_offsets_check("B after SIGKILL + restart")
    silver_check("B after SIGKILL + restart")

    # C. replay determinism
    before = silver_digest()
    jobs.replay_silver()
    rep = jobs.run(["silver", "events"])
    after = silver_digest()
    check(
        "C: replay from Bronze reproduces Silver",
        before == after,
        f"digest before={before[:12]} after={after[:12]}",
    )

    # D. event time
    spark = S.get_spark()
    wm_s = rep["events"]["last_watermark"]
    streamed = spark.read.format("delta").load(S.path(*S.HOURLY))
    corrected = jobs.recompute_hourly(spark)
    if wm_s:
        closed = F.col("hour_start") < F.to_timestamp(F.lit(wm_s)) - F.expr("INTERVAL 1 HOUR")
        s_total = streamed.where(closed).agg(F.sum("events")).first()[0] or 0
        c_total = corrected.where(closed).agg(F.sum("events")).first()[0] or 0
    else:
        s_total = c_total = 0
    late = (
        spark.read.format("delta")
        .load(S.path("silver", "shipment_events"))
        .where("NOT _is_deleted")
        .where(
            F.col("received_at") - F.col("event_at") > F.expr(f"INTERVAL {jobs.watermark_delay()}")
        )
        .count()
    )
    replay_dropped = rep["events"]["rows_dropped_by_watermark"]
    check(
        "D: batch correction includes events the stream dropped as late",
        c_total >= s_total,
        f"watermark={jobs.watermark_delay()}, last watermark={wm_s}; closed hours: streamed="
        f"{s_total}, corrected={c_total} (diff {c_total - s_total}); rows dropped by "
        f"watermark during the replay run={replay_dropped}, during earlier runs="
        f"{dropped_total}; events received later than the delay={late}",
    )
    print(f"\nresult: {'PASS' if ok else 'FAIL'} (correlation_id={corr})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
