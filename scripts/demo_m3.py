"""Milestone 3 demonstration: detection, correlation and impact of a real breaking change.

1. collect metadata from a healthy pipeline and evaluate: expect no incidents
2. inject failure scenario 1 (payments amounts switch to dollars, a breaking change)
3. run the normal pipeline: incremental extraction, dbt build, reconciliation
4. collect metadata and evaluate: print the incident, its signals, impact and evidence
5. revert the source change (the incident stays open: recovery and verification are
   Milestone 5)

Uses the lightweight databases from .env and the dbt target under dbt/target/demo_m3.
Synthetic data only; the injection is reversible.
"""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta

from dataops.config import REPO_ROOT, load_dotenv


def main() -> int:
    load_dotenv()
    import os

    target = REPO_ROOT / "dbt" / "target" / "demo_m3"
    os.environ["DATAOPS_DBT_TARGET_ROOT"] = str(target)
    from dataops.generator.generate import TABLE_ORDER
    from dataops.ingestion.batch import ingest_table, watermark
    from dataops.reconciliation import revenue
    from dataops.reliability import collectors, incidents, lineage
    from dataops.scenarios import schema_change

    def dbt_build() -> int:
        return subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                str(REPO_ROOT / "dbt"),
                "--profiles-dir",
                str(REPO_ROOT / "dbt"),
                "--target-path",
                str(target),
                "--quiet",
            ],
            capture_output=True,
            text=True,
            check=False,
        ).returncode

    def catch_up() -> int:
        now = datetime.now(UTC) + timedelta(seconds=1)
        return sum(
            ingest_table(t, watermark(t) or now - timedelta(days=1), now).inserted_rows
            for t in TABLE_ORDER
        )

    print("== 1. healthy pipeline")
    if schema_change.state() == "injected":
        schema_change.revert()
    catch_up()
    print(f"   dbt build exit={dbt_build()}")
    revenue.reconcile_source_vs_mart("demo-m3-healthy")
    col = collectors.collect(["dbt", "ingestion", "schema", "freshness"])
    print(f"   collectors: { {k: v['status'] for k, v in col.items()} }")
    before = incidents.evaluate()
    print(f"   evaluate: new_events={before['new_events']} created={len(before['created'])}")

    print("== 2. inject: payments.amount_cents switches to dollars (numeric)")
    injected_at = datetime.now(UTC)
    print(f"   {schema_change.inject()}")

    print("== 3. normal pipeline run")
    print(f"   extraction inserted {catch_up()} new row versions")
    print(f"   dbt build exit={dbt_build()}")
    r = revenue.reconcile_source_vs_mart("demo-m3-broken")
    print(f"   reconciliation {r.check_id}: {r.status} ({r.mismatched_rows} mismatched dates)")

    print("== 4. collect + evaluate")
    t0 = time.perf_counter()
    collectors.collect(["dbt", "ingestion", "schema", "freshness"])
    result = incidents.evaluate()
    print(
        f"   collect+evaluate took {time.perf_counter() - t0:.1f}s;"
        f" new_events={result['new_events']} created={len(result['created'])}"
        f" updated={len(result['updated'])}"
    )
    for iid in result["created"] + result["updated"]:
        inc = incidents.show(iid)
        print(f"\n   INCIDENT {iid}\n   {inc['severity']} {inc['status']}: {inc['title']}")
        print(f"   inferred root: {inc['root_dataset_id']} ({inc['root_cause_status']})")
        if iid in result["created"]:
            latency = (inc["detected_at"] - injected_at).total_seconds()
            print(
                f"   detection latency from injection: {latency:.1f}s"
                f" (includes the pipeline run in step 3)"
            )
        else:
            print(
                f"   new signals attached to the incident already open since"
                f" {inc['detected_at']:%Y-%m-%d %H:%M:%S} UTC (deduplicated, not re-opened)"
            )
        print(f"   signals ({len(inc['events'])}):")
        for e in inc["events"]:
            print(f"     - [{e['rule_id']}] {e['dataset_id']}: {e['summary'][:110]}")
        g, _ = lineage.load_latest()
        imp = lineage.impact(g, inc["root_dataset_id"])
        print(
            f"   impact: {imp['downstream_count']} downstream datasets,"
            f" dashboards {imp['dashboards']}"
        )
        print(f"   evidence records ({len(inc['evidence'])}):")
        for ev in inc["evidence"][:12]:
            print(f"     - {ev['evidence_id'][:72]}  [{ev['evidence_type']}]")
    again = incidents.evaluate()
    print(f"\n   re-evaluate: new_events={again['new_events']} created={len(again['created'])}")

    print("== 5. revert the source change (incident stays open until verified recovery)")
    print(f"   {schema_change.revert()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
