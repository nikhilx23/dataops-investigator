"""Scenario runner: baseline cycle -> inject -> pipeline cycle -> detection -> investigation ->
recovery through the controlled recovery service (a named human approves every action) ->
verification -> cleanup. Writes a report with timestamps from the actual run.

A "pipeline cycle" is what the scheduler would do: catch-up extraction of every table,
dbt build, reconciliation, metadata collection, incident evaluation.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from typing import Any

from dataops.config import REPO_ROOT, Role, redact
from dataops.db import connect
from dataops.scenarios import library as L

REPORTS = REPO_ROOT / "var" / "scenarios"


def _now() -> datetime:
    return datetime.now(UTC)


def cycle(label: str, skip_extract: list[str] | None = None) -> dict[str, Any]:
    from dataops.generator.generate import TABLE_ORDER
    from dataops.ingestion.batch import ingest_table, watermark
    from dataops.reconciliation import revenue
    from dataops.reliability import collectors, incidents

    started = _now()
    end = started + timedelta(seconds=1)
    ingest: dict[str, Any] = {}
    for t in TABLE_ORDER:
        if t in (skip_extract or []):
            ingest[t] = {"status": "skipped_by_scenario"}
            continue
        try:
            r = ingest_table(
                t, watermark(t) or end - timedelta(days=1), end, run_id=f"cycle:{label}", attempts=1
            )
            ingest[t] = {"status": r.status, "inserted": r.inserted_rows}
        except Exception as exc:  # noqa: BLE001 - a failed table is part of what we observe
            ingest[t] = {"status": "error", "error": redact(f"{type(exc).__name__}: {exc}")[:200]}
    target = REPO_ROOT / "dbt" / "target" / "scenarios" / f"{label}-{started:%H%M%S}"
    dbt = shutil.which("dbt") or "dbt"
    b = subprocess.run(  # noqa: S603 - fixed command
        [
            dbt,
            "build",
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
    )
    recon = [
        revenue.reconcile_row_counts(f"cycle:{label}", True, "batch").status,
        revenue.reconcile_source_vs_mart(f"cycle:{label}", True).status,
    ]
    col = collectors.collect(["dbt", "ingestion", "schema", "freshness"])
    ev = incidents.evaluate()
    return {
        "label": label,
        "started_at": started.isoformat(),
        "seconds": round((_now() - started).total_seconds(), 1),
        "ingest_problems": {t: v for t, v in ingest.items() if v["status"] != "committed"},
        "dbt_exit": b.returncode,
        "reconciliation": recon,
        "collectors": {k: v.get("status") for k, v in col.items()},
        "evaluation": {k: v for k, v in ev.items() if not isinstance(v, list)},
    }


def _active() -> dict[str, dict[str, Any]]:
    with connect(Role.REL_READER, application_name="dataops-scenario") as conn:
        rows = conn.execute(
            "select incident_id, title, severity, root_dataset_id, detected_at, first_event_at"
            " from reliability.incidents where status in ('open','investigating','mitigating')"
        ).fetchall()
    return {
        str(r[0]): {
            "title": r[1],
            "severity": r[2],
            "root": r[3],
            "detected_at": r[4],
            "first_event_at": r[5],
        }
        for r in rows
    }


def _resolve_scope(step: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
    scope = dict(step["scope"])
    if scope.get("window") == "from_injection":
        start, end = details["window"]
        scope.update(start=L._iso(start), end=L._iso(end))
        scope.pop("window")
    elif scope.get("window") == "last_day":
        now = _now()
        scope.update(start=(now - timedelta(days=1)).isoformat(), end=now.isoformat())
        scope.pop("window")
    bid = scope.get("batch_id", "")
    if bid.startswith("latest_failed:"):
        table = bid.split(":", 1)[1]
        with connect(Role.WH_LOADER, application_name="dataops-scenario") as conn:
            row = conn.execute(
                "select batch_id from raw.ingestion_batches where source_table=%s and status in"
                " ('failed','abandoned','running') order by started_at desc limit 1",
                (table,),
            ).fetchone()
        scope["batch_id"] = str(row[0]) if row else "00000000-0000-0000-0000-000000000000"
    return scope


def run(
    sid: str, actor: str, investigate_mode: str | None = "fixture", recover: bool = True
) -> dict[str, Any]:
    from dataops.recovery import service

    sc = L.SCENARIOS[sid]
    if sid == "streaming_interruption":
        return {"scenario": sid, **L.streaming_note()}
    report: dict[str, Any] = {
        "scenario": sid,
        "number": sc.number,
        "title": sc.title,
        "scope": sc.scope,
        "ground_truth": sc.ground_truth,
        "steps": [],
    }
    report["baseline"] = cycle(f"{sid}-baseline")
    before = _active()
    report["active_incidents_before"] = len(before)
    injected_at = _now()
    details = L.INJECT[sid]()
    report["injected_at"] = injected_at.isoformat()
    report["injection"] = json.loads(json.dumps(details, default=str))
    after_cycle = cycle(f"{sid}-after", sc.scope.get("skip_extract"))
    report["detection_cycle"] = after_cycle
    new = {k: v for k, v in _active().items() if k not in before}
    report["detected"] = [
        {
            "incident_id": k,
            **{kk: str(vv) for kk, vv in v.items()},
            "seconds_from_injection_to_detection": round(
                (v["detected_at"] - injected_at).total_seconds(), 1
            ),
        }
        for k, v in new.items()
    ]
    incident_id = max(new, key=lambda k: new[k]["severity"] == "sev1") if new else None
    if incident_id and investigate_mode:
        from dataops.investigator import engine

        inv = engine.investigate(incident_id, mode=investigate_mode)
        top = ((inv["output"] or {}).get("likely_causes") or [{}])[0]
        report["investigation"] = {
            "investigation_id": inv["investigation_id"],
            "mode": inv["mode"],
            "status": inv["status"],
            "top_cause": top.get("cause"),
            "category": top.get("category"),
            "dataset": top.get("dataset_id"),
            "note": "fixture mode is a deterministic harness, not AI"
            if inv["mode"] == "fixture"
            else None,
        }
    cleaned = False
    if recover and incident_id:
        for step in sc.recovery_plan:
            if step.get("cleanup_first"):
                report["steps"].append({"cleanup_as_owner_fix": L.CLEANUP[sid]()})
                cleaned = True
                continue
            scope = _resolve_scope(step, details)
            rec: dict[str, Any] = {"action": step["action"], "scope": scope}
            try:
                a = service.propose(
                    step["action"], scope, incident_id=incident_id, proposed_by="scenario-runner"
                )
                rec.update(action_id=str(a["action_id"]), preconditions=a["preconditions"])
                service.approve(
                    str(a["action_id"]), actor, f"scenario {sid}: documented recovery step"
                )
                done = service.execute(str(a["action_id"]), actor)
                rec["executed"] = done["status"]
                if done["status"] == "succeeded" and step["action"] != "escalate_to_owner":
                    v = service.verify(str(a["action_id"]), actor)
                    rec["verified"] = v["status"]
                    rec["remaining_signals"] = [
                        s["rule_id"] for s in v["verification"]["remaining_signals"]
                    ]
            except service.RecoveryError as exc:
                rec["refused"] = str(exc)
            report["steps"].append(rec)
        report["timeline"] = {k: str(v) for k, v in service.timeline(incident_id).items()}
    if not cleaned:
        report["cleanup"] = L.CLEANUP[sid]()
        report["post_cleanup_cycle"] = cycle(f"{sid}-cleanup")
    report["finished_at"] = _now().isoformat()
    REPORTS.mkdir(parents=True, exist_ok=True)
    path = REPORTS / f"{sid}-{injected_at:%Y%m%dT%H%M%SZ}.json"
    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    report["report_file"] = str(path.relative_to(REPO_ROOT))
    return report
