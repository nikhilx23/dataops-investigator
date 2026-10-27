"""Deterministic incident engine.

1. Rules turn the latest collected observations into incident *events* (signals). Event
   ids are deterministic (rule + subject + evidence record), so re-evaluating the same
   observations never creates duplicate events.
2. Correlation: unattached events and events of active incidents inside the correlation
   window are clustered when their datasets are connected in the lineage graph (one is
   upstream of the other). Each cluster has one *inferred* root: the most upstream affected
   dataset. Downstream symptoms are attached to that root's incident instead of opening
   incidents of their own.
3. Deduplication: one active incident per root fingerprint (a partial unique index enforces
   it). A new signal further upstream re-roots the existing incident (recorded in history).
4. Every incident gets evidence rows that point at the exact observation records, plus the
   lineage paths from the root to each impacted dashboard.

The root is always 'inferred' here. Only the recovery service (Milestone 5) can mark a
cause 'confirmed', after a verified fix.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from dataops.config import Role
from dataops.db import connect
from dataops.reliability import lineage

log = logging.getLogger(__name__)

RULE_VERSION = "1"
SEV_ORDER = {"sev1": 1, "sev2": 2, "sev3": 3}


def correlation_window() -> timedelta:
    return timedelta(minutes=int(os.environ.get("DATAOPS_CORRELATION_WINDOW_MIN", "180")))


def lookback() -> timedelta:
    return timedelta(hours=int(os.environ.get("DATAOPS_RULE_LOOKBACK_H", "24")))


def lag_threshold_s() -> float:
    return float(os.environ.get("DATAOPS_MAX_CONSUMER_LAG_S", "900"))


@dataclass(frozen=True)
class Event:
    rule_id: str
    dataset_id: str | None
    severity: str
    failure_class: str
    summary: str
    evidence_table: str
    evidence_id: str
    observed_at: datetime
    run_id: str | None = None
    task_run_id: str | None = None
    check_id: str | None = None
    artifact_checksum: str | None = None
    # Signals sharing a group (e.g. one consumer lagging on many topics) are correlated even
    # when their datasets are not connected in the lineage graph.
    group: str | None = None

    @property
    def event_id(self) -> str:
        return f"{self.rule_id}:{self.dataset_id}:{self.evidence_table}:{self.evidence_id}"


# ------------------------------------------------------------------------- rules
def _rules(conn: Any, since: datetime) -> list[Event]:
    ev: list[Event] = []

    # R1 failed task in a failed run of a critical pipeline
    for tr, run, task, ds, msg, ended, pipe, chk in conn.execute(
        "select t.task_run_id, t.run_id, t.task_id, t.dataset_id, t.message,"
        " coalesce(t.ended_at, r.ended_at, r.collected_at), r.pipeline_id, r.artifact_checksum"
        " from reliability.task_runs t join reliability.pipeline_runs r using (run_id)"
        " join reliability.pipeline_definitions p using (pipeline_id)"
        " where t.status = 'failed' and r.status = 'failed' and p.criticality = 'critical'"
        " and coalesce(t.ended_at, r.ended_at, r.collected_at) >= %s"
        " and not exists (select 1 from reliability.task_runs t2"
        "  join reliability.pipeline_runs r2 on r2.run_id = t2.run_id"
        "  where t2.task_id = t.task_id and t2.status = 'success'"
        "  and r2.pipeline_id = r.pipeline_id"
        "  and coalesce(t2.ended_at, r2.ended_at)"
        "      > coalesce(t.ended_at, r.ended_at, r.collected_at))",
        (since,),
    ).fetchall():
        ev.append(
            Event(
                "failed_critical_task",
                ds,
                "sev2",
                "pipeline_execution",
                f"{pipe} task {task} failed: {(msg or 'no message')[:200]}",
                "task_runs",
                tr,
                ended,
                run_id=run,
                task_run_id=tr,
                artifact_checksum=chk,
            )
        )

    # (R1 and R1b ignore tasks that a later run of the same pipeline completed successfully.)
    # R1b models not rebuilt because an upstream node failed (dbt skips descendants)
    for tr, run, task, ds, ended, pipe in conn.execute(
        "select t.task_run_id, t.run_id, t.task_id, t.dataset_id,"
        " coalesce(t.ended_at, r.ended_at, r.collected_at), r.pipeline_id"
        " from reliability.task_runs t join reliability.pipeline_runs r using (run_id)"
        " join reliability.pipeline_definitions p using (pipeline_id)"
        " where t.status = 'skipped' and r.status = 'failed' and p.criticality = 'critical'"
        " and t.task_id like 'model.%%'"
        " and coalesce(t.ended_at, r.ended_at, r.collected_at) >= %s"
        " and not exists (select 1 from reliability.task_runs t2"
        "  join reliability.pipeline_runs r2 on r2.run_id = t2.run_id"
        "  where t2.task_id = t.task_id and t2.status = 'success'"
        "  and r2.pipeline_id = r.pipeline_id"
        "  and coalesce(t2.ended_at, r2.ended_at)"
        "      > coalesce(t.ended_at, r.ended_at, r.collected_at))",
        (since,),
    ).fetchall():
        ev.append(
            Event(
                "downstream_model_skipped",
                ds,
                "sev3",
                "pipeline_execution",
                f"{task} was not rebuilt in {pipe} run {run}: skipped after an upstream"
                " failure, so it still holds data from its previous successful build",
                "task_runs",
                tr,
                ended,
                run_id=run,
                task_run_id=tr,
            )
        )

    # R2 failing quality checks (latest result per check)
    for rid, check, ctype, ds, col, status, failures, msg, at, run in conn.execute(
        "select distinct on (check_id) result_id, check_id, check_type, dataset_id, column_name,"
        " status, failures, message, observed_at, run_id from reliability.quality_check_results"
        " where observed_at >= %s order by check_id, observed_at desc",
        (since,),
    ).fetchall():
        if status not in ("fail", "error"):
            continue
        rule = {
            "unique": "duplicate_business_keys",
            "relationships": "orphan_foreign_keys",
            "quarantine": "contract_quarantine",
        }.get(ctype, "quality_check_failed")
        what = f"{ctype} check on {ds}{'.' + col if col else ''} {status}"
        ev.append(
            Event(
                rule,
                ds,
                "sev2" if rule != "quality_check_failed" else "sev3",
                "data_quality",
                (
                    f"{what}: {failures} failing rows."
                    if status == "fail"
                    else f"{what}: the check itself could not run. {(msg or '')[:160]}"
                ),
                "quality_check_results",
                rid,
                at,
                run_id=run,
                check_id=check,
            )
        )

    # R3 stale datasets: latest observation per dataset and measure, and only datasets the
    # most recent freshness collection observed (an observation that the latest collection
    # did not repeat is history, not current evidence).
    for oid, ds, measure, lag, thr, at in conn.execute(
        "with latest_run as (select max(observed_at) - interval '10 minutes' as t"
        "  from reliability.freshness_observations)"
        " select distinct on (dataset_id, measure) observation_id, dataset_id, measure,"
        " lag_seconds, threshold_seconds, observed_at from reliability.freshness_observations"
        " where observed_at >= greatest(%s, (select t from latest_run))"
        " order by dataset_id, measure, observed_at desc",
        (since,),
    ).fetchall():
        row = conn.execute(
            "select status from reliability.freshness_observations where observation_id = %s",
            (oid,),
        ).fetchone()
        if row and row[0] == "stale":
            ev.append(
                Event(
                    "stale_dataset",
                    ds,
                    "sev2",
                    "source_freshness",
                    f"{ds} {measure} {float(lag) / 3600:.1f} h exceeds {float(thr) / 3600:.0f} h",
                    "freshness_observations",
                    str(oid),
                    at,
                    check_id=f"freshness:{measure}",
                    # Staleness of several tables on one warehouse input path (e.g. every
                    # warehouse.cdc_pub.* table) usually has one cause: that path stopped.
                    group=f"input_path:{ds.rsplit('.', 1)[0]}",
                )
            )

    # R4 breaking schema change (latest observation per dataset)
    for oid, ds, diffs, at, ver in conn.execute(
        "select distinct on (dataset_id) observation_id, dataset_id, differences, observed_at,"
        " contract_version from reliability.schema_observations where observed_at >= %s"
        " order by dataset_id, observed_at desc",
        (since,),
    ).fetchall():
        comp = conn.execute(
            "select compatibility from reliability.schema_observations where observation_id = %s",
            (oid,),
        ).fetchone()
        if comp and comp[0] == "breaking":
            changes = "; ".join(
                f"{d['column']} {d['change']}"
                + (f" ({d.get('expected')} -> {d.get('observed')})" if d.get("expected") else "")
                for d in diffs
                if d.get("breaking")
            )
            ev.append(
                Event(
                    "breaking_schema_change",
                    ds,
                    "sev2",
                    "schema_change",
                    f"{ds} no longer matches contract v{ver}: {changes}",
                    "schema_observations",
                    str(oid),
                    at,
                    check_id=f"contract:v{ver}",
                )
            )

    # R5 failed reconciliation (latest per check); financial checks are sev1
    for rid, check, ds, status, mism, at, err in conn.execute(
        "select distinct on (check_id) reconciliation_id, check_id, dataset_id, status,"
        " mismatched_rows, observed_at, error_message from reliability.reconciliation_results"
        " where observed_at >= %s order by check_id, observed_at desc",
        (since,),
    ).fetchall():
        if status in ("fail", "error"):
            sev = "sev1" if check.startswith(("revenue.", "inventory.")) else "sev2"
            detail = f"{mism} mismatched keys" if status == "fail" else f"error: {err}"
            ev.append(
                Event(
                    "reconciliation_failed",
                    ds,
                    sev,
                    "reconciliation",
                    f"{check} {status}: {detail}",
                    "reconciliation_results",
                    str(rid),
                    at,
                    check_id=check,
                )
            )

    # R6 consumer lag and R7 connector failure (latest per subject)
    for oid, ds, m, at in conn.execute(
        "select distinct on (dataset_id) observation_id, dataset_id, metrics, observed_at"
        " from reliability.stream_observations where kind = 'kafka_lag' and observed_at >= %s"
        " order by dataset_id, observed_at desc",
        (since,),
    ).fetchall():
        age = m.get("oldest_unprocessed_age_s")
        if age is not None and age > lag_threshold_s():
            ev.append(
                Event(
                    "consumer_lag",
                    ds,
                    "sev3",
                    "streaming",
                    f"{m['lag_messages']} unprocessed messages on {m['topic']}, oldest"
                    f" {age / 60:.0f} min old (threshold {lag_threshold_s() / 60:.0f} min)",
                    "stream_observations",
                    oid,
                    at,
                    group=f"consumer:{m.get('consumer', 'unknown')}",
                )
            )
    for oid, m, at in conn.execute(
        "select distinct on (kind) observation_id, metrics, observed_at"
        " from reliability.stream_observations where kind = 'connector_status'"
        " and observed_at >= %s order by kind, observed_at desc",
        (since,),
    ).fetchall():
        states = [m.get("connector")] + [t.get("state") for t in m.get("tasks", [])]
        if "FAILED" in states:
            ev.append(
                Event(
                    "connector_failed",
                    "kafka.cdc.shop",
                    "sev2",
                    "streaming",
                    f"Debezium connector states {states}",
                    "stream_observations",
                    oid,
                    at,
                    # A failed connector stops the whole CDC input path: correlate it with the
                    # staleness of the published tables it feeds.
                    group="input_path:warehouse.cdc_pub",
                )
            )
    return ev


# -------------------------------------------------------------------- correlation
def _clusters(g: lineage.Graph, events: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Union events whose datasets are lineage-connected (ancestor/descendant)."""
    parent = list(range(len(events)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    reach: dict[str, set[str]] = {}
    for e in events:
        ds = e["dataset_id"]
        if ds and ds in g.nodes and ds not in reach:
            reach[ds] = set(g.downstream(ds)) | set(g.upstream(ds))
    for i, a in enumerate(events):
        for j in range(i + 1, len(events)):
            b = events[j]
            da, db = a["dataset_id"], b["dataset_id"]
            ga = (a["evidence_ref"] or {}).get("group") if "evidence_ref" in a else None
            gb = (b["evidence_ref"] or {}).get("group") if "evidence_ref" in b else None
            if (da and db and (da == db or db in reach.get(da, set()))) or (ga and ga == gb):
                parent[find(i)] = find(j)
    groups: dict[int, list[dict[str, Any]]] = {}
    for i, e in enumerate(events):
        groups.setdefault(find(i), []).append(e)
    return list(groups.values())


def _title(
    g: lineage.Graph, root: str | None, events: list[dict[str, Any]], dashboards: list[str]
) -> str:
    classes = sorted({e["failure_class"].replace("_", " ") for e in events})
    where = g.nodes.get(root or "", {}).get("display_name") or root or "unknown dataset"
    tail = f", {len(dashboards)} dashboard(s) affected" if dashboards else ""
    return f"{' + '.join(classes).capitalize()} at {where}{tail}"


def evaluate(now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    g, version = lineage.load_latest(Role.REL_APP)
    created, updated, new_events = [], [], 0
    with connect(Role.REL_APP, application_name="dataops-incidents") as conn, conn.transaction():
        conn.execute("select pg_advisory_xact_lock(7311)")  # one evaluator at a time
        for sig in _rules(conn, now - lookback()):
            cur = conn.execute(
                "insert into reliability.incident_events (event_id, rule_id, rule_version,"
                " dataset_id, severity, failure_class, summary, evidence_ref, observed_at)"
                " values (%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (event_id) do nothing",
                (
                    sig.event_id,
                    sig.rule_id,
                    RULE_VERSION,
                    sig.dataset_id,
                    sig.severity,
                    sig.failure_class,
                    sig.summary,
                    json.dumps(
                        {
                            "table": sig.evidence_table,
                            "id": sig.evidence_id,
                            "run_id": sig.run_id,
                            "task_run_id": sig.task_run_id,
                            "check_id": sig.check_id,
                            "artifact_checksum": sig.artifact_checksum,
                            "group": sig.group,
                        }
                    ),
                    sig.observed_at,
                ),
            )
            new_events += cur.rowcount
        cols = [
            "event_id",
            "rule_id",
            "dataset_id",
            "severity",
            "failure_class",
            "summary",
            "evidence_ref",
            "observed_at",
            "incident_id",
        ]
        window_events = [
            dict(zip(cols, r, strict=True))
            for r in conn.execute(
                "select e.event_id, e.rule_id, e.dataset_id, e.severity, e.failure_class,"
                " e.summary,"
                " e.evidence_ref, e.observed_at, e.incident_id from reliability.incident_events e"
                " left join reliability.incidents i using (incident_id)"
                " where (e.incident_id is null and e.observed_at >= %s)"
                "    or i.status in ('open','investigating','mitigating')",
                (now - correlation_window() - lookback(),),
            ).fetchall()
        ]
        for cluster in _clusters(g, window_events):
            if all(e["incident_id"] for e in cluster):
                continue  # nothing new in this cluster
            datasets = [e["dataset_id"] for e in cluster if e["dataset_id"]]
            roots = lineage.most_upstream(g, datasets) or sorted(set(datasets))
            groups: set[str] = {
                grp for e in cluster if (grp := (e["evidence_ref"] or {}).get("group"))
            }
            # Several unrelated roots joined only by a shared group (e.g. one lagging consumer):
            # no single dataset is the root; the incident is keyed by the group instead.
            group_key = sorted(groups)[0] if len(roots) > 1 and groups else None
            root: str | None = None if group_key else (roots[0] if roots else None)
            known = [r for r in ([root] if root else roots) if r in g.nodes]
            dashboards = sorted({d for r in known for d in lineage.impact(g, r)["dashboards"]})
            impacted = sorted({d for r in known for d in g.downstream(r)})
            sev = min((e["severity"] for e in cluster), key=SEV_ORDER.__getitem__)
            critical_dash = [d for d in dashboards if g.nodes[d].get("criticality") == "critical"]
            if critical_dash and sev == "sev2":
                sev = "sev1"
            existing = {e["incident_id"] for e in cluster if e["incident_id"]}
            fingerprint = f"group:{group_key}" if group_key else f"root:{root}"
            if not existing:
                row = conn.execute(
                    "select incident_id from reliability.incidents where fingerprint = %s and"
                    " status in ('open','investigating','mitigating')",
                    (fingerprint,),
                ).fetchone()
                if row:
                    existing = {row[0]}
            first = min(e["observed_at"] for e in cluster)
            last = max(e["observed_at"] for e in cluster)
            rules_seen = ", ".join(sorted({e["rule_id"] for e in cluster}))
            if group_key:
                title = _title(g, None, cluster, dashboards).replace(
                    "unknown dataset", f"{group_key} ({len(roots)} datasets)"
                )
                summary = (
                    f"{len(cluster)} correlated signal(s) ({rules_seen}) share {group_key} across"
                    f" {len(roots)} unrelated datasets ({', '.join(roots[:5])}); no single"
                    " upstream dataset explains them. Inferred grouping; not confirmed."
                )
            else:
                title = _title(g, root, cluster, dashboards)
                summary = (
                    f"Inferred root: {root} — the most upstream dataset among "
                    f"{len(cluster)} correlated signal(s) ({rules_seen}). "
                    f"Inferred from lineage; not confirmed."
                )
            if existing:
                incident_id = sorted(existing, key=str)[0]
                old_row = conn.execute(
                    "select root_dataset_id, severity from reliability.incidents"
                    " where incident_id = %s",
                    (incident_id,),
                ).fetchone()
                old = old_row or (None, sev)
                conn.execute(
                    "update reliability.incidents set root_dataset_id=%s, fingerprint=%s,"
                    " title=%s, severity=%s, root_cause_summary=%s, impacted_datasets=%s,"
                    " lineage_version=%s, last_event_at=greatest(last_event_at, %s),"
                    " updated_at=clock_timestamp() where incident_id=%s",
                    (
                        root,
                        fingerprint,
                        title,
                        min(sev, old[1], key=SEV_ORDER.__getitem__),
                        summary,
                        json.dumps(impacted),
                        version,
                        last,
                        incident_id,
                    ),
                )
                if old[0] != root:
                    conn.execute(
                        "insert into reliability.incident_status_history (incident_id,"
                        " from_status, to_status, reason, actor) select incident_id, status,"
                        " status, %s, 'incident_engine' from reliability.incidents"
                        " where incident_id=%s",
                        (
                            f"root re-inferred: {old[0]} -> {root} (new upstream signal)",
                            incident_id,
                        ),
                    )
                updated.append(str(incident_id))
            else:
                incident_id = uuid.uuid4()
                conn.execute(
                    "insert into reliability.incidents (incident_id, fingerprint, title, severity,"
                    " status, root_dataset_id, root_cause_status, root_cause_summary,"
                    " impacted_datasets, lineage_version, detected_at, first_event_at,"
                    " last_event_at) values (%s,%s,%s,%s,'open',%s,'inferred',%s,%s,%s,"
                    " clock_timestamp(),%s,%s)",
                    (
                        incident_id,
                        fingerprint,
                        title,
                        sev,
                        root,
                        summary,
                        json.dumps(impacted),
                        version,
                        first,
                        last,
                    ),
                )
                conn.execute(
                    "insert into reliability.incident_status_history (incident_id, from_status,"
                    " to_status, reason, actor) values (%s, null, 'open', %s, 'incident_engine')",
                    (
                        incident_id,
                        f"{len(cluster)} correlated signal(s); rules "
                        f"{sorted({e['rule_id'] for e in cluster})}",
                    ),
                )
                created.append(str(incident_id))
            for e in cluster:
                conn.execute(
                    "update reliability.incident_events set incident_id=%s where event_id=%s",
                    (incident_id, e["event_id"]),
                )
                ref = e["evidence_ref"]
                conn.execute(
                    "insert into reliability.incident_evidence (evidence_id, incident_id,"
                    " evidence_type, source_table, source_id, dataset_id, run_id, task_run_id,"
                    " check_id, artifact_checksum, observed_at, summary) values"
                    " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    " on conflict (incident_id, source_table, source_id) do nothing",
                    (
                        f"ev:{ref['table']}:{ref['id']}",
                        incident_id,
                        {
                            "task_runs": "task_run",
                            "quality_check_results": "quality_check",
                            "freshness_observations": "freshness",
                            "schema_observations": "schema_observation",
                            "reconciliation_results": "reconciliation",
                            "stream_observations": "stream",
                        }[ref["table"]],
                        ref["table"],
                        ref["id"],
                        e["dataset_id"],
                        ref.get("run_id"),
                        ref.get("task_run_id"),
                        ref.get("check_id"),
                        ref.get("artifact_checksum"),
                        e["observed_at"],
                        e["summary"][:500],
                    ),
                )
            if root and version:
                for dash in dashboards:
                    path = g.path(root, dash) or []
                    conn.execute(
                        "insert into reliability.incident_evidence (evidence_id, incident_id,"
                        " evidence_type, source_table, source_id, dataset_id, observed_at,"
                        " summary) values (%s,%s,'lineage_path','lineage_edges',%s,%s,"
                        " clock_timestamp(),%s) on conflict (incident_id, source_table,"
                        " source_id) do nothing",
                        (
                            f"ev:lineage:{version}:{root}->{dash}",
                            incident_id,
                            f"{version}:{root}->{dash}",
                            dash,
                            " -> ".join(path) + f" ({len(path) - 1} hops, lineage {version[:8]})",
                        ),
                    )
    return {
        "new_events": new_events,
        "created": created,
        "updated": updated,
        "lineage_version": version,
    }


def list_incidents(active_only: bool = True) -> list[dict[str, Any]]:
    with connect(Role.REL_READER, application_name="dataops-incidents") as conn:
        cur = conn.execute(
            "select incident_id, title, severity, status, root_dataset_id, root_cause_status,"
            " detected_at, first_event_at, last_event_at,"
            " (select count(*) from reliability.incident_events e"
            "  where e.incident_id = i.incident_id) as events,"
            " (select count(*) from reliability.incident_evidence v"
            "  where v.incident_id = i.incident_id) as evidence"
            " from reliability.incidents i"
            " where (not %s or status in ('open','investigating','mitigating'))"
            " order by detected_at desc limit 100",
            (active_only,),
        )
        cols = [d.name for d in cur.description or []]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def show(incident_id: str) -> dict[str, Any]:
    with connect(Role.REL_READER, application_name="dataops-incidents") as conn:
        cur = conn.execute(
            "select * from reliability.incidents where incident_id = %s", (incident_id,)
        )
        cols = [d.name for d in cur.description or []]
        row = cur.fetchone()
        if not row:
            raise KeyError(incident_id)
        inc = dict(zip(cols, row, strict=True))
        inc["events"] = [
            dict(
                zip(("rule_id", "dataset_id", "severity", "summary", "observed_at"), r, strict=True)
            )
            for r in conn.execute(
                "select rule_id, dataset_id, severity, summary, observed_at from"
                " reliability.incident_events where incident_id=%s order by observed_at",
                (incident_id,),
            ).fetchall()
        ]
        inc["evidence"] = [
            dict(zip(("evidence_id", "evidence_type", "dataset_id", "summary"), r, strict=True))
            for r in conn.execute(
                "select evidence_id, evidence_type, dataset_id, summary from"
                " reliability.incident_evidence where incident_id=%s order by evidence_type,"
                " evidence_id",
                (incident_id,),
            ).fetchall()
        ]
        inc["history"] = [
            dict(zip(("from_status", "to_status", "reason", "actor", "changed_at"), r, strict=True))
            for r in conn.execute(
                "select from_status, to_status, reason, actor, changed_at from"
                " reliability.incident_status_history where incident_id=%s order by changed_at",
                (incident_id,),
            ).fetchall()
        ]
    return inc


def close(incident_id: str, reason: str, actor: str) -> dict[str, Any]:
    """Close an incident by hand with a recorded reason (e.g. a false positive). Recovery
    verification (Milestone 5) is the normal path to 'resolved'."""
    if not reason.strip():
        raise ValueError("a reason is required to close an incident")
    with connect(Role.REL_APP, application_name="dataops-incidents") as conn, conn.transaction():
        row = conn.execute(
            "select status from reliability.incidents where incident_id = %s for update",
            (incident_id,),
        ).fetchone()
        if not row:
            raise KeyError(incident_id)
        if row[0] == "closed":
            return {"incident_id": incident_id, "status": "closed", "changed": False}
        conn.execute(
            "update reliability.incidents set status = 'closed',"
            " updated_at = clock_timestamp() where incident_id = %s",
            (incident_id,),
        )
        conn.execute(
            "insert into reliability.incident_status_history (incident_id, from_status,"
            " to_status, reason, actor) values (%s, %s, 'closed', %s, %s)",
            (incident_id, row[0], reason[:1000], actor[:100]),
        )
    return {"incident_id": incident_id, "status": "closed", "changed": True}
