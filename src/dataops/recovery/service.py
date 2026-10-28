"""Recovery lifecycle: propose -> approve | reject -> execute -> verify.

- Every transition is checked against the current status and appended to
  `reliability.recovery_action_events` (who, when, why, details).
- Approval needs a named human actor (not a service identity), a reason, and all
  preconditions met at approval time; they are evaluated again right before execution.
- Only one recovery action runs at a time (advisory lock).
- Verification runs the action's checks and then re-evaluates the incident's rules on fresh
  metadata. Only when no signal remains for the incident's datasets is the incident marked
  resolved and its cause 'confirmed' (and only if a corrective action, not an escalation,
  was verified).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import ValidationError

from dataops.config import Role
from dataops.db import connect
from dataops.recovery import actions as A

SERVICE_IDENTITIES = {
    "investigator",
    "incident_engine",
    "recovery_service",
    "system",
    "ai",
    "model",
}
LOCK_KEY = 0x5245434F  # 'RECO'


class RecoveryError(RuntimeError):
    pass


def _j(v: Any) -> str:
    return json.dumps(v, default=str)


def _event(
    conn: Any,
    action_id: str,
    frm: str | None,
    to: str,
    actor: str,
    reason: str | None,
    details: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        "insert into reliability.recovery_action_events (action_id, from_status, to_status, actor,"
        " reason, details) values (%s,%s,%s,%s,%s,%s)",
        (action_id, frm, to, actor, reason, _j(details or {})),
    )


def _incident_history(conn: Any, incident_id: str | None, to: str, reason: str, actor: str) -> None:
    if not incident_id:
        return
    row = conn.execute(
        "select status from reliability.incidents where incident_id=%s for update", (incident_id,)
    ).fetchone()
    if not row or row[0] == to:
        return
    conn.execute(
        "update reliability.incidents set status=%s, updated_at=clock_timestamp() where"
        " incident_id=%s",
        (to, incident_id),
    )
    conn.execute(
        "insert into reliability.incident_status_history (incident_id, from_status, to_status,"
        " reason,"
        " actor) values (%s,%s,%s,%s,%s)",
        (incident_id, row[0], to, reason, actor),
    )


def _parse(action: str, scope: dict[str, Any]) -> Any:
    spec = A.SPECS.get(action)
    if spec is None:
        raise RecoveryError(f"unknown action {action!r}; allowed: {sorted(A.SPECS)}")
    try:
        return spec.scope_model.model_validate(scope)
    except ValidationError as exc:
        raise RecoveryError(f"invalid scope for {action}: {exc.errors(include_url=False)}") from exc


def _human(actor: str) -> str:
    a = (actor or "").strip()
    if not a or a.lower() in SERVICE_IDENTITIES:
        raise RecoveryError("approval and execution need a named human actor")
    return a


def get(action_id: str) -> dict[str, Any]:
    with connect(Role.REL_READER, application_name="dataops-recovery") as conn:
        cur = conn.execute(
            "select * from reliability.recovery_actions where action_id=%s", (action_id,)
        )
        cols = [d.name for d in cur.description or []]
        row = cur.fetchone()
        if not row:
            raise KeyError(action_id)
        out = dict(zip(cols, row, strict=True))
        out["history"] = [
            dict(
                zip(
                    ("from_status", "to_status", "actor", "reason", "details", "at"), r, strict=True
                )
            )
            for r in conn.execute(
                "select from_status, to_status, actor, reason, details, at from"
                " reliability.recovery_action_events where action_id=%s order by event_seq",
                (action_id,),
            ).fetchall()
        ]
    return out


def list_actions(incident_id: str | None = None) -> list[dict[str, Any]]:
    with connect(Role.REL_READER, application_name="dataops-recovery") as conn:
        cur = conn.execute(
            "select action_id, incident_id, action_type, scope, status, proposed_by,"
            " approved_by, created_at,"
            " finished_at, verified_at from reliability.recovery_actions where (%s::uuid is"
            " null or incident_id=%s)"
            " order by created_at desc limit 200",
            (incident_id, incident_id),
        )
        cols = [d.name for d in cur.description or []]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def propose(
    action: str,
    scope: dict[str, Any],
    *,
    incident_id: str | None,
    proposed_by: str,
    investigation_id: str | None = None,
) -> dict[str, Any]:
    parsed = _parse(action, scope)
    pre = A.preconditions(action, parsed)
    dry = A.dry_run(action, parsed)
    action_id = str(uuid.uuid4())
    with connect(Role.REL_APP, application_name="dataops-recovery") as conn, conn.transaction():
        conn.execute(
            "insert into reliability.recovery_actions (action_id, incident_id, action_type,"
            " scope, status,"
            " preconditions, dry_run_output, expected_effects, proposed_by, investigation_id)"
            " values (%s,%s,%s,%s,'proposed',%s,%s,%s,%s,%s)",
            (
                action_id,
                incident_id,
                action,
                parsed.model_dump_json(),
                _j(pre),
                _j(dry),
                _j({"effect": dry.get("effect"), "changes_data": A.SPECS[action].changes_data}),
                proposed_by,
                investigation_id,
            ),
        )
        _event(conn, action_id, None, "proposed", proposed_by, None, {"preconditions": pre})
    return get(action_id)


def _transition(action_id: str, expect: set[str]) -> dict[str, Any]:
    a = get(action_id)
    if a["status"] not in expect:
        raise RecoveryError(f"action is {a['status']}; expected one of {sorted(expect)}")
    return a


def approve(action_id: str, actor: str, reason: str) -> dict[str, Any]:
    actor = _human(actor)
    if not reason or len(reason.strip()) < 5:
        raise RecoveryError("an approval needs a reason")
    a = _transition(action_id, {"proposed"})
    pre = A.preconditions(a["action_type"], _parse(a["action_type"], a["scope"]))
    blocking = [p for p in pre if p["status"] != "met"]
    with connect(Role.REL_APP, application_name="dataops-recovery") as conn, conn.transaction():
        conn.execute(
            "update reliability.recovery_actions set preconditions=%s where action_id=%s",
            (_j(pre), action_id),
        )
        if blocking:
            _event(
                conn,
                action_id,
                "proposed",
                "proposed",
                actor,
                "approval refused: preconditions not met",
                {"blocking": blocking},
            )
            raise_after = RecoveryError(f"preconditions not met: {[b['check'] for b in blocking]}")
        else:
            conn.execute(
                "update reliability.recovery_actions set status='approved', approved_by=%s,"
                " approved_at=clock_timestamp(), approval_reason=%s where action_id=%s",
                (actor, reason, action_id),
            )
            _event(conn, action_id, "proposed", "approved", actor, reason, {"preconditions": pre})
            raise_after = None
    if raise_after:
        raise raise_after
    return get(action_id)


def reject(action_id: str, actor: str, reason: str) -> dict[str, Any]:
    actor = _human(actor)
    _transition(action_id, {"proposed", "approved"})
    with connect(Role.REL_APP, application_name="dataops-recovery") as conn, conn.transaction():
        prev = conn.execute(  # type: ignore[index]
            "select status from reliability.recovery_actions where action_id=%s for update",
            (action_id,),
        ).fetchone()[0]
        conn.execute(
            "update reliability.recovery_actions set status='rejected' where action_id=%s",
            (action_id,),
        )
        _event(conn, action_id, prev, "rejected", actor, reason)
    return get(action_id)


def execute(action_id: str, actor: str) -> dict[str, Any]:
    actor = _human(actor)
    a = _transition(action_id, {"approved"})
    scope = _parse(a["action_type"], a["scope"])
    lock = connect(Role.REL_APP, application_name="dataops-recovery-lock", autocommit=True)
    try:
        if not lock.execute("select pg_try_advisory_lock(%s)", (LOCK_KEY,)).fetchone()[0]:  # type: ignore[index]
            raise RecoveryError("another recovery action is running")
        pre = A.preconditions(a["action_type"], scope)
        blocking = [p for p in pre if p["status"] != "met"]
        if blocking:
            with (
                connect(Role.REL_APP, application_name="dataops-recovery") as conn,
                conn.transaction(),
            ):
                _event(
                    conn,
                    action_id,
                    "approved",
                    "approved",
                    actor,
                    "execution refused: preconditions changed since approval",
                    {"blocking": blocking},
                )
            raise RecoveryError(
                f"preconditions not met at execution: {[b['check'] for b in blocking]}"
            )
        with connect(Role.REL_APP, application_name="dataops-recovery") as conn, conn.transaction():
            conn.execute(
                "update reliability.recovery_actions set status='running',"
                " started_at=clock_timestamp(),"
                " executed_by=%s where action_id=%s",
                (actor, action_id),
            )
            _event(conn, action_id, "approved", "running", actor, None)
            if a["incident_id"] and A.SPECS[a["action_type"]].changes_data:
                conn.execute(
                    "update reliability.incidents set"
                    " recovery_started_at=coalesce(recovery_started_at,"
                    " clock_timestamp()) where incident_id=%s",
                    (a["incident_id"],),
                )
                _incident_history(
                    conn,
                    str(a["incident_id"]),
                    "mitigating",
                    f"recovery action {a['action_type']} started",
                    actor,
                )
        try:
            out = A.execute(a["action_type"], scope, action_id)
            status = "succeeded" if out.get("ok") else "failed"
        except Exception as exc:  # noqa: BLE001 - recorded, never retried automatically
            out, status = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:1000]}, "failed"
        with connect(Role.REL_APP, application_name="dataops-recovery") as conn, conn.transaction():
            conn.execute(
                "update reliability.recovery_actions set status=%s, finished_at=clock_timestamp(),"
                " execution_output=%s where action_id=%s",
                (status, _j(out), action_id),
            )
            _event(conn, action_id, "running", status, actor, None, {"ok": out.get("ok")})
    finally:
        lock.close()
    return get(action_id)


def current_signals(incident_id: str) -> list[dict[str, Any]]:
    """Rule signals that still fire *now* for the incident's datasets (nothing is persisted)."""
    from dataops.reliability import incidents

    with connect(Role.REL_READER, application_name="dataops-recovery") as conn:
        inc = conn.execute(
            "select root_dataset_id, impacted_datasets from reliability.incidents"
            " where incident_id=%s",
            (incident_id,),
        ).fetchone()
        if not inc:
            raise KeyError(incident_id)
        datasets = {inc[0]} | set(inc[1] or [])
        datasets |= {
            r[0]
            for r in conn.execute(
                "select distinct dataset_id from reliability.incident_events where incident_id=%s",
                (incident_id,),
            ).fetchall()
        }
        since = datetime.now(UTC) - incidents.lookback()
        events = incidents._rules(conn, since)
    return [
        {"rule_id": e.rule_id, "dataset_id": e.dataset_id, "summary": e.summary}
        for e in events
        if e.dataset_id in datasets
    ]


def verify(action_id: str, actor: str, collectors: list[str] | None = None) -> dict[str, Any]:
    a = _transition(action_id, {"succeeded", "verification_failed"})
    scope = _parse(a["action_type"], a["scope"])
    checks = A.verify(a["action_type"], scope, action_id)
    from dataops.reliability import collectors as col
    from dataops.reliability import incidents

    collected = col.collect(collectors or ["dbt", "ingestion", "schema", "freshness"])
    incidents.evaluate()
    remaining = current_signals(str(a["incident_id"])) if a["incident_id"] else []
    corrective = A.SPECS[a["action_type"]].changes_data
    passed = (checks["passed"] is not False) and not remaining and corrective
    status = "verified" if passed else "verification_failed"
    verification = {
        "action_checks": checks,
        "remaining_signals": remaining,
        "collectors": {k: v.get("status") for k, v in collected.items()},
        "verified_by": actor,
        "at": datetime.now(UTC).isoformat(),
    }
    with connect(Role.REL_APP, application_name="dataops-recovery") as conn, conn.transaction():
        conn.execute(
            "update reliability.recovery_actions set status=%s, verification=%s,"
            " verified_at=case when %s then clock_timestamp() end where action_id=%s",
            (status, _j(verification), passed, action_id),
        )
        _event(
            conn,
            action_id,
            a["status"],
            status,
            actor,
            "no remaining signals" if passed else f"{len(remaining)} signal(s) remain",
            {"checks_passed": checks["passed"]},
        )
        if passed and a["incident_id"]:
            conn.execute(
                "update reliability.incidents set verified_at=clock_timestamp(),"
                " resolved_at=clock_timestamp(),"
                " root_cause_status='confirmed' where incident_id=%s",
                (a["incident_id"],),
            )
            _incident_history(
                conn,
                str(a["incident_id"]),
                "resolved",
                f"verified after {a['action_type']} (action {action_id}); cause confirmed",
                actor,
            )
    return get(action_id)


def timeline(incident_id: str) -> dict[str, Any]:
    with connect(Role.REL_READER, application_name="dataops-recovery") as conn:
        row = conn.execute(
            "select first_event_at, detected_at, investigation_started_at,"
            " recovery_started_at, verified_at,"
            " resolved_at from reliability.incidents where incident_id=%s",
            (incident_id,),
        ).fetchone()
        inv = conn.execute(  # type: ignore[index]
            "select min(started_at) from reliability.investigation_runs where incident_id=%s",
            (incident_id,),
        ).fetchone()[0]
    if not row:
        raise KeyError(incident_id)
    first, detected, _, rec, ver, res = row

    def mins(a: datetime | None, b: datetime | None) -> float | None:
        return None if not (a and b) else round((b - a) / timedelta(minutes=1), 1)

    return {
        "first_signal_at": first,
        "detected_at": detected,
        "first_investigation_at": inv,
        "recovery_started_at": rec,
        "verified_at": ver,
        "resolved_at": res,
        "detection_minutes": mins(first, detected),
        "to_recovery_minutes": mins(detected, rec),
        "to_verified_minutes": mins(detected, ver),
        "note": "all timestamps are recorded by the platform during actual runs",
    }
