"""DataOps Investigator API.

Reads use the read-only role. Writes (start an investigation, propose / approve / reject /
execute / verify a recovery action) need `Authorization: Bearer $DATAOPS_API_TOKEN` and an
`X-Actor` header naming the person; without a configured token they are disabled. The API
never accepts SQL, shell text or file paths, and recovery requests go through the same
typed, precondition-checked service as the CLI.

Run: uvicorn dataops.api.app:app --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import hmac
import os
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import CollectorRegistry, generate_latest
from pydantic import BaseModel, Field

from dataops.api.metrics import ReliabilityCollector
from dataops.config import Role, load_dotenv
from dataops.db import connect

load_dotenv()
app = FastAPI(
    title="DataOps Investigator API",
    version="0.5.0",
    description="Synthetic data only. Read-only investigator; controlled recovery.",
)
REGISTRY = CollectorRegistry()
REGISTRY.register(ReliabilityCollector())


def _clean(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _clean(x) for k, x in v.items()}
    if isinstance(v, list | tuple):
        return [_clean(x) for x in v]
    return v


def _query(sql: str, params: Any = ()) -> list[dict[str, Any]]:
    with connect(Role.REL_READER, application_name="dataops-api", statement_timeout_s=15) as conn:
        cur = conn.execute(sql, params)
        cols = [d.name for d in cur.description or []]
        return [_clean(dict(zip(cols, r, strict=True))) for r in cur.fetchall()]


def writer(
    authorization: Annotated[str | None, Header()] = None,
    x_actor: Annotated[str | None, Header()] = None,
) -> str:
    token = os.environ.get("DATAOPS_API_TOKEN")
    if not token:
        raise HTTPException(
            403, "write endpoints are disabled: DATAOPS_API_TOKEN is not configured"
        )
    given = (authorization or "").removeprefix("Bearer ").strip()
    if not hmac.compare_digest(given.encode(), token.encode()):
        raise HTTPException(401, "missing or invalid bearer token")
    if not x_actor or not x_actor.strip():
        raise HTTPException(400, "X-Actor header (the person acting) is required")
    return x_actor.strip()[:120]


Actor = Annotated[str, Depends(writer)]


# ------------------------------------------------------------------ health and metrics
@app.get("/health")
def health() -> dict[str, Any]:
    try:
        _query("select 1 as ok")
        return {"status": "ok", "reliability_db": "reachable"}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, f"reliability database unavailable: {type(exc).__name__}") from exc


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> PlainTextResponse:
    return PlainTextResponse(
        generate_latest(REGISTRY).decode(), media_type="text/plain; version=0.0.4"
    )


# ------------------------------------------------------------------ platform state
@app.get("/overview")
def overview() -> dict[str, Any]:
    return {
        "pipelines": _query("select * from reliability.v_pipeline_health order by pipeline_id"),
        "active_incidents": _query(
            "select * from reliability.v_active_incidents order by severity, detected_at"
        ),
        "reconciliation": _query(
            "select * from reliability.v_reconciliation_current order by check_id"
        ),
        "freshness_stale": _query(
            "select * from reliability.v_freshness_current where status='stale' order by dataset_id"
        ),
        "freshness_count": _query(
            "select status, count(*) as n from reliability.v_freshness_current group by 1"
        ),
        "quality_failures_7d": _query(
            "select count(*) as n from reliability.v_quality_failures_recent"
        ),
    }


@app.get("/incidents")
def incidents(all: bool = False) -> list[dict[str, Any]]:  # noqa: A002 - query parameter name
    from dataops.reliability import incidents as inc

    return [_clean(i) for i in inc.list_incidents(active_only=not all)]


@app.get("/incidents/{incident_id}")
def incident(incident_id: UUID) -> dict[str, Any]:
    from dataops.recovery import service
    from dataops.reliability import incidents as inc

    try:
        out = inc.show(str(incident_id))
    except KeyError as exc:
        raise HTTPException(404, "incident not found") from exc
    out["timeline"] = service.timeline(str(incident_id))
    out["investigations"] = _query(
        "select investigation_id, mode, model, status, started_at, latency_ms, tool_call_count,"
        " input_tokens, output_tokens from reliability.investigation_runs where incident_id=%s"
        " order by started_at desc",
        (incident_id,),
    )
    out["recovery_actions"] = [_clean(a) for a in service.list_actions(str(incident_id))]
    return _clean(out)


@app.get("/incidents/{incident_id}/impact")
def impact(incident_id: UUID) -> dict[str, Any]:
    from dataops.reliability import lineage

    rows = _query(
        "select root_dataset_id from reliability.incidents where incident_id=%s", (incident_id,)
    )
    if not rows:
        raise HTTPException(404, "incident not found")
    root = rows[0]["root_dataset_id"]
    if not root:
        return {
            "root": None,
            "note": "incident is keyed by a group (several roots); see its signals",
        }
    g, version = lineage.load_latest()
    return _clean({"lineage_version": version, **lineage.impact(g, root)})


# ------------------------------------------------------------------ investigations
class InvestigationRequest(BaseModel):
    mode: Literal["model", "fixture"] = "model"
    question: str | None = Field(None, max_length=500)


@app.post("/incidents/{incident_id}/investigations")
def start_investigation(
    incident_id: UUID, body: InvestigationRequest, actor: Actor
) -> dict[str, Any]:
    from dataops.investigator import engine
    from dataops.investigator.providers import ProviderUnavailable

    try:
        res = engine.investigate(str(incident_id), mode=body.mode, question=body.question)
    except ProviderUnavailable as exc:
        raise HTTPException(409, str(exc)) from exc
    return _clean(
        {
            k: res[k]
            for k in (
                "investigation_id",
                "mode",
                "model",
                "status",
                "output",
                "error",
                "latency_ms",
                "input_tokens",
                "output_tokens",
            )
        }
        | {
            "validation": {k: v for k, v in res["validation"].items() if k != "raw_output"},
            "tool_calls": res["tool_calls"],
            "requested_by": actor,
        }
    )


@app.get("/investigations/{investigation_id}")
def investigation(investigation_id: UUID) -> dict[str, Any]:
    from dataops.investigator import engine

    try:
        return _clean(engine.show(str(investigation_id)))
    except KeyError as exc:
        raise HTTPException(404, "investigation not found") from exc


# ------------------------------------------------------------------ recovery
class ProposeRequest(BaseModel):
    incident_id: UUID | None = None
    action: str = Field(max_length=40)
    scope: dict[str, Any]
    investigation_id: UUID | None = None


class Decision(BaseModel):
    reason: str = Field(min_length=5, max_length=500)


def _recovery(fn: Any, *args: Any) -> dict[str, Any]:
    from dataops.recovery import service

    try:
        return _clean(fn(*args))
    except service.RecoveryError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, "recovery action not found") from exc


@app.get("/recovery/catalog")
def catalog() -> dict[str, Any]:
    from dataops.recovery.actions import SPECS

    return {
        k: {
            "description": v.description,
            "changes_data": v.changes_data,
            "scope_schema": v.scope_model.model_json_schema(),
        }
        for k, v in SPECS.items()
    }


@app.get("/recovery")
def recovery_list(incident_id: Annotated[UUID | None, Query()] = None) -> list[dict[str, Any]]:
    from dataops.recovery import service

    return [_clean(a) for a in service.list_actions(str(incident_id) if incident_id else None)]


@app.get("/recovery/{action_id}")
def recovery_get(action_id: UUID) -> dict[str, Any]:
    from dataops.recovery import service

    return _recovery(service.get, str(action_id))


@app.post("/recovery")
def recovery_propose(body: ProposeRequest, actor: Actor) -> dict[str, Any]:
    from dataops.recovery import service

    def go() -> dict[str, Any]:
        return service.propose(
            body.action,
            body.scope,
            incident_id=str(body.incident_id) if body.incident_id else None,
            proposed_by=actor,
            investigation_id=str(body.investigation_id) if body.investigation_id else None,
        )

    return _recovery(go)


@app.post("/recovery/{action_id}/approve")
def recovery_approve(action_id: UUID, body: Decision, actor: Actor) -> dict[str, Any]:
    from dataops.recovery import service

    return _recovery(service.approve, str(action_id), actor, body.reason)


@app.post("/recovery/{action_id}/reject")
def recovery_reject(action_id: UUID, body: Decision, actor: Actor) -> dict[str, Any]:
    from dataops.recovery import service

    return _recovery(service.reject, str(action_id), actor, body.reason)


@app.post("/recovery/{action_id}/execute")
def recovery_execute(action_id: UUID, actor: Actor) -> dict[str, Any]:
    from dataops.recovery import service

    return _recovery(service.execute, str(action_id), actor)


@app.post("/recovery/{action_id}/verify")
def recovery_verify(action_id: UUID, actor: Actor) -> dict[str, Any]:
    from dataops.recovery import service

    return _recovery(service.verify, str(action_id), actor)
