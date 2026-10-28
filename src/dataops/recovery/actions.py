"""Predefined recovery actions.

Each action has a typed scope (validated, bounded), live preconditions, a dry run that
changes nothing, an executor that calls existing, tested platform code with fixed arguments,
and a verification. There is no generic "run this command" action.

Never available: dropping or truncating data, deleting checkpoints or replication slots,
editing rows by hand, full resets.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from dataops.config import REPO_ROOT, Role, redact
from dataops.db import connect
from dataops.generator.generate import TABLE_ORDER

MAX_BACKFILL_DAYS = 31
ModelName = Annotated[
    str, StringConstraints(pattern=r"^(stg|int|dim|fct|mart)_[a-z0-9_]+$", max_length=60)
]
Table = Literal[tuple(TABLE_ORDER)]  # type: ignore[valid-type]


class _Scope(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RetryScope(_Scope):
    batch_id: Annotated[str, StringConstraints(pattern=r"^[0-9a-f-]{36}$")]


class BackfillScope(_Scope):
    tables: list[Table] = Field(min_length=1, max_length=len(TABLE_ORDER))  # type: ignore[valid-type]
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def _bounded(self) -> BackfillScope:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("start and end must be timezone-aware")
        if not self.start < self.end:
            raise ValueError("start must be before end")
        if self.end - self.start > timedelta(days=MAX_BACKFILL_DAYS):
            raise ValueError(f"window exceeds {MAX_BACKFILL_DAYS} days")
        return self


class RebuildScope(_Scope):
    models: list[ModelName] = Field(min_length=1, max_length=20)
    include_downstream: bool = True
    full_refresh: bool = False


class ReprocessScope(_Scope):
    tables: list[Table] = Field(min_length=1)  # type: ignore[valid-type]


class EscalateScope(_Scope):
    dataset_id: Annotated[str, StringConstraints(pattern=r"^[a-z0-9_]+(\.[a-z0-9_]+){1,4}$")]
    message: Annotated[str, StringConstraints(min_length=10, max_length=1000)]


def _check(name: str, ok: bool | None, detail: str) -> dict[str, Any]:
    return {
        "check": name,
        "status": "met" if ok else ("unknown" if ok is None else "unmet"),
        "detail": detail,
    }


# ---------------------------------------------------------------- shared checks
def no_running_batches(tables: list[str]) -> dict[str, Any]:
    with connect(Role.WH_LOADER, application_name="dataops-recovery") as conn:
        n = conn.execute(  # type: ignore[index]
            "select count(*) from raw.ingestion_batches where status='running'"
            " and source_table = any(%s)",
            (tables,),
        ).fetchone()[0]
    return _check("no_running_ingestion_batch", n == 0, f"{n} running batch(es) for {tables}")


def live_loader_sessions() -> int:
    """Loader connections currently open (the loader tags every connection)."""
    with connect(Role.WH_LOADER, application_name="dataops-recovery") as conn:
        return int(
            conn.execute(  # type: ignore[index]
                "select count(*) from pg_stat_activity where application_name='dataops-ingest'"
                " and pid <> pg_backend_pid()"
            ).fetchone()[0]
        )


def source_matches_contract(tables: list[str]) -> dict[str, Any]:
    from dataops.contracts import load as load_contracts
    from dataops.reliability.collectors import _contract_type, compare_to_contract

    contracts = load_contracts()
    with connect(Role.SRC_READER, application_name="dataops-recovery") as src:
        cols = src.execute(
            "select table_name, column_name, data_type, character_maximum_length, is_nullable"
            " from information_schema.columns where table_schema='shop' and table_name = any(%s)"
            " order by table_name, ordinal_position",
            (tables,),
        ).fetchall()
    observed: dict[str, list[dict[str, Any]]] = {}
    for t, name, dtype, clen, nullable in cols:
        observed.setdefault(t, []).append(
            {"name": name, "type": _contract_type(dtype, clen), "nullable": nullable == "YES"}
        )
    breaking = {}
    for t in tables:
        if t in contracts:
            compat, diffs = compare_to_contract(observed.get(t, []), contracts[t])
            if compat == "breaking":
                breaking[t] = [d for d in diffs if d["breaking"]]
    return _check(
        "source_schema_matches_contract",
        not breaking,
        "all tables match contract v1" if not breaking else f"breaking: {breaking}",
    )


def source_rows_in_window(table: str, start: datetime, end: datetime) -> int:
    with connect(Role.SRC_READER, application_name="dataops-recovery") as conn:
        return int(
            conn.execute(  # type: ignore[index]
                f"select count(*) from shop.{table} where updated_at >= %s and updated_at < %s",  # noqa: S608
                (start, end),
            ).fetchone()[0]
        )


# ---------------------------------------------------------------- dbt helpers
def known_models() -> set[str]:
    with connect(Role.REL_READER, application_name="dataops-recovery") as conn:
        rows = conn.execute(
            "select dataset_id from reliability.datasets where kind='model' and system='warehouse'"
        ).fetchall()
    return {r[0].rsplit(".", 1)[1] for r in rows}


def _dbt(args: list[str], target: Path) -> subprocess.CompletedProcess[str]:
    dbt = shutil.which("dbt")
    if dbt is None:
        raise RuntimeError("dbt is not installed")
    return subprocess.run(  # noqa: S603 - fixed executable, validated model names only
        [
            dbt,
            *args,
            "--project-dir",
            str(REPO_ROOT / "dbt"),
            "--profiles-dir",
            str(REPO_ROOT / "dbt"),
            "--target-path",
            str(target),
            "--log-path",
            str(target / "logs"),
        ],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        check=False,
        timeout=1800,
    )


def _selector(scope: RebuildScope) -> list[str]:
    return [m + ("+" if scope.include_downstream else "") for m in scope.models]


def _vars() -> list[str]:
    return ["--vars", "{source_path: " + os.environ.get("DATAOPS_SOURCE_PATH", "batch") + "}"]


def _tail(text: str, n: int = 25) -> list[str]:
    lines = [redact(re.sub(r"\x1b\[[0-9;]*m", "", ln)) for ln in text.splitlines() if ln.strip()]
    return lines[-n:]


# ---------------------------------------------------------------- action specs
@dataclass(frozen=True)
class Spec:
    name: str
    scope_model: type[_Scope]
    description: str
    changes_data: bool


def preconditions(action: str, scope: Any) -> list[dict[str, Any]]:
    if action == "retry_failed_batch":
        with connect(Role.WH_LOADER, application_name="dataops-recovery") as conn:
            row = conn.execute(
                "select source_table, window_start, window_end, status from raw.ingestion_batches"
                " where batch_id=%s",
                (scope.batch_id,),
            ).fetchone()
            if not row:
                return [_check("batch_exists", False, f"no batch {scope.batch_id}")]
            table, ws, we, status = row
            later = conn.execute(  # type: ignore[index]
                "select count(*) from raw.ingestion_batches where source_table=%s and"
                " status='committed'"
                " and window_start <= %s and window_end >= %s and started_at > (select started_at"
                " from raw.ingestion_batches where batch_id=%s)",
                (table, ws, we, scope.batch_id),
            ).fetchone()[0]
        live = live_loader_sessions()
        orphaned = status == "running" and live == 0
        out = [
            _check(
                "batch_failed_or_orphaned",
                status in ("failed", "abandoned") or orphaned,
                f"batch status {status}; live loader sessions {live}",
            ),
            _check(
                "not_already_reloaded",
                later == 0,
                f"{later} later committed batch(es) cover the window",
            ),
        ]
        out.append(
            _check("no_live_loader", live == 0, f"{live} live loader session(s)")
            if orphaned
            else no_running_batches([table])
        )
        try:
            with connect(Role.SRC_READER, application_name="dataops-recovery", attempts=1):
                pass
            out.append(_check("source_reachable", True, "connected as src_reader"))
        except Exception as exc:  # noqa: BLE001
            out.append(_check("source_reachable", False, redact(str(exc))[:200]))
        return out
    if action == "bounded_backfill":
        return [
            _check("window_bounded", True, f"{scope.start.isoformat()} .. {scope.end.isoformat()}"),
            _check("window_in_past", scope.end <= datetime.now(UTC), "end is not in the future"),
            no_running_batches(list(scope.tables)),
            source_matches_contract(list(scope.tables)),
        ]
    if action == "rebuild_models":
        models = known_models()
        unknown = [m for m in scope.models if m not in models]
        out = [
            _check(
                "models_are_derived_dbt_models",
                not unknown,
                f"unknown or not permitted: {unknown}" if unknown else "all are dbt models",
            )
        ]
        out.append(source_matches_contract(list(TABLE_ORDER)))
        return out
    if action == "reprocess_quarantine":
        lake = Path(os.environ.get("DATAOPS_LAKE_DIR", REPO_ROOT / "var" / "lake"))
        has_lake = (lake / "bronze").exists()
        out = [
            _check("full_profile_lakehouse_present", has_lake, f"lakehouse at {lake}"),
            source_matches_contract(list(scope.tables)),
        ]
        return out
    if action == "escalate_to_owner":
        with connect(Role.REL_READER, application_name="dataops-recovery") as conn:
            row = conn.execute(
                "select owner from reliability.datasets where dataset_id=%s", (scope.dataset_id,)
            ).fetchone()
        return [
            _check("dataset_has_owner", bool(row and row[0]), f"owner {row[0] if row else None}")
        ]
    raise ValueError(action)


def dry_run(action: str, scope: Any) -> dict[str, Any]:
    if action == "retry_failed_batch":
        with connect(Role.WH_LOADER, application_name="dataops-recovery") as conn:
            row = conn.execute(
                "select source_table, window_start, window_end from raw.ingestion_batches"
                " where batch_id=%s",
                (scope.batch_id,),
            ).fetchone()
        if not row:
            return {"error": "batch not found"}
        t, ws, we = row
        return {
            "table": t,
            "window": [ws.isoformat(), we.isoformat()],
            "source_rows_in_window": source_rows_in_window(t, ws, we),
            "effect": "re-extract this window as a new batch; content-addressed records make"
            " it idempotent",
        }
    if action == "bounded_backfill":
        return {
            "windows_per_table": max(1, (scope.end - scope.start).days),
            "source_rows_in_window": {
                t: source_rows_in_window(t, scope.start, scope.end) for t in scope.tables
            },
            "effect": "append new record versions only; existing raw records are never modified",
        }
    if action == "rebuild_models":
        target = REPO_ROOT / "dbt" / "target" / "recovery-dryrun"
        r = _dbt(
            [
                "ls",
                "--resource-type",
                "model",
                "--select",
                *_selector(scope),
                "--output",
                "name",
                *_vars(),
            ],
            target,
        )
        nodes = [
            ln.strip() for ln in r.stdout.splitlines() if re.fullmatch(r"[a-z0-9_]+", ln.strip())
        ]
        return {
            "models_to_build": nodes,
            "tests_run": "all tests attached to these models",
            "full_refresh": scope.full_refresh,
            "dbt_exit_code": r.returncode,
            "effect": "rebuild derived models only; sources and raw data are not touched",
        }
    if action == "reprocess_quarantine":
        return {
            "effect": "deterministic Silver replay from immutable Bronze; quarantined records are"
            " validated again against the current contract",
            "tables": list(scope.tables),
        }
    if action == "escalate_to_owner":
        return {"effect": "record an escalation to the dataset owner; no system change"}
    raise ValueError(action)


def execute(action: str, scope: Any, correlation_id: str) -> dict[str, Any]:
    if action == "retry_failed_batch":
        from dataops.ingestion.batch import ingest_table

        with connect(Role.WH_LOADER, application_name="dataops-recovery") as conn:
            t, ws, we, ov = conn.execute(  # type: ignore[misc]
                "select source_table, window_start, window_end, overlap_seconds from"
                " raw.ingestion_batches"
                " where batch_id=%s",
                (scope.batch_id,),
            ).fetchone()
            # An orphaned 'running' batch (its loader is gone; preconditions checked that no loader
            # session is live) is marked abandoned first. Metadata only: it published nothing.
            abandoned = conn.execute(
                "update raw.ingestion_batches set status='abandoned',"
                " finished_at=clock_timestamp(),"
                " error_message=%s where batch_id=%s and status='running'",
                (f"abandoned by recovery {correlation_id}: no live loader session", scope.batch_id),
            ).rowcount
            conn.commit()
        r = ingest_table(
            t,
            ws,
            we,
            overlap=timedelta(seconds=ov),
            run_id=f"recovery:{correlation_id}",
            correlation_id=correlation_id,
        )
        return {
            "ok": r.status == "committed",
            "abandoned_orphaned_batch": bool(abandoned),
            "batch": r.as_dict(),
        }
    if action == "bounded_backfill":
        from dataops.ingestion.batch import ingest_range

        rs = ingest_range(
            list(scope.tables),
            scope.start,
            scope.end,
            step=timedelta(days=1),
            overlap=timedelta(0),
            run_id=f"recovery:{correlation_id}",
            correlation_id=correlation_id,
        )
        failed = [r.as_dict() for r in rs if r.status != "committed"]
        return {
            "ok": not failed,
            "batches": len(rs),
            "inserted_rows": sum(r.inserted_rows or 0 for r in rs),
            "failed": failed,
        }
    if action == "rebuild_models":
        target = REPO_ROOT / "dbt" / "target" / f"recovery-{correlation_id[:8]}"
        args = ["build", "--select", *_selector(scope), *_vars()]
        if scope.full_refresh:
            args.append("--full-refresh")
        proc = _dbt(args, target)
        return {
            "ok": proc.returncode == 0,
            "dbt_exit_code": proc.returncode,
            "target": str(target.relative_to(REPO_ROOT)),
            "log_tail": _tail(proc.stdout),
        }
    if action == "reprocess_quarantine":
        from dataops.streaming.jobs import replay_silver

        return {"ok": True, "replay": replay_silver()}
    if action == "escalate_to_owner":
        return {
            "ok": True,
            "escalated_to": preconditions(action, scope)[0]["detail"].removeprefix("owner "),
            "message": scope.message,
        }
    raise ValueError(action)


def verify(action: str, scope: Any, correlation_id: str) -> dict[str, Any]:
    from dataops.reconciliation import revenue

    path = os.environ.get("DATAOPS_SOURCE_PATH", "batch")
    if action in ("retry_failed_batch", "bounded_backfill"):
        r = revenue.reconcile_row_counts(correlation_id, True, path)
        return {"passed": r.status == "pass", "checks": [r.as_dict()]}
    if action == "rebuild_models":
        r = revenue.reconcile_source_vs_mart(correlation_id, True)
        return {"passed": r.status == "pass", "checks": [r.as_dict()]}
    if action == "reprocess_quarantine":
        from dataops.cdc import reconcile as cdc_reconcile

        r = cdc_reconcile.reconcile(correlation_id, list(scope.tables))
        return {"passed": r.status == "pass", "checks": [r.as_dict()]}
    if action == "escalate_to_owner":
        return {
            "passed": None,
            "note": "an escalation has nothing to verify; the owner's fix is"
            " verified by the follow-up action",
        }
    raise ValueError(action)


SPECS: dict[str, Spec] = {
    s.name: s
    for s in [
        Spec(
            "retry_failed_batch",
            RetryScope,
            "Re-run one failed ingestion batch for the same window.",
            True,
        ),
        Spec(
            "bounded_backfill",
            BackfillScope,
            f"Re-extract explicit tables over a window of at most"
            f" {MAX_BACKFILL_DAYS} days (append-only).",
            True,
        ),
        Spec(
            "rebuild_models",
            RebuildScope,
            "Rebuild explicitly named derived dbt models (and descendants) with their tests.",
            True,
        ),
        Spec(
            "reprocess_quarantine",
            ReprocessScope,
            "Replay Silver from Bronze after the contract or source is fixed (full profile).",
            True,
        ),
        Spec(
            "escalate_to_owner", EscalateScope, "Record an escalation to the dataset owner.", False
        ),
    ]
}
