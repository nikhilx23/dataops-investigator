"""Snapshot-to-stream reconciliation: the state rebuilt from the CDC topics must equal the
source tables, column by column. Results are persisted like every other reconciliation."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from dataops.cdc import stream
from dataops.cdc.connector import CDC_TABLES
from dataops.config import Role
from dataops.db import connect
from dataops.generator.generate import PRIMARY_KEYS
from dataops.reconciliation.revenue import MAX_MISMATCH_SAMPLE, ReconciliationResult, _persist

CHECK_ID = "cdc.state.source_vs_stream"


def _norm(value: Any) -> Any:
    """Normalize Debezium JSON and psycopg values to one comparable representation."""
    if isinstance(value, datetime):
        if value.tzinfo is None:  # Spark collects UTC timestamps as naive datetimes
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str) and len(value) >= 20 and value[4] == "-" and value[10] == "T":
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(UTC).isoformat(timespec="microseconds")
        except ValueError:
            return value
    return value


def _row_digest(row: dict[str, Any]) -> str:
    canon = json.dumps({k: _norm(v) for k, v in sorted(row.items())}, sort_keys=True, default=str)
    return hashlib.sha256(canon.encode()).hexdigest()


def source_state(table: str) -> dict[Any, str]:
    pk = PRIMARY_KEYS[table]
    with connect(Role.SRC_READER, application_name="dataops-cdc-reconcile") as conn:
        cur = conn.execute(f"select * from shop.{table}")  # noqa: S608 - allowlisted table
        cols = [d.name for d in cur.description or []]
        return {
            r[cols.index(pk)]: _row_digest(dict(zip(cols, r, strict=True))) for r in cur.fetchall()
        }


def stream_state(table: str) -> tuple[dict[Any, str], stream.Materialized]:
    m = stream.materialize(table, list(stream.read_topic(table)))
    return {k: _row_digest(v) for k, v in m.rows.items()}, m


def reconcile(
    correlation_id: str, tables: list[str] | None = None, persist: bool = True
) -> ReconciliationResult:
    per_table: dict[str, dict[str, Any]] = {}
    mismatches: list[dict[str, Any]] = []
    left_total = right_total = 0
    for table in tables or CDC_TABLES:
        src = source_state(table)
        stm, m = stream_state(table)
        missing = sorted(set(src) - set(stm), key=str)
        extra = sorted(set(stm) - set(src), key=str)
        differ = sorted((k for k in set(src) & set(stm) if src[k] != stm[k]), key=str)
        left_total += len(src)
        right_total += len(stm)
        per_table[table] = {
            "source_rows": len(src),
            "stream_rows": len(stm),
            "ops": m.ops,
            "transport_duplicates": m.transport_duplicates,
        }
        for kind, keys in (
            ("missing_in_stream", missing),
            ("extra_in_stream", extra),
            ("content_differs", differ),
        ):
            for k in keys[:5]:
                mismatches.append({"key": f"{table}:{k}", "problem": kind})
    status = "pass" if not mismatches else "fail"
    result = ReconciliationResult(
        str(uuid.uuid4()),
        CHECK_ID,
        "source_vs_stream",
        status,
        compared_rows=left_total,
        mismatched_rows=len(mismatches),
        left_totals={"rows": left_total},
        right_totals={"rows": right_total},
        mismatches=mismatches[:MAX_MISMATCH_SAMPLE],
        scope={"tables": per_table},
    )
    if persist:
        fp = hashlib.sha256(f"{CHECK_ID}|{sorted(per_table)}".encode()).hexdigest()
        _persist(result, "1", "kafka.cdc.shop", fp, correlation_id)
    return result
