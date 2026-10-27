"""Evidence identifiers and their resolution against stored records.

Formats
  ev:<table>:<primary key>                 a row in an allowlisted reliability table
  ev:lineage:<version>:<from>-><to>        a dependency path in a stored lineage version
  doc:<chunk id>@<runbook version>         an indexed runbook chunk

A citation is valid only if it resolves here. Resolution uses fixed SQL per table; the
identifier is always a bound parameter.
"""

from __future__ import annotations

import re
from typing import Any

from dataops.config import Role
from dataops.db import connect

KEYS: dict[str, str] = {
    "incidents": "incident_id",
    "incident_events": "event_id",
    "pipeline_runs": "run_id",
    "task_runs": "task_run_id",
    "quality_check_results": "result_id",
    "schema_observations": "observation_id",
    "freshness_observations": "observation_id",
    "reconciliation_results": "reconciliation_id",
    "stream_observations": "observation_id",
    "lineage_observations": "lineage_version",
    "datasets": "dataset_id",
}
_EV = re.compile(r"^ev:([a-z_]+):(.+)$")
_LINEAGE = re.compile(r"^ev:lineage:([0-9a-f-]{36}):(.+?)->(.+)$")
_DOC = re.compile(r"^doc:(RB-\d{3}#[a-z0-9-]+)@([0-9A-Za-z.]+)$")

PATH_SQL = (
    "with recursive walk(node, depth) as ("
    " select %(src)s::text, 0"
    " union select e.downstream_id, w.depth + 1 from walk w join reliability.lineage_edges e"
    "  on e.upstream_id = w.node and e.lineage_version = %(ver)s::uuid where w.depth < 25)"
    " select exists (select 1 from walk where node = %(dst)s)"
)


def ev(table: str, key: Any) -> str:
    if table not in KEYS:
        raise ValueError(table)
    return f"ev:{table}:{key}"


def resolve(ids: list[str], conn: Any = None) -> dict[str, bool]:
    """Map each identifier to whether it resolves to a stored record."""
    own = conn is None
    c = conn or connect(
        Role.REL_READER, application_name="dataops-evidence", statement_timeout_s=10
    )
    try:
        out: dict[str, bool] = {}
        for i in dict.fromkeys(ids):
            out[i] = _resolve_one(c, i)
        return out
    finally:
        if own:
            c.close()


def _resolve_one(conn: Any, ident: str) -> bool:
    if not isinstance(ident, str) or len(ident) > 400:
        return False
    if m := _LINEAGE.match(ident):
        ver, src, dst = m.groups()
        row = conn.execute(PATH_SQL, {"src": src, "dst": dst, "ver": ver}).fetchone()
        return bool(row and row[0])
    if m := _DOC.match(ident):
        row = conn.execute(
            "select 1 from reliability.runbook_chunks where chunk_id=%s and doc_version=%s",
            m.groups(),
        ).fetchone()
        return row is not None
    if m := _EV.match(ident):
        table, key = m.groups()
        col = KEYS.get(table)
        if col is None:
            return False
        row = conn.execute(
            f"select 1 from reliability.{table} where {col}::text = %s",  # noqa: S608 - allowlisted
            (key,),
        ).fetchone()
        return row is not None
    return False
