"""Silver-to-source reconciliation: every non-deleted Silver row must equal the source row,
column by column, and every source row must be present in Silver."""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

from dataops.cdc.reconcile import _row_digest, source_state
from dataops.contracts import load as load_contracts
from dataops.reconciliation.revenue import MAX_MISMATCH_SAMPLE, ReconciliationResult, _persist
from dataops.streaming import session as S

CHECK_ID = "lakehouse.silver.source_vs_silver"


def silver_state(spark: Any, table: str) -> tuple[dict[Any, str], dict[str, int]]:
    contract = load_contracts()[table]
    df = spark.read.format("delta").load(S.path("silver", table))
    stats = {"rows": df.count(), "deleted": df.where("_is_deleted").count()}
    live = df.where("not _is_deleted").select(*contract.column_names).collect()
    return {r[contract.key]: _row_digest(r.asDict()) for r in live}, stats


def reconcile(spark: Any, correlation_id: str, persist: bool = True) -> ReconciliationResult:
    per_table: dict[str, Any] = {}
    mismatches: list[dict[str, Any]] = []
    left = right = 0
    for table in load_contracts():
        src = source_state(table)
        sil, stats = silver_state(spark, table)
        left += len(src)
        right += len(sil)
        per_table[table] = {"source_rows": len(src), "silver_live_rows": len(sil), **stats}
        for kind, keys in (
            ("missing_in_silver", set(src) - set(sil)),
            ("extra_in_silver", set(sil) - set(src)),
            ("content_differs", {k for k in set(src) & set(sil) if src[k] != sil[k]}),
        ):
            for k in sorted(keys, key=str)[:5]:
                mismatches.append({"key": f"{table}:{k}", "problem": kind})
    result = ReconciliationResult(
        str(uuid.uuid4()),
        CHECK_ID,
        "source_vs_silver",
        "pass" if not mismatches else "fail",
        left,
        len(mismatches),
        {"rows": left},
        {"rows": right},
        mismatches[:MAX_MISMATCH_SAMPLE],
        {"tables": per_table},
    )
    if persist:
        _persist(
            result,
            "1",
            "lakehouse.silver",
            hashlib.sha256(CHECK_ID.encode()).hexdigest(),
            correlation_id,
        )
    return result
