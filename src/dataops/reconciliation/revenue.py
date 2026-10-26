"""Source-to-mart and ledger-to-mart reconciliation for daily revenue, plus source-to-raw
row-count reconciliation. Results are persisted to reliability.reconciliation_results.

Each side is computed independently:
- source: SQL over the operational database (role src_reader), implementing the rules in
  docs/business_rules.md directly, without the ingestion or dbt layers;
- mart: warehouse marts.mart_daily_revenue (role wh_reader);
- ledger: the generator's Python computation (dataops.generator.ledger).
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from dataops.config import Role, redact
from dataops.db import connect
from dataops.generator.generate import PRIMARY_KEYS, TABLE_ORDER

log = logging.getLogger(__name__)

METRICS = [
    "orders_placed",
    "orders_cancelled",
    "booked_revenue_cents",
    "captured_cents",
    "refunded_cents",
    "net_collected_cents",
]
MART_DATASET = "warehouse.marts.mart_daily_revenue"
MAX_MISMATCH_SAMPLE = 20

SOURCE_DAILY_SQL = """
with order_totals as (
    select order_id,
           sum(quantity::bigint * unit_price_cents - discount_cents) as total_cents
    from shop.order_items group by order_id
), o as (
    select (placed_at at time zone 'UTC')::date as d,
           count(*) as orders_placed,
           count(*) filter (where status = 'cancelled') as orders_cancelled,
           coalesce(sum(t.total_cents) filter (where status <> 'cancelled'), 0) as booked
    from shop.orders o left join order_totals t using (order_id) group by 1
), c as (
    select (captured_at at time zone 'UTC')::date as d, sum(amount_cents) as captured
    from shop.payments where status in ('succeeded','refunded') group by 1
), r as (
    select (refunded_at at time zone 'UTC')::date as d, sum(amount_cents) as refunded
    from shop.refunds group by 1
), days as (select d from o union select d from c union select d from r)
select days.d, coalesce(o.orders_placed,0), coalesce(o.orders_cancelled,0), coalesce(o.booked,0),
       coalesce(c.captured,0), coalesce(r.refunded,0),
       coalesce(c.captured,0) - coalesce(r.refunded,0)
from days left join o using (d) left join c using (d) left join r using (d)
order by days.d
"""

MART_DAILY_SQL = """
select revenue_date, orders_placed, orders_cancelled, booked_revenue_cents, captured_cents,
       refunded_cents, net_collected_cents
from marts.mart_daily_revenue order by revenue_date
"""


@dataclass
class ReconciliationResult:
    reconciliation_id: str
    check_id: str
    comparison: str
    status: str
    compared_rows: int
    mismatched_rows: int
    left_totals: dict[str, int]
    right_totals: dict[str, int]
    mismatches: list[dict[str, Any]] = field(default_factory=list)
    scope: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fingerprint(*sql: str) -> str:
    return hashlib.sha256("\n--\n".join(sql).encode()).hexdigest()


def _rows_to_map(rows: list[tuple[Any, ...]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for row in rows:
        key = row[0].isoformat() if isinstance(row[0], date) else str(row[0])
        out[key] = {m: int(v) for m, v in zip(METRICS, row[1:], strict=True)}
    return out


def compare_daily(
    left: dict[str, dict[str, int]], right: dict[str, dict[str, int]], metrics: list[str] = METRICS
) -> tuple[int, list[dict[str, Any]]]:
    """Compare per-date metrics; a date missing on one side counts as zeros there."""
    zero = dict.fromkeys(metrics, 0)
    mismatches: list[dict[str, Any]] = []
    keys = sorted(set(left) | set(right))
    for key in keys:
        lrow, rrow = left.get(key, zero), right.get(key, zero)
        diff = {
            m: {
                "left": lrow.get(m, 0),
                "right": rrow.get(m, 0),
                "delta": rrow.get(m, 0) - lrow.get(m, 0),
            }
            for m in metrics
            if lrow.get(m, 0) != rrow.get(m, 0)
        }
        if diff:
            mismatches.append(
                {
                    "key": key,
                    "missing_left": key not in left,
                    "missing_right": key not in right,
                    "metrics": diff,
                }
            )
    return len(keys), mismatches


def _totals(m: dict[str, dict[str, int]]) -> dict[str, int]:
    return {k: sum(r.get(k, 0) for r in m.values()) for k in METRICS}


def _persist(
    result: ReconciliationResult,
    check_version: str,
    dataset_id: str,
    fingerprint: str,
    correlation_id: str,
) -> None:
    with connect(Role.REL_APP, application_name="dataops-reconcile") as conn, conn.transaction():
        conn.execute(
            "INSERT INTO reliability.reconciliation_results (reconciliation_id, check_id,"
            " check_version, dataset_id, comparison, scope, status, compared_rows,"
            " mismatched_rows, left_totals, right_totals, mismatches, query_fingerprint,"
            " correlation_id, error_message) VALUES"
            " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                result.reconciliation_id,
                result.check_id,
                check_version,
                dataset_id,
                result.comparison,
                json.dumps(result.scope),
                result.status,
                result.compared_rows,
                result.mismatched_rows,
                json.dumps(result.left_totals),
                json.dumps(result.right_totals),
                json.dumps(result.mismatches[:MAX_MISMATCH_SAMPLE]),
                fingerprint,
                correlation_id,
                result.error_message,
            ),
        )


def _finish(
    check_id: str,
    comparison: str,
    left: dict[str, dict[str, int]],
    right: dict[str, dict[str, int]],
    fingerprint: str,
    correlation_id: str,
    scope: dict[str, Any],
    persist: bool,
    dataset_id: str = MART_DATASET,
) -> ReconciliationResult:
    compared, mismatches = compare_daily(left, right)
    result = ReconciliationResult(
        str(uuid.uuid4()),
        check_id,
        comparison,
        "pass" if not mismatches else "fail",
        compared,
        len(mismatches),
        _totals(left),
        _totals(right),
        mismatches[:MAX_MISMATCH_SAMPLE],
        scope,
    )
    if persist:
        _persist(result, "1", dataset_id, fingerprint, correlation_id)
    return result


def _error(
    check_id: str, comparison: str, exc: Exception, correlation_id: str, persist: bool
) -> ReconciliationResult:
    result = ReconciliationResult(
        str(uuid.uuid4()),
        check_id,
        comparison,
        "error",
        0,
        0,
        {},
        {},
        error_message=redact(f"{type(exc).__name__}: {exc}")[:2000],
    )
    if persist:
        _persist(result, "1", MART_DATASET, _fingerprint(), correlation_id)
    return result


def mart_daily() -> dict[str, dict[str, int]]:
    with connect(Role.WH_READER, application_name="dataops-reconcile") as conn:
        return _rows_to_map(conn.execute(MART_DAILY_SQL).fetchall())


def source_daily() -> dict[str, dict[str, int]]:
    with connect(Role.SRC_READER, application_name="dataops-reconcile") as conn:
        return _rows_to_map(conn.execute(SOURCE_DAILY_SQL).fetchall())


def reconcile_source_vs_mart(correlation_id: str, persist: bool = True) -> ReconciliationResult:
    check = "revenue.daily.source_vs_mart"
    try:
        return _finish(
            check,
            "source_vs_mart",
            source_daily(),
            mart_daily(),
            _fingerprint(SOURCE_DAILY_SQL, MART_DAILY_SQL),
            correlation_id,
            {"metrics": METRICS, "date_basis": "UTC"},
            persist,
        )
    except Exception as exc:  # recorded as an 'error' observation, never as a pass
        log.exception("reconciliation %s errored", check)
        return _error(check, "source_vs_mart", exc, correlation_id, persist)


def reconcile_ledger_vs_mart(
    ledger: dict[str, Any], correlation_id: str, persist: bool = True
) -> ReconciliationResult:
    check = "revenue.daily.ledger_vs_mart"
    try:
        scope = {
            "ledger_seed": ledger["seed"],
            "ledger_profile": ledger["profile"],
            "dataset_checksum": ledger["dataset_checksum"],
        }
        return _finish(
            check,
            "ledger_vs_mart",
            ledger["daily_revenue"],
            mart_daily(),
            _fingerprint(MART_DAILY_SQL, ledger["dataset_checksum"]),
            correlation_id,
            scope,
            persist,
        )
    except Exception as exc:
        log.exception("reconciliation %s errored", check)
        return _error(check, "ledger_vs_mart", exc, correlation_id, persist)


SOURCE_COUNT_SQL = "select count(*) from shop.{table}"
RAW_COUNT_SQL = """
select r.source_table, count(distinct r.source_pk)
from raw.source_records r
join raw.ingestion_batches b on b.batch_id = r.batch_id and b.status = 'committed'
where r.source_system = 'commerce_source'
group by r.source_table
"""


def reconcile_row_counts(correlation_id: str, persist: bool = True) -> ReconciliationResult:
    """Distinct source keys present in committed raw batches vs live source row counts.
    Equal counts are expected right after a full catch-up ingestion; source rows deleted
    after extraction make raw larger (batch cannot see deletes)."""
    check = "ingestion.row_counts.source_vs_raw"
    try:
        with connect(Role.SRC_READER, application_name="dataops-reconcile") as src:
            source = {
                t: {"rows": int(src.execute(SOURCE_COUNT_SQL.format(table=t)).fetchone()[0])}  # type: ignore[index]
                for t in TABLE_ORDER
                if t in PRIMARY_KEYS
            }
        with connect(Role.WH_READER, application_name="dataops-reconcile") as wh:
            raw = {t: {"rows": int(n)} for t, n in wh.execute(RAW_COUNT_SQL).fetchall()}
        compared, mismatches = compare_daily(source, raw, ["rows"])
        result = ReconciliationResult(
            str(uuid.uuid4()),
            check,
            "source_vs_raw",
            "pass" if not mismatches else "fail",
            compared,
            len(mismatches),
            {"rows": sum(v["rows"] for v in source.values())},
            {"rows": sum(v["rows"] for v in raw.values())},
            mismatches,
            {"tables": TABLE_ORDER},
        )
        if persist:
            _persist(
                result,
                "1",
                "warehouse.raw.source_records",
                _fingerprint(SOURCE_COUNT_SQL, RAW_COUNT_SQL),
                correlation_id,
            )
        return result
    except Exception as exc:
        log.exception("reconciliation %s errored", check)
        return _error(check, "source_vs_raw", exc, correlation_id, persist)
