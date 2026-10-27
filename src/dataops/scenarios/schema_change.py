"""Failure scenario 1: incompatible source schema change (reversible, synthetic data only).

The simulated payments service "starts storing amounts in dollars": shop.payments.amount_cents
changes from bigint cents to numeric(12,2) dollars, and recent payments are touched so the
change reaches the next incremental extraction. Contract v1 says integer cents, so this is
a breaking change.

Ground truth (kept here, never given to the investigator as evidence):
  cause:   commerce_source.shop.payments.amount_cents type change bigint -> numeric(12,2)
  expected detection: breaking schema observation; dbt failures downstream of stg_payments;
                      revenue reconciliation failure
  recovery: revert the column to integer cents (this module's `revert`), re-extract the
            affected window, rebuild dbt, reconcile
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from dataops.config import Role
from dataops.db import connect

SCENARIO_ID = "breaking_schema_change.payments_amount_dollars"
GROUND_TRUTH = {
    "scenario_id": SCENARIO_ID,
    "root_dataset": "commerce_source.shop.payments",
    "cause": "payments.amount_cents changed from integer cents (bigint) to dollars (numeric(12,2))",
    "contract_violation": "amount_cents: expected int",
}


def state() -> str:
    with connect(Role.SRC_READER) as conn:
        row = conn.execute(
            "select data_type from information_schema.columns where table_schema='shop'"
            " and table_name='payments' and column_name='amount_cents'"
        ).fetchone()
    return "injected" if row and row[0] == "numeric" else "clean"


def inject(touch_since: datetime | None = None, touch_hours: int = 24) -> dict[str, Any]:
    """Apply the change. Touches payments updated in the last `touch_hours` before the
    latest payment (simulated time) so they are re-extracted with dollar values."""
    if state() == "injected":
        return {"scenario_id": SCENARIO_ID, "status": "already_injected"}
    with connect(Role.SRC_APP, application_name="dataops-scenario") as conn, conn.transaction():
        row = conn.execute("select max(updated_at) from shop.payments").fetchone()
        if not row or row[0] is None:
            raise RuntimeError("shop.payments is empty; load or simulate the source first")
        since = touch_since or row[0] - timedelta(hours=touch_hours)
        conn.execute(
            "alter table shop.payments alter column amount_cents type numeric(12,2)"
            " using round(amount_cents / 100.0, 2)"
        )
        touched = conn.execute(
            "update shop.payments set updated_at = clock_timestamp() where updated_at >= %s",
            (since,),
        ).rowcount
    return {"scenario_id": SCENARIO_ID, "status": "injected", "touched_payments": touched}


def revert() -> dict[str, Any]:
    """Restore integer cents (exact: values were rounded to cents when injected) and touch
    the same rows again so the corrected values are re-extracted."""
    if state() == "clean":
        return {"scenario_id": SCENARIO_ID, "status": "already_clean"}
    with connect(Role.SRC_APP, application_name="dataops-scenario") as conn, conn.transaction():
        conn.execute(
            "alter table shop.payments alter column amount_cents type bigint"
            " using round(amount_cents * 100)::bigint"
        )
        touched = conn.execute(
            "update shop.payments set updated_at = clock_timestamp()"
            " where updated_at >= now() - interval '1 day'"
        ).rowcount
    return {"scenario_id": SCENARIO_ID, "status": "reverted", "touched_payments": touched}
