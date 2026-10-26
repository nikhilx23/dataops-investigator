"""Order-independent content fingerprints of warehouse models.

Used to prove that incremental models converge to the same result as a full refresh:
fingerprint after a sequence of incremental runs, rebuild with --full-refresh, fingerprint
again, compare. Only allowlisted models can be fingerprinted (identifiers are never taken
from user input).
"""

from __future__ import annotations

from dataclasses import dataclass

from dataops.config import Role
from dataops.db import connect

# model -> ordering key
FINGERPRINTABLE: dict[str, str] = {
    "marts.fct_order_item": "order_item_id",
    "marts.fct_payment_transaction": "transaction_id",
    "marts.fct_inventory_movement": "movement_id",
    "marts.fct_shipment_event": "shipment_event_id",
    "marts.dim_product_history": "product_version_key",
    "marts.mart_daily_revenue": "revenue_date",
    "marts.mart_inventory_position": "product_id, warehouse_id",
    "marts.mart_payment_reconciliation": "order_id",
    "marts.mart_order_fulfillment": "order_id",
}


@dataclass(frozen=True)
class Fingerprint:
    model: str
    rows: int
    md5: str


def fingerprint(model: str) -> Fingerprint:
    if model not in FINGERPRINTABLE:
        raise ValueError(f"{model!r} is not fingerprintable; allowed: {sorted(FINGERPRINTABLE)}")
    key = FINGERPRINTABLE[model]
    sql = (
        f"select count(*), coalesce(md5(string_agg(t::text, '|' order by {key})), '') "  # noqa: S608
        f"from {model} t"
    )
    with connect(Role.WH_READER, application_name="dataops-fingerprint") as conn:
        rows, digest = conn.execute(sql).fetchone()  # type: ignore[misc]
    return Fingerprint(model, int(rows), str(digest))


def fingerprint_all(models: list[str] | None = None) -> dict[str, Fingerprint]:
    return {m: fingerprint(m) for m in (models or list(FINGERPRINTABLE))}
