"""Explicit, destructive reset of the SYNTHETIC demo environment.

Used to restart a demonstration from an empty source and warehouse (for example after
switching seeds, which batch extraction cannot reconcile because it cannot see deletes).
Never runs implicitly: the CLI requires --confirm-synthetic-reset. Reliability metadata
(reconciliation history, incidents, audit trails) is kept unless asked otherwise.
"""

from __future__ import annotations

import logging

from dataops.config import Role
from dataops.db import session
from dataops.generator.generate import TABLE_ORDER

log = logging.getLogger(__name__)

DBT_SCHEMAS = ("staging", "intermediate", "marts")


def reset_synthetic_environment(include_reliability: bool = False) -> dict[str, list[str]]:
    done: dict[str, list[str]] = {}
    with session(Role.SRC_APP, application_name="dataops-reset") as conn, conn.transaction():
        conn.execute(
            "TRUNCATE "
            + ", ".join(f"shop.{t}" for t in reversed(TABLE_ORDER))
            + " RESTART IDENTITY"
        )
        conn.execute("DELETE FROM sim.clock")
        done["source"] = [f"shop.{t}" for t in TABLE_ORDER] + ["sim.clock"]
    with session(Role.WH_LOADER, application_name="dataops-reset") as conn, conn.transaction():
        conn.execute("TRUNCATE raw.source_records, raw.ingestion_watermarks, raw.ingestion_batches")
        done["warehouse_raw"] = [
            "raw.source_records",
            "raw.ingestion_watermarks",
            "raw.ingestion_batches",
        ]
    with session(Role.WH_TRANSFORM, application_name="dataops-reset") as conn, conn.transaction():
        for schema in DBT_SCHEMAS:
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        done["warehouse_dbt"] = list(DBT_SCHEMAS)
    if include_reliability:
        with session(Role.REL_APP, application_name="dataops-reset") as conn, conn.transaction():
            conn.execute("TRUNCATE reliability.reconciliation_results")
            done["reliability"] = ["reliability.reconciliation_results"]
    log.warning("synthetic environment reset: %s", done)
    return done
