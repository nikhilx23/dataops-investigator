"""Load a generated dataset into the operational source database (role src_app)."""

from __future__ import annotations

import logging

from dataops.config import Role
from dataops.db import session
from dataops.generator.generate import TABLE_ORDER, Dataset

log = logging.getLogger(__name__)


class SourceNotEmptyError(RuntimeError):
    pass


def load_source(ds: Dataset, *, replace: bool = False) -> dict[str, int]:
    """Insert every table with COPY in one transaction.

    The source is synthetic, but replacing it is still destructive: it requires replace=True
    and truncates only the shop schema tables this project created.
    """
    counts: dict[str, int] = {}
    with (
        session(
            Role.SRC_APP, application_name="dataops-generator", statement_timeout_s=600
        ) as conn,
        conn.transaction(),
    ):
        existing = conn.execute("SELECT count(*) FROM shop.orders").fetchone()
        if existing and existing[0] and not replace:
            raise SourceNotEmptyError(
                f"shop.orders already holds {existing[0]} rows; "
                "pass --replace to reload the synthetic source"
            )
        if replace:
            conn.execute("TRUNCATE " + ", ".join(f"shop.{t}" for t in reversed(TABLE_ORDER)))
        for table in TABLE_ORDER:
            rows = ds.tables[table]
            if not rows:
                counts[table] = 0
                continue
            cols = list(rows[0].keys())
            with conn.cursor().copy(
                f"COPY shop.{table} ({', '.join(cols)}) FROM STDIN"  # noqa: S608 - fixed names
            ) as copy:
                for row in rows:
                    copy.write_row([row[c] for c in cols])
            counts[table] = len(rows)
    log.info("loaded synthetic source: %s", counts)
    return counts
