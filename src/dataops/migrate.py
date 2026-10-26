"""Minimal forward-only SQL migration runner.

Each target database has a directory of NNNN_name.sql files applied in order, each in its
own transaction, by the role that owns the target schema. Applied migrations are recorded
with their checksum; a changed checksum for an applied file is reported as an error.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from importlib import resources

from dataops.config import Role
from dataops.db import session

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Target:
    name: str
    role: Role
    schema: str


TARGETS = [
    Target("source", Role.SRC_APP, "shop"),
    Target("warehouse", Role.WH_LOADER, "raw"),
    Target("reliability", Role.REL_APP, "reliability"),
]


def _files(target: Target) -> list[tuple[str, str]]:
    folder = resources.files("dataops.migrations").joinpath(target.name)
    items = sorted(
        (p.name, p.read_text(encoding="utf-8")) for p in folder.iterdir() if p.name.endswith(".sql")
    )
    return items


def migrate(targets: list[str] | None = None) -> list[str]:
    applied_now: list[str] = []
    for target in TARGETS:
        if targets and target.name not in targets:
            continue
        # autocommit: each `conn.transaction()` below is then a real top-level transaction
        # (with autocommit off, an earlier SELECT would open an implicit transaction and the
        # blocks would become savepoints that are rolled back when the session closes).
        with session(target.role, application_name="dataops-migrate", autocommit=True) as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {target.schema}.schema_migrations ("
                " filename text PRIMARY KEY, checksum text NOT NULL,"
                " applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
            )
            done = dict(
                conn.execute(
                    f"SELECT filename, checksum FROM {target.schema}.schema_migrations"  # noqa: S608
                ).fetchall()
            )
            for name, sql in _files(target):
                checksum = hashlib.sha256(sql.encode()).hexdigest()
                if name in done:
                    if done[name] != checksum:
                        raise RuntimeError(
                            f"{target.name}/{name} changed after being applied; "
                            "add a new migration instead of editing an applied one"
                        )
                    continue
                with conn.transaction():
                    conn.execute(sql.encode())
                    conn.execute(
                        f"INSERT INTO {target.schema}.schema_migrations (filename, checksum) "  # noqa: S608
                        "VALUES (%s, %s)",
                        (name, checksum),
                    )
                applied_now.append(f"{target.name}/{name}")
                log.info("applied migration %s/%s", target.name, name)
    return applied_now
