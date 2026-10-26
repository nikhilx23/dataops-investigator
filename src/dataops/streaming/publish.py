"""Controlled publication of curated Silver data into the warehouse (cdc_pub schema).

Boundary guarantees:
- Each Silver table is read at one Delta version (a consistent snapshot of that table); the
  versions are recorded in the manifest. Tables are read one after another, so when the
  streaming jobs are running, two tables can be at slightly different points in time:
  publish after a processing cycle (as the orchestrated pipeline does) for a cross-table
  consistent cut.
- The warehouse side is atomic: manifest insert, removal of the previous records and the
  COPY of the new ones happen in one PostgreSQL transaction, under an advisory lock that
  rejects a concurrent publisher. dbt (source_path=cdc) reads only the latest committed
  publication, so it never consumes a partial one.
- Soft-deleted Silver rows are not published: the warehouse sees source deletes as absent.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from dataops.config import Role
from dataops.contracts import load as load_contracts
from dataops.db import connect
from dataops.streaming import session as S

log = logging.getLogger(__name__)
PUBLISHER_VERSION = "silver-publisher/1.0"
LOCK_KEY = 0x5D_A7A0_C0B1  # pg_advisory lock id for the cdc_pub publication scope


class ConcurrentPublicationError(RuntimeError):
    pass


def _collect(spark: Any) -> tuple[list[tuple[Any, ...]], dict[str, int], dict[str, int]]:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    rows: list[tuple[Any, ...]] = []
    versions: dict[str, int] = {}
    counts: dict[str, int] = {}
    for table, contract in load_contracts().items():
        location = S.path("silver", table)
        last = DeltaTable.forPath(spark, location).history(1).first()
        if last is None:
            raise RuntimeError(f"Silver table {table} has no history; run the Silver job first")
        version = int(last["version"])
        df = (
            spark.read.format("delta")
            .option("versionAsOf", version)
            .load(location)
            .where("NOT _is_deleted")
        )
        payload = F.to_json(
            F.struct(*contract.column_names),
            {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX", "ignoreNullFields": "false"},
        )
        out = df.select(
            F.col(contract.key).cast("string").alias("pk"),
            F.col("updated_at"),
            payload.alias("payload"),
            F.col("_lsn"),
        )
        part = [
            (
                table,
                r.pk,
                r.updated_at.replace(tzinfo=UTC),
                r.payload,
                f"{table}:{r.pk}@{r._lsn}",
                r._lsn,
            )
            for r in out.collect()
        ]
        rows.extend(part)
        versions[table], counts[table] = version, len(part)
    return rows, versions, counts


def publish(spark: Any, correlation_id: str) -> dict[str, Any]:
    started = datetime.now(UTC)
    rows, versions, counts = _collect(spark)
    checksum = hashlib.sha256("\n".join(sorted(r[4] for r in rows)).encode()).hexdigest()
    publication_id = str(uuid.uuid4())
    with (
        connect(
            Role.WH_LOADER, application_name="dataops-publisher", statement_timeout_s=600
        ) as conn,
        conn.transaction(),
    ):
        locked = conn.execute("select pg_try_advisory_xact_lock(%s)", (LOCK_KEY,)).fetchone()
        if not (locked and locked[0]):
            raise ConcurrentPublicationError("another Silver publication is in progress")
        conn.execute(
            "insert into cdc_pub.publications (publication_id, status, silver_versions,"
            " row_counts, content_checksum, correlation_id, publisher_version, started_at)"
            " values (%s, 'committed', %s, %s, %s, %s, %s, %s)",
            (
                publication_id,
                json.dumps(versions),
                json.dumps(counts),
                checksum,
                correlation_id,
                PUBLISHER_VERSION,
                started,
            ),
        )
        conn.execute("delete from cdc_pub.records")
        with conn.cursor().copy(
            "copy cdc_pub.records (source_table, source_pk, source_updated_at, payload,"
            " record_id, publication_id, source_lsn) from stdin"
        ) as copy:
            for table, pk, updated_at, payload, record_id, lsn in rows:
                copy.write_row([table, pk, updated_at, payload, record_id, publication_id, lsn])
    result = {
        "publication_id": publication_id,
        "rows": len(rows),
        "silver_versions": versions,
        "row_counts": counts,
        "content_checksum": checksum,
    }
    log.info("published silver snapshot %s", json.dumps(result))
    return result
