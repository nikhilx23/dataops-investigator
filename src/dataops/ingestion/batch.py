"""Incremental, restart-safe batch extraction from the source into warehouse raw.

Guarantees (see docs/architecture/guarantees.md):
- Window semantics: rows with window_start - overlap <= updated_at < window_end.
  The overlap re-reads recent rows so that changes committed late (after a previous window
  closed) are still captured; re-read rows are de-duplicated by content.
- Source reads use one REPEATABLE READ, READ ONLY transaction, so every page of a batch sees
  the same snapshot. Pagination is keyset-based on (updated_at, primary key): stable even
  when rows share timestamps.
- Publication is atomic: all record inserts, the batch status change to 'committed' and the
  watermark advance happen in a single warehouse transaction.
- Idempotent: record_id = sha256(system|table|pk|payload checksum); re-running any window
  inserts nothing new for unchanged rows.
- One writer per table: a partial unique index rejects a second 'running' batch.
- Not captured: hard deletes (batch cannot see them; CDC in the full profile can), and
  intermediate versions of a row changed several times between two extractions.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg import errors

from dataops.config import Role, redact
from dataops.db import connect
from dataops.generator.generate import PRIMARY_KEYS, TABLE_ORDER

log = logging.getLogger(__name__)

SOURCE_SYSTEM = "commerce_source"
LOADER_VERSION = "batch-ingest/1.0"
DEFAULT_OVERLAP = timedelta(minutes=15)
DEFAULT_PAGE_SIZE = 2_000
RUNNING_LEASE = timedelta(minutes=30)


class OverlappingWriterError(RuntimeError):
    """Another batch for the same table is already running."""


@dataclass
class BatchResult:
    batch_id: str
    source_table: str
    window_start: str
    window_end: str
    status: str
    extracted_rows: int = 0
    inserted_rows: int = 0
    duplicate_rows: int = 0
    pages: int = 0
    content_checksum: str | None = None
    attempts: int = 1
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def payload_checksum(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def record_id(table: str, pk: str, checksum: str) -> str:
    return hashlib.sha256(f"{SOURCE_SYSTEM}|{table}|{pk}|{checksum}".encode()).hexdigest()


def _validate(table: str, start: datetime, end: datetime) -> None:
    if table not in PRIMARY_KEYS:
        raise ValueError(f"unknown source table {table!r}; allowed: {sorted(PRIMARY_KEYS)}")
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("window bounds must be timezone-aware (UTC)")
    if end <= start:
        raise ValueError("window_end must be after window_start")


def _register_batch(
    table: str,
    start: datetime,
    end: datetime,
    overlap: timedelta,
    run_id: str | None,
    correlation_id: str,
) -> str:
    batch_id = str(uuid.uuid4())
    with connect(Role.WH_LOADER, application_name="dataops-ingest", autocommit=True) as conn:
        # A crashed loader leaves 'running' behind; nothing it wrote was committed.
        conn.execute(
            "UPDATE raw.ingestion_batches"
            " SET status = 'abandoned', finished_at = clock_timestamp(),"
            " error_message = 'lease expired; no data was published by this batch'"
            " WHERE source_system = %s AND source_table = %s AND status = 'running'"
            " AND started_at < clock_timestamp() - %s",
            (SOURCE_SYSTEM, table, RUNNING_LEASE),
        )
        try:
            conn.execute(
                "INSERT INTO raw.ingestion_batches (batch_id, source_system, source_table,"
                " window_start, window_end, overlap_seconds, extract_lower_bound, status,"
                " run_id, correlation_id, loader_version)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,'running',%s,%s,%s)",
                (
                    batch_id,
                    SOURCE_SYSTEM,
                    table,
                    start,
                    end,
                    int(overlap.total_seconds()),
                    start - overlap,
                    run_id,
                    correlation_id,
                    LOADER_VERSION,
                ),
            )
        except errors.UniqueViolation as exc:
            raise OverlappingWriterError(
                f"another batch for {table} is running; refusing to start an overlapping writer"
            ) from exc
    return batch_id


def _mark_failed(batch_id: str, message: str) -> None:
    with connect(Role.WH_LOADER, application_name="dataops-ingest", autocommit=True) as conn:
        conn.execute(
            "UPDATE raw.ingestion_batches SET status='failed', finished_at=clock_timestamp(),"
            " error_message=%s WHERE batch_id=%s AND status='running'",
            (redact(message)[:2000], batch_id),
        )


def _extract_and_publish(
    batch_id: str, table: str, start: datetime, end: datetime, overlap: timedelta, page_size: int
) -> BatchResult:
    pk = PRIMARY_KEYS[table]
    lower = start - overlap
    result = BatchResult(batch_id, table, start.isoformat(), end.isoformat(), "running")
    digest = hashlib.sha256()
    max_updated: datetime | None = None
    # Fixed identifiers from an allowlist; values are always bound parameters.
    page_sql = (
        f"SELECT to_jsonb(t) AS payload, t.updated_at, t.{pk}::text AS pk "  # noqa: S608
        f"FROM shop.{table} t WHERE t.updated_at >= %(lo)s AND t.updated_at < %(hi)s "
        f"AND (t.updated_at, t.{pk}) > (%(after_ts)s, %(after_pk)s) "
        f"ORDER BY t.updated_at, t.{pk} LIMIT %(limit)s"
    )
    with (
        connect(Role.SRC_READER, application_name="dataops-ingest") as src,
        connect(Role.WH_LOADER, application_name="dataops-ingest", statement_timeout_s=600) as wh,
    ):
        src.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        src.read_only = True
        with src.transaction(), wh.transaction():
            wh.execute(
                "CREATE TEMP TABLE _stage (record_id text, source_pk text,"
                " source_updated_at timestamptz, payload jsonb, payload_checksum text)"
                " ON COMMIT DROP"
            )
            after_ts = datetime(1970, 1, 1, tzinfo=UTC)
            after_pk: int = -1
            while True:
                rows = src.execute(
                    page_sql,
                    {
                        "lo": lower,
                        "hi": end,
                        "after_ts": after_ts,
                        "after_pk": after_pk,
                        "limit": page_size,
                    },
                ).fetchall()
                if not rows:
                    break
                result.pages += 1
                with wh.cursor().copy(
                    "COPY _stage (record_id, source_pk, source_updated_at, payload,"
                    " payload_checksum) FROM STDIN"
                ) as copy:
                    for payload, updated_at, pk_value in rows:
                        checksum = payload_checksum(payload)
                        digest.update(checksum.encode())
                        copy.write_row(
                            [
                                record_id(table, pk_value, checksum),
                                pk_value,
                                updated_at,
                                json.dumps(payload),
                                checksum,
                            ]
                        )
                result.extracted_rows += len(rows)
                after_ts, after_pk = rows[-1][1], int(rows[-1][2])
                max_updated = after_ts
                if len(rows) < page_size:
                    break
            cur = wh.execute(
                "INSERT INTO raw.source_records (record_id, source_system, source_table,"
                " source_pk, source_updated_at, payload, payload_checksum, batch_id)"
                " SELECT DISTINCT ON (record_id) record_id, %s, %s, source_pk,"
                " source_updated_at, payload, payload_checksum, %s FROM _stage"
                " ORDER BY record_id ON CONFLICT (record_id) DO NOTHING",
                (SOURCE_SYSTEM, table, batch_id),
            )
            result.inserted_rows = cur.rowcount
            result.duplicate_rows = result.extracted_rows - result.inserted_rows
            result.content_checksum = digest.hexdigest()
            wh.execute(
                "UPDATE raw.ingestion_batches"
                " SET status='committed', finished_at=clock_timestamp(),"
                " extracted_rows=%s, inserted_rows=%s, duplicate_rows=%s, pages=%s,"
                " content_checksum=%s, max_source_updated_at=%s WHERE batch_id=%s",
                (
                    result.extracted_rows,
                    result.inserted_rows,
                    result.duplicate_rows,
                    result.pages,
                    result.content_checksum,
                    max_updated,
                    batch_id,
                ),
            )
            wh.execute(
                "INSERT INTO raw.ingestion_watermarks (source_system, source_table,"
                " high_watermark, last_batch_id) VALUES (%s,%s,%s,%s)"
                " ON CONFLICT (source_system, source_table) DO UPDATE SET"
                " high_watermark = GREATEST(raw.ingestion_watermarks.high_watermark,"
                " EXCLUDED.high_watermark), last_batch_id = EXCLUDED.last_batch_id,"
                " updated_at = clock_timestamp()",
                (SOURCE_SYSTEM, table, end, batch_id),
            )
    result.status = "committed"
    return result


def ingest_table(
    table: str,
    window_start: datetime,
    window_end: datetime,
    *,
    overlap: timedelta = DEFAULT_OVERLAP,
    page_size: int = DEFAULT_PAGE_SIZE,
    run_id: str | None = None,
    correlation_id: str | None = None,
    attempts: int = 3,
    backoff_s: float = 2.0,
) -> BatchResult:
    """Extract one bounded window for one table. Retries transient failures, each retry
    as a fresh batch (a failed attempt publishes nothing)."""
    _validate(table, window_start, window_end)
    correlation_id = correlation_id or str(uuid.uuid4())
    last_error = ""
    for attempt in range(1, attempts + 1):
        batch_id = _register_batch(table, window_start, window_end, overlap, run_id, correlation_id)
        try:
            result = _extract_and_publish(
                batch_id, table, window_start, window_end, overlap, page_size
            )
            result.attempts = attempt
            log.info("ingest committed %s", json.dumps(result.as_dict()))
            return result
        except (psycopg.OperationalError, ConnectionError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            _mark_failed(batch_id, last_error)
            log.warning(
                "ingest attempt %s/%s for %s failed: %s",
                attempt,
                attempts,
                table,
                redact(last_error),
            )
            if attempt < attempts:
                time.sleep(backoff_s * 2 ** (attempt - 1))
        except Exception as exc:
            _mark_failed(batch_id, f"{type(exc).__name__}: {exc}")
            raise
    return BatchResult(
        batch_id,
        table,
        window_start.isoformat(),
        window_end.isoformat(),
        "failed",
        attempts=attempts,
        error=redact(last_error),
    )


def split_windows(
    start: datetime, end: datetime, step: timedelta
) -> list[tuple[datetime, datetime]]:
    windows = []
    cursor = start
    while cursor < end:
        nxt = min(cursor + step, end)
        windows.append((cursor, nxt))
        cursor = nxt
    return windows


def ingest_range(
    tables: list[str],
    start: datetime,
    end: datetime,
    *,
    step: timedelta = timedelta(days=1),
    **kwargs: Any,
) -> list[BatchResult]:
    """Ingest [start, end) as consecutive bounded windows, tables in dependency order."""
    ordered = [t for t in TABLE_ORDER if t in tables]
    results: list[BatchResult] = []
    for w_start, w_end in split_windows(start, end, step):
        for table in ordered:
            results.append(ingest_table(table, w_start, w_end, **kwargs))
    return results


def watermark(table: str) -> datetime | None:
    with connect(Role.WH_LOADER, application_name="dataops-ingest") as conn:
        row = conn.execute(
            "SELECT high_watermark FROM raw.ingestion_watermarks"
            " WHERE source_system=%s AND source_table=%s",
            (SOURCE_SYSTEM, table),
        ).fetchone()
    return row[0] if row else None
