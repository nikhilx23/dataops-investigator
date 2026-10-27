"""Cross-process single-writer lock for the lakehouse (streaming jobs and Silver publication).

Airflow's `warehouse_writer` pool only serializes Airflow tasks; a run started from the CLI
(or a second scheduler) could still write the same Delta tables and checkpoints. Spark
detects concurrent writers on one checkpoint and fails the newer query
(CONCURRENT_STREAM_LOG_UPDATE) - safe, but noisy and late. This advisory file lock rejects
the second writer immediately with a clear message. POSIX only (fcntl), like the jobs.
"""

from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

from dataops.streaming.session import lake_root


class LakehouseBusyError(RuntimeError):
    pass


@contextmanager
def lakehouse_lock(holder: str) -> Iterator[None]:
    path = lake_root() / "_locks" / "lakehouse.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            current = os.pread(fd, 300, 0).decode(errors="replace").strip()
            raise LakehouseBusyError(
                f"another lakehouse writer holds {path}: {current or 'unknown holder'}"
            ) from None
        os.ftruncate(fd, 0)
        os.pwrite(
            fd, f"{holder} pid={os.getpid()} since={datetime.now(UTC).isoformat()}".encode(), 0
        )
        yield
    finally:
        os.close(fd)  # closing the descriptor releases the lock
