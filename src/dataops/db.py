"""PostgreSQL connections with bounded retries and per-session statement timeouts."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg

from dataops.config import Role, connection_spec, redact

log = logging.getLogger(__name__)

TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (psycopg.OperationalError,)


def connect(
    role: Role,
    *,
    application_name: str = "dataops",
    statement_timeout_s: int = 120,
    attempts: int = 3,
    backoff_s: float = 1.0,
    autocommit: bool = False,
) -> psycopg.Connection:
    spec = connection_spec(role, application_name)
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            conn = psycopg.connect(
                **spec.kwargs(),  # type: ignore[arg-type]
                autocommit=autocommit,
                options=f"-c statement_timeout={statement_timeout_s * 1000} -c TimeZone=UTC",
            )
            return conn
        except TRANSIENT_ERRORS as exc:
            last = exc
            log.warning(
                "connect attempt %s/%s to %s failed: %s",
                attempt,
                attempts,
                spec.safe_description(),
                redact(str(exc)).strip(),
            )
            if attempt < attempts:
                time.sleep(backoff_s * 2 ** (attempt - 1))
    raise ConnectionError(
        f"could not connect to {spec.safe_description()} after {attempts} attempts: "
        f"{redact(str(last)).strip()}"
    )


@contextmanager
def session(role: Role, **kwargs: object) -> Iterator[psycopg.Connection]:
    conn = connect(role, **kwargs)  # type: ignore[arg-type]
    try:
        yield conn
    finally:
        conn.close()
