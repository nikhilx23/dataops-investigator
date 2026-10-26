"""Debezium PostgreSQL connector: configuration and lifecycle through the Connect REST API.

The connector reads the publication `dataops_cdc` (created by migration 0004, owned by
src_app) through the logical replication slot `dataops_cdc` as role `cdc_reader`. The
password is referenced as ${env:DATAOPS_CDC_PASSWORD}, resolved inside the Connect worker,
so it never appears in the stored connector config or in REST responses.
"""

from __future__ import annotations

import os
import time
from typing import Any

import httpx

from dataops.config import database_name, redact
from dataops.generator.generate import TABLE_ORDER

CONNECTOR_NAME = "dataops-source-cdc"
TOPIC_PREFIX = "cdc"
SLOT_NAME = "dataops_cdc"
PUBLICATION = "dataops_cdc"
CDC_TABLES = list(TABLE_ORDER)  # every shop table is published (see migration 0004)


def connect_url() -> str:
    return os.environ.get("DATAOPS_CONNECT_URL", "http://127.0.0.1:8083")


def topic_for(table: str) -> str:
    return f"{TOPIC_PREFIX}.shop.{table}"


def connector_config() -> dict[str, str]:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        # The worker reaches PostgreSQL on the Compose network.
        "database.hostname": os.environ.get("DATAOPS_CDC_DB_HOST", "postgres"),
        "database.port": os.environ.get("DATAOPS_CDC_DB_PORT", "5432"),
        "database.user": "cdc_reader",
        "database.password": "${env:DATAOPS_CDC_PASSWORD}",
        "database.dbname": database_name("DATAOPS_SOURCE_DB"),
        "topic.prefix": TOPIC_PREFIX,
        "plugin.name": "pgoutput",
        "slot.name": SLOT_NAME,
        "publication.name": PUBLICATION,
        "publication.autocreate.mode": "disabled",
        "table.include.list": ",".join(f"shop.{t}" for t in CDC_TABLES),
        # Initial consistent snapshot (exported with the slot), then streaming from the slot.
        "snapshot.mode": "initial",
        "slot.drop.on.stop": "false",
        "tombstones.on.delete": "true",
        "decimal.handling.mode": "string",
        # Advances the slot even when the published tables are idle, so WAL is not pinned.
        "heartbeat.interval.ms": "10000",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        # One partition per table topic: total order of changes per table (and per key).
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "1",
        "topic.creation.default.cleanup.policy": "delete",
        "topic.creation.default.retention.ms": str(7 * 24 * 3600 * 1000),
    }


class ConnectError(RuntimeError):
    pass


def _client() -> httpx.Client:
    return httpx.Client(base_url=connect_url(), timeout=15.0)


def register() -> dict[str, Any]:
    """Create or update the connector (PUT is idempotent)."""
    with _client() as c:
        r = c.put(f"/connectors/{CONNECTOR_NAME}/config", json=connector_config())
        if r.status_code not in (200, 201):
            raise ConnectError(f"register failed: {r.status_code} {redact(r.text)[:500]}")
        return dict(r.json())


def status() -> dict[str, Any]:
    with _client() as c:
        r = c.get(f"/connectors/{CONNECTOR_NAME}/status")
        if r.status_code == 404:
            return {"name": CONNECTOR_NAME, "connector": {"state": "NOT_REGISTERED"}, "tasks": []}
        r.raise_for_status()
        out = dict(r.json())
    for task in out.get("tasks", []):
        if "trace" in task:  # stack traces can be long; keep the first lines, redacted
            task["trace"] = redact(task["trace"])[:1500]
    return out


def wait_running(timeout_s: float = 120) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = status()
        tasks = last.get("tasks", [])
        if (
            last.get("connector", {}).get("state") == "RUNNING"
            and tasks
            and all(t["state"] == "RUNNING" for t in tasks)
        ):
            return last
        if any(t.get("state") == "FAILED" for t in tasks):
            raise ConnectError(f"connector task failed: {last}")
        time.sleep(2)
    raise ConnectError(f"connector not running after {timeout_s}s: {last}")


def delete() -> None:
    """Remove the connector definition. The replication slot is kept (slot.drop.on.stop=false)."""
    with _client() as c:
        r = c.delete(f"/connectors/{CONNECTOR_NAME}")
        if r.status_code not in (204, 404):
            raise ConnectError(f"delete failed: {r.status_code} {redact(r.text)[:500]}")
