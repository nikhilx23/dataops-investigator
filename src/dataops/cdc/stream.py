"""Read Debezium change topics and materialize current state.

Ordering rule (documented in docs/architecture/guarantees.md): within a table topic there is
one partition, and Debezium writes changes in commit order. Each envelope carries
source.lsn; the materializer applies changes ordered by (lsn, kafka offset) and keeps, per
primary key, the change with the greatest lsn. Re-delivered envelopes (at-least-once after a
connector restart) carry the same lsn and the same content, so applying them again is a
no-op: a *transport* duplicate. Two different source changes with identical business
content are distinct envelopes with different lsns: they are not transport duplicates.

Deletes: op = 'd' removes the key; the following tombstone (null value) is ignored.
Snapshot reads (op = 'r') have lsn of the snapshot point and are treated as inserts.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition

from dataops.cdc.connector import topic_for
from dataops.generator.generate import PRIMARY_KEYS


def bootstrap_servers() -> str:
    return os.environ.get("DATAOPS_KAFKA_BOOTSTRAP", "127.0.0.1:29092")


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    offset: int
    key: Any
    op: str | None  # c, u, d, r; None for tombstones
    lsn: int | None
    tx_id: int | None
    source_ts_ms: int | None
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    snapshot: str | None

    @property
    def is_tombstone(self) -> bool:
        return self.op is None


def parse(
    table: str, offset: int, key_bytes: bytes | None, value_bytes: bytes | None
) -> ChangeEvent:
    key = json.loads(key_bytes) if key_bytes else None
    if value_bytes is None:
        return ChangeEvent(table, offset, key, None, None, None, None, None, None, None)
    env = json.loads(value_bytes)
    src = env.get("source") or {}
    return ChangeEvent(
        table=table,
        offset=offset,
        key=key,
        op=env.get("op"),
        lsn=src.get("lsn"),
        tx_id=src.get("txId"),
        source_ts_ms=src.get("ts_ms"),
        before=env.get("before"),
        after=env.get("after"),
        snapshot=str(src.get("snapshot")) if src.get("snapshot") is not None else None,
    )


def read_topic(
    table: str, *, idle_timeout_s: float = 5.0, max_wait_s: float = 120.0
) -> Iterator[ChangeEvent]:
    """Read a table topic from the beginning up to its current end (bounded)."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": f"dataops-reader-{time.time_ns()}",  # throwaway: no committed offsets
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    topic = topic_for(table)
    try:
        md = consumer.list_topics(topic, timeout=10)
        if md.topics[topic].error is not None:
            return
        tp = TopicPartition(topic, 0)
        _low, high = consumer.get_watermark_offsets(tp, timeout=10)
        if high == 0:
            return
        consumer.assign([TopicPartition(topic, 0, 0)])
        deadline = time.monotonic() + max_wait_s
        last_progress = time.monotonic()
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                if time.monotonic() - last_progress > idle_timeout_s:
                    raise TimeoutError(f"{topic}: no progress before reaching offset {high}")
                continue
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"{topic}: {err}")
            last_progress = time.monotonic()
            offset = msg.offset()
            if offset is None:
                continue
            yield parse(table, offset, msg.key(), msg.value())
            if offset >= high - 1:
                return
        raise TimeoutError(f"{topic}: exceeded {max_wait_s}s")
    finally:
        consumer.close()


@dataclass
class Materialized:
    table: str
    rows: dict[Any, dict[str, Any]] = field(default_factory=dict)
    applied: int = 0
    transport_duplicates: int = 0
    tombstones: int = 0
    ops: dict[str, int] = field(default_factory=dict)


def materialize(table: str, events: list[ChangeEvent]) -> Materialized:
    pk = PRIMARY_KEYS[table]
    out = Materialized(table)
    last_lsn: dict[Any, int] = {}
    seen: set[tuple[Any, int | None, str | None]] = set()
    ordered = sorted(
        (e for e in events if not e.is_tombstone), key=lambda e: (e.lsn or 0, e.offset)
    )
    out.tombstones = sum(1 for e in events if e.is_tombstone)
    for e in ordered:
        row = e.after if e.op != "d" else e.before
        key = (row or e.key or {}).get(pk)
        ident = (key, e.lsn, e.op)
        if ident in seen and e.op != "r":
            out.transport_duplicates += 1
            continue
        seen.add(ident)
        assert e.op is not None
        out.ops[e.op] = out.ops.get(e.op, 0) + 1
        if e.lsn is not None and key in last_lsn and e.lsn < last_lsn[key]:
            continue  # older than what we already applied for this key
        if e.lsn is not None:
            last_lsn[key] = e.lsn
        if e.op == "d":
            out.rows.pop(key, None)
        else:
            out.rows[key] = dict(e.after or {})
        out.applied += 1
    return out
