"""Bronze, Silver and event-time streaming jobs (Spark Structured Streaming on Delta).

Bronze  Kafka cdc.shop.* -> Delta bronze/cdc_events, append-only, immutable raw envelopes
        with topic/partition/offset provenance and a payload checksum. Kafka source +
        checkpoint + Delta sink: each Kafka offset is written exactly once to Bronze
        (Delta's streaming sink is idempotent per micro-batch id).
Silver  Bronze (Delta stream) -> one Delta table per source table, current state, typed per
        the v1 contract. foreachBatch: validate, quarantine violations with reasons,
        de-duplicate per key keeping the highest LSN, MERGE only when the incoming LSN is
        newer (idempotent on micro-batch retry). Deletes are soft (_is_deleted = true).
Events  Bronze shipment_events -> hourly counts by event type, event time = event_at,
        watermark (configurable delay) bounds state; business duplicates (same shipment and
        event type) are dropped within the watermark. Events later than the watermark are
        dropped from this low-latency aggregate (counted in progress metrics) and are still
        in Bronze and Silver, so the batch correction (`recompute_hourly`) includes them.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from dataops.contracts import load as load_contracts
from dataops.streaming import session as S
from dataops.streaming.validation import silver_schema_ddl, typed_columns, with_validation

log = logging.getLogger(__name__)

TOPIC_PATTERN = r"cdc\.shop\..*"


def watermark_delay() -> str:
    return os.environ.get("DATAOPS_EVENT_WATERMARK", "6 hours")


# ------------------------------------------------------------------------------- bronze
def bronze_query(spark: Any, trigger: dict[str, Any]) -> Any:
    from pyspark.sql import functions as F

    from dataops.cdc.stream import bootstrap_servers

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers())
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        # Unread data deleted by retention is a visible failure, never a silent gap.
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )
    out = raw.select(
        F.regexp_extract("topic", r"^cdc\.shop\.(.+)$", 1).alias("source_table"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.sha2(F.col("value").cast("string"), 256).alias("value_sha256"),
        F.current_timestamp().alias("ingested_at"),
    )
    return (
        out.writeStream.format("delta")
        .option("checkpointLocation", S.checkpoint("bronze"))
        .partitionBy("source_table")
        .queryName("bronze")
        .trigger(**trigger)
        .start(S.path(*S.BRONZE))
    )


# ------------------------------------------------------------------------------- silver
ENVELOPE_DDL = (
    "op string, before map<string,string>, after map<string,string>, "
    "source struct<lsn:bigint, txId:bigint, ts_ms:bigint>"
)


def _ensure_tables(spark: Any) -> None:
    from delta.tables import DeltaTable

    for table, contract in load_contracts().items():
        (
            DeltaTable.createIfNotExists(spark)
            .location(S.path("silver", table))
            .addColumns(spark.createDataFrame([], silver_schema_ddl(contract)).schema)
            .execute()
        )
    for name, ddl in (
        (
            S.QUARANTINE,
            "source_table string, key string, lsn bigint, op string, "
            "reasons array<string>, value string, bronze_topic string, "
            "bronze_offset bigint, contract_version int, silver_batch_id bigint, "
            "quarantined_at timestamp",
        ),
        (
            S.BATCH_LOG,
            "silver_batch_id bigint, source_table string, input_rows bigint, "
            "tombstones bigint, quarantined bigint, transport_duplicates bigint, "
            "upserts bigint, deletes bigint, logged_at timestamp",
        ),
    ):
        (
            DeltaTable.createIfNotExists(spark)
            .location(S.path(*name))
            .addColumns(spark.createDataFrame([], ddl).schema)
            .execute()
        )


def process_silver_batch(batch: Any, batch_id: int) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = batch.sparkSession
    contracts = load_contracts()
    parsed = (
        batch.withColumn("env", F.from_json("value", ENVELOPE_DDL))
        .select(
            "source_table",
            "topic",
            "offset",
            "value",
            F.col("env.op").alias("op"),
            F.col("env.source.lsn").alias("lsn"),
            F.col("env.source.ts_ms").alias("ts_ms"),
            F.when(F.col("env.op") == "d", F.col("env.before"))
            .otherwise(F.col("env.after"))
            .alias("row"),
        )
        .cache()
    )
    try:
        logs = []
        for table, contract in contracts.items():
            part = parsed.where(F.col("source_table") == table)
            tomb = part.where(F.col("value").isNull())
            events = with_validation(part.where(F.col("value").isNotNull()), contract)
            bad = events.where(F.size("_reasons") > 0)
            good = events.where(F.size("_reasons") == 0)
            n_in, n_tomb, n_bad = part.count(), tomb.count(), bad.count()
            if n_bad:
                (
                    bad.select(
                        F.lit(table).alias("source_table"),
                        F.col("row")[contract.key].alias("key"),
                        "lsn",
                        "op",
                        F.col("_reasons").alias("reasons"),
                        "value",
                        F.col("topic").alias("bronze_topic"),
                        F.col("offset").alias("bronze_offset"),
                        F.lit(contract.version).alias("contract_version"),
                        F.lit(batch_id).cast("bigint").alias("silver_batch_id"),
                        F.current_timestamp().alias("quarantined_at"),
                    )
                    .write.format("delta")
                    .mode("append")
                    .option("txnAppId", f"silver-quarantine-{table}")
                    .option("txnVersion", batch_id)
                    .save(S.path(*S.QUARANTINE))
                )
            # Transport duplicates: the same change (key, lsn, op) delivered more than once.
            good = good.withColumn("_k", F.col("row")[contract.key])
            dedup_exact = good.dropDuplicates(["_k", "lsn", "op"])
            n_dup = good.count() - dedup_exact.count()
            w = Window.partitionBy("_k").orderBy(F.col("lsn").desc(), F.col("offset").desc())
            latest = (
                dedup_exact.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
            )
            n_del = latest.where(F.col("op") == "d").count()
            n_up = latest.count() - n_del
            if n_up + n_del:
                src = latest.select(
                    *typed_columns(contract),
                    F.col("lsn").alias("_lsn"),
                    F.col("op").alias("_op"),
                    (F.col("op") == "d").alias("_is_deleted"),
                    F.timestamp_millis("ts_ms").alias("_source_ts"),
                    F.col("topic").alias("_bronze_topic"),
                    F.col("offset").alias("_bronze_offset"),
                    F.lit(batch_id).cast("bigint").alias("_silver_batch_id"),
                    F.current_timestamp().alias("_updated_at"),
                )
                target = DeltaTable.forPath(spark, S.path("silver", table))
                k = contract.key
                meta = [
                    "_lsn",
                    "_op",
                    "_is_deleted",
                    "_source_ts",
                    "_bronze_topic",
                    "_bronze_offset",
                    "_silver_batch_id",
                    "_updated_at",
                ]
                (
                    target.alias("t")
                    .merge(src.alias("s"), f"t.`{k}` = s.`{k}`")
                    # Only a strictly newer change wins: replays and retries are no-ops.
                    # A delete is soft: it flags the row and keeps its last known values.
                    .whenMatchedUpdate(
                        condition="s._lsn > t._lsn AND s._op = 'd'", set={m: f"s.{m}" for m in meta}
                    )
                    .whenMatchedUpdateAll(condition="s._lsn > t._lsn AND s._op <> 'd'")
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            if n_in:
                logs.append((batch_id, table, n_in, n_tomb, n_bad, n_dup, n_up, n_del))
        if logs:
            (
                spark.createDataFrame(
                    logs,
                    "silver_batch_id bigint, source_table string, "
                    "input_rows bigint, tombstones bigint, "
                    "quarantined bigint, transport_duplicates bigint, "
                    "upserts bigint, deletes bigint",
                )
                .withColumn("logged_at", F.current_timestamp())
                .write.format("delta")
                .mode("append")
                .option("txnAppId", "silver-batch-log")
                .option("txnVersion", batch_id)
                .save(S.path(*S.BATCH_LOG))
            )
    finally:
        parsed.unpersist()


def silver_query(spark: Any, trigger: dict[str, Any]) -> Any:
    _ensure_tables(spark)
    src = spark.readStream.format("delta").option("maxFilesPerTrigger", 200).load(S.path(*S.BRONZE))
    return (
        src.writeStream.foreachBatch(process_silver_batch)
        .option("checkpointLocation", S.checkpoint("silver"))
        .queryName("silver")
        .trigger(**trigger)
        .start()
    )


# ------------------------------------------------------------------------------- events
def _shipment_events(df: Any) -> Any:
    from pyspark.sql import functions as F

    env = F.from_json("value", ENVELOPE_DDL)
    return (
        df.where((F.col("source_table") == "shipment_events") & F.col("value").isNotNull())
        .withColumn("env", env)
        .where(F.col("env.op").isin("c", "r"))
        .select(
            F.col("env.after")["shipment_event_id"].cast("bigint").alias("shipment_event_id"),
            F.col("env.after")["shipment_id"].cast("bigint").alias("shipment_id"),
            F.col("env.after")["event_type"].alias("event_type"),
            F.to_timestamp(F.col("env.after")["event_at"]).alias("event_at"),
            F.to_timestamp(F.col("env.after")["received_at"]).alias("received_at"),
        )
        .where(F.col("event_at").isNotNull())
    )


def events_query(spark: Any, trigger: dict[str, Any]) -> Any:
    from pyspark.sql import functions as F

    src = spark.readStream.format("delta").load(S.path(*S.BRONZE))
    hourly = (
        _shipment_events(src)
        .withWatermark("event_at", watermark_delay())
        .dropDuplicatesWithinWatermark(["shipment_id", "event_type"])
        .groupBy(F.window("event_at", "1 hour").alias("w"), "event_type")
        .agg(F.count("*").alias("events"))
        .select(F.col("w.start").alias("hour_start"), "event_type", "events")
    )
    return (
        hourly.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", S.checkpoint("events_hourly"))
        .queryName("events_hourly")
        .trigger(**trigger)
        .start(S.path(*S.HOURLY))
    )


def recompute_hourly(spark: Any) -> Any:
    """Batch correction path: hourly counts from Silver, including events that arrived
    after the streaming watermark. Business duplicates are removed the same way."""
    from pyspark.sql import functions as F

    ev = (
        spark.read.format("delta")
        .load(S.path("silver", "shipment_events"))
        .where(~F.col("_is_deleted"))
        .dropDuplicates(["shipment_id", "event_type"])
    )
    return ev.groupBy(F.date_trunc("hour", "event_at").alias("hour_start"), "event_type").agg(
        F.count("*").alias("events")
    )


# ------------------------------------------------------------------------------ runner
def run(
    jobs: list[str],
    *,
    available_now: bool = True,
    processing_time: str = "10 seconds",
    run_seconds: float | None = None,
) -> dict[str, Any]:
    """Run the given jobs in one Spark application. available_now: process everything
    present and stop; otherwise run continuously (for run_seconds, or until interrupted)."""
    spark = S.get_spark("dataops-streaming")
    trigger: dict[str, Any] = (
        {"availableNow": True} if available_now else {"processingTime": processing_time}
    )
    report: dict[str, Any] = {}
    order = [j for j in ("bronze", "silver", "events") if j in jobs]
    builders = {"bronze": bronze_query, "silver": silver_query, "events": events_query}
    if available_now:  # sequential: each stage sees everything its upstream wrote
        for job in order:
            q = builders[job](spark, trigger)
            q.awaitTermination()
            report[job] = _summarize(q)
    else:
        queries = [builders[j](spark, trigger) for j in order]
        try:
            if run_seconds is None:
                spark.streams.awaitAnyTermination()
            else:
                spark.streams.awaitAnyTermination(int(run_seconds * 1000))
        finally:
            for q in queries:
                report[q.name] = _summarize(q)
                q.stop()
    return report


def _summarize(q: Any) -> dict[str, Any]:
    progs = q.recentProgress or []
    dropped = sum(
        op.get("numRowsDroppedByWatermark", 0)
        for p in progs
        for op in (p.get("stateOperators") or [])
    )
    state_rows = [
        op.get("numRowsTotal", 0) for p in progs[-1:] for op in (p.get("stateOperators") or [])
    ]
    if q.exception():
        raise RuntimeError(f"{q.name} failed: {q.exception()}")
    return {
        "batches": len(progs),
        "input_rows": sum(p.get("numInputRows", 0) for p in progs),
        "rows_dropped_by_watermark": dropped,
        "state_rows_last": state_rows,
        "last_watermark": (progs[-1].get("eventTime") or {}).get("watermark") if progs else None,
        "last_progress": json.loads(json.dumps(progs[-1], default=str)) if progs else None,
    }


def replay_silver() -> dict[str, Any]:
    """Explicit rebuild of every derived lakehouse table from Bronze (never automatic).

    Deletes Silver tables, the quarantine and batch log, the hourly aggregate and their
    checkpoints. Bronze (the immutable raw history) and its checkpoint are untouched, so the
    rebuild replays exactly the same envelopes.
    """
    import shutil

    removed = []
    for rel in (("silver",), ("_checkpoints", "silver"), ("_checkpoints", "events_hourly")):
        target = S.lake_root().joinpath(*rel)
        if target.exists():
            shutil.rmtree(target)
            removed.append(str(target.relative_to(S.lake_root())))
    log.warning("replay: removed derived lakehouse state %s", removed)
    return {"removed": removed}
