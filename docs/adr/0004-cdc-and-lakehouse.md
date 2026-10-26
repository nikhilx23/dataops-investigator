# ADR 0004: CDC with Debezium, Spark on Delta for Bronze/Silver, controlled publication

Status: accepted (Milestone 2b)

## Context
Batch extraction cannot see hard deletes or intermediate versions (see guarantees). The full
profile must demonstrate real change data capture, streaming with event time, and a
lakehouse, without letting partially processed data reach dbt.

## Decision
- **Capture**: Debezium 3.0.8 PostgreSQL connector (pgoutput) reads publication
  `dataops_cdc` through slot `dataops_cdc` as `cdc_reader` (REPLICATION + SELECT only). The
  publication is created by a migration owned by `src_app`; the connector never creates it.
  The password is resolved in the Connect worker via `EnvVarConfigProvider`, so it is not
  stored in the connector config. One single-partition topic per table (`cdc.shop.<table>`).
- **Transport**: Kafka 3.9 (KRaft, single node) from Debezium's Quay image. Docker Hub was
  avoided for the broker after anonymous pull rate limits (HTTP 429) in the build sandbox.
- **Bronze**: Spark Structured Streaming, Kafka -> Delta append, raw envelope + offsets +
  sha256, partitioned by source table (11 partitions; no per-day partitioning at demo volumes).
- **Silver**: Bronze Delta stream -> `foreachBatch`: contract validation (contracts/source/
  shop.v1.yaml), quarantine with reasons, per-key latest-LSN de-duplication, MERGE that only
  applies strictly newer LSNs, soft deletes. Idempotent appends use Delta txnAppId/txnVersion.
- **Event time**: shipment events hourly counts with a configurable watermark
  (`DATAOPS_EVENT_WATERMARK`, default 6 hours) and `dropDuplicatesWithinWatermark` on the
  business key (shipment, event type); RocksDB state store. A batch recomputation from Silver
  is the correction path for events later than the watermark.
- **Publication**: Silver snapshot -> `cdc_pub` in the warehouse in one transaction with a
  manifest and advisory lock; dbt reads the latest committed publication when
  `source_path=cdc`. The same staging models serve both paths.
- **Orchestration**: Airflow 3.3.2 runs bounded work only (available-now stream processing,
  publication, dbt, reconciliation). Continuous streaming is a separate long-running process.

## Alternatives considered
- Kafka Connect sink to the warehouse: skips the lakehouse and contract quarantine.
- Spark writing straight to PostgreSQL over JDBC: no atomic multi-table publication.
- Hard deletes in Silver: loses the last known state that investigation needs; soft delete
  keeps it while publication still removes deleted rows.

## Consequences
- Delivery is at-least-once from Debezium (after restarts), exactly-once from Kafka into
  Bronze (checkpoint + idempotent Delta sink), and effectively-once into Silver (LSN-guarded
  MERGE). End-to-end exactly-once is not claimed.
- Spark startup costs ~40 s per run in local mode; acceptable for scheduled bounded runs.
