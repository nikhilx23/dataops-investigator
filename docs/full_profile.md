# Full profile: CDC, streaming, lakehouse, orchestration

All data is synthetic. Every command below was run in the build environment; results are in
[PROGRESS.md](PROGRESS.md). Ports bind to 127.0.0.1.

## Components and pinned versions

| Component | Version | Runs as |
|---|---|---|
| PostgreSQL + pgvector | 16 / 0.8.0 (`pgvector/pgvector:0.8.0-pg16`), `wal_level=logical` | Compose `postgres`, host port 5433 |
| Kafka (KRaft) | 3.9.0 (`quay.io/debezium/kafka:3.0.8.Final`) | Compose `kafka`, host port 29092 |
| Debezium PostgreSQL connector | 3.0.8.Final (`quay.io/debezium/connect:3.0.8.Final`) | Compose `connect`, REST 8083 |
| Spark / Delta Lake | 4.2.0 / 4.4.1 (Scala 2.13), Java 21 | Python process (`dataops stream ...`), local[2] |
| Kafka source for Spark | `spark-sql-kafka-0-10_2.13:4.2.0` (Maven Central) | resolved on first run into `~/.ivy2*` |
| Airflow | 3.3.2 with official constraints (Python 3.13) | `scripts/airflow.sh`, own venv |

## Start, check, stop (Unix shell; PowerShell equivalents in scripts/*.ps1)

```bash
python scripts/init_env.py                     # once; adds DATAOPS_CDC_PASSWORD too
docker compose --profile full up -d --wait postgres kafka connect
export DATAOPS_PG_PORT=5433                     # the CLI talks to the Compose PostgreSQL
python -m dataops db migrate
python -m dataops simulate --until 2026-03-05T00:00:00+00:00   # synthetic source, day 3
python -m dataops cdc register                  # Debezium snapshot + streaming
python -m dataops cdc reconcile                 # topics vs source, column level
python -m dataops stream run                    # Bronze -> Silver -> hourly events (available now)
python -m dataops stream reconcile              # Silver vs source, column level
python -m dataops publish                       # Silver -> warehouse cdc_pub (atomic)
dbt build --project-dir dbt --profiles-dir dbt --vars '{source_path: cdc}'
python -m dataops reconcile                     # source vs mart
python scripts/verify_cdc.py                    # CDC checks incl. connector restart
python scripts/verify_streaming.py              # lakehouse checks incl. SIGKILL + replay
scripts/airflow.sh standalone                   # Airflow UI on 127.0.0.1:8080
docker compose --profile full down              # keeps volumes; add -v to delete them
```

## Replication slot and WAL retention

- Slot `dataops_cdc` is created by Debezium on first start and is **not dropped** when the
  connector stops (`slot.drop.on.stop=false`), so a restart resumes from the last
  acknowledged LSN instead of re-snapshotting.
- While the connector is down the slot pins WAL. `max_slot_wal_keep_size=1024MB` bounds this
  in development: past it PostgreSQL invalidates the slot rather than filling the disk, and
  the connector then needs a new snapshot (`snapshot.mode=initial` with a new slot).
- Heartbeats (10 s) keep the connector alive, but the slot's confirmed position only advances
  when published tables change; an idle source leaves a small, stable lag (measured
  ~340 KB in the CDC verification). A `heartbeat.action.query` writing to a heartbeat table
  would advance it; not added because the demo source is never idle for long.
- To remove CDC completely: delete the connector, then `SELECT pg_drop_replication_slot('dataops_cdc')`.

## Lakehouse layout (`var/lake`, override with DATAOPS_LAKE_DIR)

| Path | Content | Partitioning | Written by |
|---|---|---|---|
| `bronze/cdc_events` | every Kafka record: topic, partition, offset, key, value, sha256 | `source_table` | Bronze stream |
| `silver/<table>` (11) | current state, contract-typed, `_lsn`, `_is_deleted`, provenance | none | Silver MERGE |
| `silver/_quarantine` | rejected records with reasons and raw value | none | Silver |
| `silver/_batch_log` | per micro-batch, per table counts | none | Silver |
| `silver/shipment_events_hourly` | event-time hourly counts (streaming, watermark) | none | events stream |
| `_checkpoints/{bronze,silver,events_hourly}` | stream progress and state (RocksDB) | | Spark |

Small files: demo volumes produce one file per micro-batch per partition; Silver tables are
MERGE targets with optimized writes. Compaction (`OPTIMIZE`) and `VACUUM` are not scheduled;
`VACUUM` must keep at least the default 7-day retention so time travel and replay of
recent versions keep working.

## Schema evolution

Silver tables have the v1 contract schema. Unknown extra source columns are ignored by the
typed projection (still in Bronze). A type change or a missing required value quarantines
the record with a reason; it is never coerced. Changing Silver's schema is a contract
version bump plus an explicit replay.

## Replay and backfill

- **Silver from Bronze**: `python -m dataops stream replay --confirm-rebuild-from-bronze`,
  then `stream run --jobs silver events`. Bronze is never deleted by any command.
- **Bronze from Kafka**: possible while Kafka retains the data (7 days), by removing the
  Bronze table and its checkpoint manually; not automated on purpose.
- **Late events**: the streaming hourly aggregate drops events later than the watermark;
  `jobs.recompute_hourly` recomputes from Silver, which includes them.

## Measured resource use

Recorded with `docker stats --no-stream` and `top` during verification runs on the 2 vCPU /
7.8 GiB build VM; see PROGRESS.md for the numbers of each run.
