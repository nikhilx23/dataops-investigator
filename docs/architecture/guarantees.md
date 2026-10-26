# Boundary guarantees

What each boundary actually guarantees, as implemented today. Boundaries that belong to
later milestones are listed as **not implemented** rather than described aspirationally.

## Source → warehouse raw (batch ingestion, `dataops.ingestion.batch`) — implemented, tested

| Property | Guarantee | How |
|---|---|---|
| Snapshot consistency | All pages of one batch see one source snapshot | single `REPEATABLE READ READ ONLY` transaction |
| Ordering / pagination | Stable, no skipped or repeated rows within a batch | keyset on `(updated_at, pk)` |
| Late commits | Changes committed up to `overlap` (default 15 min) after a window closed are still captured | next window reads from `window_start − overlap` |
| Duplicates | Re-reading a row (overlap, retry, rerun) never creates a second copy | `record_id = sha256(system│table│pk│payload_checksum)`, `ON CONFLICT DO NOTHING` |
| Atomic publication | A batch is visible entirely or not at all | records + status `committed` + watermark in one transaction; dbt reads committed batches only |
| Single writer | Two loaders cannot publish the same table concurrently | partial unique index on `status='running'` |
| Crash recovery | A crashed batch publishes nothing; its `running` row expires after 30 min and the window can be rerun | lease → `abandoned`; reruns are idempotent |
| Retries | Transient connection errors retry up to 3 times with exponential backoff; each retry is a new batch | `ingest_table(attempts=3)` |
| **Not guaranteed** | Hard deletes are invisible to batch extraction. A row changed several times between two extractions lands as its latest version only. Commits delayed longer than the overlap are missed until a wider re-run (backfill). | CDC (full profile) addresses deletes and intermediate versions |

Delivery semantics into raw: **at-least-once extraction, effectively-once storage** (content
de-duplication). This is not end-to-end exactly-once: the source can change between windows.

## Warehouse raw → dbt staging — implemented, tested

- Staging reads only `committed` batches (join on `raw.ingestion_batches`).
- Current version of a key: latest `source_updated_at`, ties broken by `ingested_at`, then
  `record_id` (deterministic).

## dbt → reconciliation — implemented, tested

Three independent comparisons, persisted to `reliability.reconciliation_results`:
source vs raw row counts, source SQL vs mart, generator ledger vs mart. A failed or errored
check is recorded as `fail`/`error`, never as a pass.

## Not implemented yet

CDC (Debezium → Kafka), streaming (Spark Structured Streaming), Bronze/Silver Delta tables,
Silver → warehouse publication, Airflow orchestration. Their guarantees will be documented
here when they exist.
