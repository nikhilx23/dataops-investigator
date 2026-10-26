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

## Product history (SCD2) — implemented, tested

- Every product attribute change is appended to `shop.product_versions` in the same
  transaction as the change; no-op updates add nothing. The batch path therefore receives
  every intermediate version, in order (tested: two price changes between two extractions
  both appear in `dim_product_history`).
- `dim_product_history` intervals are `[valid_from, valid_to)`, contiguous, gap-free in
  version numbers, with exactly one current version per product (singular test).
- `fct_order_item` uses the version valid at `placed_at`; a test asserts the line price equals
  that version's list price (437 of 3,635 demo lines were sold at a price that is no longer
  current, so the point-in-time join is load-bearing).

## Incremental facts — implemented, tested

- `delete+insert` on the fact key for every row whose change cursor is within
  `incremental_lookback_days` (default 3) of the latest processed cursor. The cursor of an
  order line includes its order row, so status-only changes propagate.
- Equivalence: after day-by-day incremental runs (with a late update in between), each fact
  and mart fingerprint equals the fingerprint after `--full-refresh` (integration test and
  `scripts/demo_m2.py`).
- **Not guaranteed**: a change whose cursor is older than the lookback window when it lands
  (e.g. a back-dated source correction) is not picked up incrementally; it needs a bounded
  backfill or full refresh. Reconciliation is what detects this case.

## Source -> Kafka (Debezium CDC, full profile) — implemented, verified

| Property | Guarantee | Evidence |
|---|---|---|
| Initial snapshot | consistent with the slot's starting LSN; every row once as `op=r` | 2,539/2,539 rows, column-level match (`scripts/verify_cdc.py`) |
| Inserts, updates, deletes | `c`, `u`, `d` + tombstone, in commit (LSN) order per table | probe row: `c,u,u,d,tombstone`, LSNs strictly increasing |
| Ordering | one partition per table topic; per key, the highest LSN wins | `dataops.cdc.stream.materialize` |
| Restart / resume | slot kept on stop; resumes from the last acknowledged LSN | 936 changes made while Connect was stopped all delivered; 0 mismatches |
| Duplicates | at-least-once after restarts: *transport* duplicates share (key, LSN, op) and are dropped; equal business content with a new LSN is a real change | 0 transport duplicates observed in the verified restart |
| Delete images | only the primary key is real (REPLICA IDENTITY DEFAULT); other fields are Debezium placeholders | caught by verification, see PROGRESS |
| **Not guaranteed** | DDL is not captured; if the slot is invalidated (`max_slot_wal_keep_size`) a new snapshot is required | docs/full_profile.md |

## Kafka -> Bronze -> Silver (Spark on Delta, full profile) — implemented, verified

| Boundary | Guarantee | Evidence |
|---|---|---|
| Kafka -> Bronze | exactly once per Kafka offset (checkpointed source + idempotent Delta sink); immutable append | 6,772 Bronze rows = 6,772 distinct offsets = Kafka end offsets, **including after SIGKILL of the running job** |
| Bronze -> Silver | effectively once: LSN-guarded MERGE (only strictly newer changes apply), idempotent quarantine/log appends (`txnAppId/txnVersion`) | Silver equals source column-by-column before and after the kill (6,013 rows) |
| Validation | contract violations quarantined with reasons, never coerced | Spark tests: decimal amount, missing required, bad enum/min/timestamp/boolean |
| Deletes | soft delete: `_is_deleted`, last known values kept; not published | probe customer present as deleted, absent from publication |
| Replay | Silver is a deterministic function of Bronze | digest identical before/after `stream replay` |
| Event time | hourly counts with watermark (default 6 h), business de-duplication within the watermark, RocksDB state bounded by the watermark | 10 rows dropped as late in incremental runs; correction path recomputes from Silver |
| **Not guaranteed** | end-to-end exactly-once from the source; cross-table consistency of a publication while streaming runs (each table is one Delta version) | docs/adr/0004 |

## Silver -> warehouse (`cdc_pub`) — implemented, verified

All-or-nothing publication in one PostgreSQL transaction with a manifest (Silver Delta
versions, row counts, checksum) under an advisory lock; dbt with `source_path=cdc` reads
only the latest committed publication. Verified: 6,013 published rows = 6,013 live source
rows; `mart_daily_revenue` built from the CDC path reconciles with the source (0 mismatches).

## Orchestration (Airflow 3.3.2) — implemented, verified

Bounded tasks only; `max_active_runs=1` and a one-slot `warehouse_writer` pool on every
writing DAG. Verified under the real scheduler: publication -> (asset) dbt build -> (asset)
reconciliation, all `success`; a manual publication run queued until the scheduled one
finished; the backfill DAG ran explicit windows from its parameters.
