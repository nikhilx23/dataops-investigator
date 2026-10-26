-- Raw landing zone in the warehouse (owned by wh_loader).
--
-- Publication contract: rows in raw.source_records are visible to dbt ONLY through batches
-- whose raw.ingestion_batches.status = 'committed'. Record inserts, the status change to
-- 'committed' and the watermark advance happen in ONE transaction, so a batch is either
-- fully published or not at all. dbt staging models additionally filter on committed
-- batches as defense in depth (see dbt/models/staging/_raw_committed.sql).

CREATE TABLE raw.ingestion_batches (
    batch_id             uuid PRIMARY KEY,
    source_system        text        NOT NULL,          -- e.g. 'commerce_source'
    source_table         text        NOT NULL,
    window_start         timestamptz NOT NULL,          -- requested interval [start, end)
    window_end           timestamptz NOT NULL,
    overlap_seconds      integer     NOT NULL CHECK (overlap_seconds >= 0),
    extract_lower_bound  timestamptz NOT NULL,          -- window_start - overlap
    status               text        NOT NULL CHECK (status IN ('running','committed','failed','abandoned')),
    extracted_rows       integer,
    inserted_rows        integer,
    duplicate_rows       integer,
    pages                integer,
    content_checksum     text,                          -- sha256 over ordered record checksums
    max_source_updated_at timestamptz,
    run_id               text,                          -- orchestrator run id, if any
    correlation_id       text        NOT NULL,
    loader_version       text        NOT NULL,
    started_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at          timestamptz,
    error_message        text,
    CHECK (window_end > window_start)
);

-- At most one in-flight writer per source table (prevents overlapping publication scopes).
CREATE UNIQUE INDEX ingestion_batches_one_running
    ON raw.ingestion_batches (source_system, source_table) WHERE status = 'running';
CREATE INDEX ON raw.ingestion_batches (source_table, status, window_end);

CREATE TABLE raw.ingestion_watermarks (
    source_system    text        NOT NULL,
    source_table     text        NOT NULL,
    high_watermark   timestamptz NOT NULL,   -- window_end of the latest committed batch
    last_batch_id    uuid        NOT NULL REFERENCES raw.ingestion_batches,
    updated_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_system, source_table)
);

-- Append-only, immutable record versions. One row per distinct payload of a source key;
-- re-extracting an unchanged row (overlap windows, retries) is a no-op.
CREATE TABLE raw.source_records (
    record_id          text        PRIMARY KEY,   -- sha256(system|table|pk|payload_checksum)
    source_system      text        NOT NULL,
    source_table       text        NOT NULL,
    source_pk          text        NOT NULL,
    source_updated_at  timestamptz NOT NULL,
    payload            jsonb       NOT NULL,      -- full source row, unmodified
    payload_checksum   text        NOT NULL,      -- sha256 of canonical JSON payload
    batch_id           uuid        NOT NULL REFERENCES raw.ingestion_batches,
    ingested_at        timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON raw.source_records (source_table, source_pk, source_updated_at DESC);
CREATE INDEX ON raw.source_records (batch_id);
