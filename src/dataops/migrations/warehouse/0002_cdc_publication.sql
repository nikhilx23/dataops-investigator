-- Publication target for curated Silver data (full profile).
--
-- Contract: dbt reads cdc_pub.records only for the publication recorded as the latest
-- 'committed' row in cdc_pub.publications. The publisher replaces the records and inserts
-- the manifest in ONE transaction, so readers see either the previous publication or the
-- new one, never a mix. Silver Delta table versions are recorded for provenance.
-- The schema itself is created by infra/postgres/bootstrap.sql (owner wh_loader).

CREATE TABLE cdc_pub.publications (
    publication_id    uuid PRIMARY KEY,
    status            text        NOT NULL CHECK (status IN ('committed')),
    silver_versions   jsonb       NOT NULL,   -- {table: delta version}
    row_counts        jsonb       NOT NULL,   -- {table: live rows published}
    content_checksum  text        NOT NULL,
    correlation_id    text        NOT NULL,
    publisher_version text        NOT NULL,
    started_at        timestamptz NOT NULL,
    committed_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Same shape as the columns dbt staging reads from raw.source_records.
CREATE TABLE cdc_pub.records (
    source_table       text        NOT NULL,
    source_pk          text        NOT NULL,
    source_updated_at  timestamptz NOT NULL,
    payload            jsonb       NOT NULL,
    record_id          text        NOT NULL,   -- source_table:pk@lsn
    publication_id     uuid        NOT NULL REFERENCES cdc_pub.publications,
    source_lsn         bigint      NOT NULL,
    PRIMARY KEY (source_table, source_pk)
);
