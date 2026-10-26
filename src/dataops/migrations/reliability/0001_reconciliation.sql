-- Reliability metadata, first slice: reconciliation results (Milestone 1).
-- The full metadata model (runs, quality, lineage, incidents, evidence) arrives in Milestone 3.

CREATE TABLE reliability.reconciliation_results (
    reconciliation_id   uuid PRIMARY KEY,
    check_id            text        NOT NULL,   -- stable identifier, e.g. 'revenue.daily.source_vs_mart'
    check_version       text        NOT NULL,
    dataset_id          text        NOT NULL,   -- e.g. 'warehouse.marts.mart_daily_revenue'
    comparison          text        NOT NULL,   -- e.g. 'source_vs_mart', 'ledger_vs_mart'
    scope               jsonb       NOT NULL,   -- date range / filters the check covered
    status              text        NOT NULL CHECK (status IN ('pass','fail','error')),
    compared_rows       integer     NOT NULL,
    mismatched_rows     integer     NOT NULL,
    left_totals         jsonb       NOT NULL,
    right_totals        jsonb       NOT NULL,
    mismatches          jsonb       NOT NULL,   -- bounded sample of differing keys
    query_fingerprint   text        NOT NULL,   -- sha256 of the SQL used on each side
    correlation_id      text        NOT NULL,
    observed_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    error_message       text
);
CREATE INDEX ON reliability.reconciliation_results (check_id, observed_at DESC);
