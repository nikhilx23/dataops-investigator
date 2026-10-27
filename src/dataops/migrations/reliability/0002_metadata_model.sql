-- Reliability and investigation metadata (Milestone 3).
--
-- Conventions:
--   * Natural, stable identifiers (text) for things that exist outside this database
--     (dataset_id 'warehouse.marts.mart_daily_revenue', pipeline_id 'dbt', run ids issued by
--     Airflow/dbt/the loader); uuids for records created here.
--   * Every observation carries observed_at and, where it exists, the external run/task id,
--     artifact checksum and check/query identifier needed to reproduce it.
--   * Collectors upsert on natural keys, so re-collecting the same facts changes nothing.
--   * No passwords or connection strings are stored; free text goes through redaction.

-- ------------------------------------------------------------------ catalog
CREATE TABLE reliability.datasets (
    dataset_id        text PRIMARY KEY,          -- system.schema.object
    system            text        NOT NULL,      -- commerce_source, kafka, lakehouse, warehouse, bi
    kind              text        NOT NULL,      -- source_table, topic, delta_table, raw_table, model, seed, test, exposure
    display_name      text        NOT NULL,
    owner             text,
    description       text,
    criticality       text        NOT NULL DEFAULT 'standard'
                      CHECK (criticality IN ('critical','standard','low')),
    freshness_hours   integer,
    contract_id       text,
    registered_by     text        NOT NULL,      -- contract, dbt_manifest, declared
    first_seen_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE reliability.dataset_contract_versions (
    contract_id       text        NOT NULL,      -- e.g. 'shop'
    version           integer     NOT NULL,
    dataset_id        text        NOT NULL REFERENCES reliability.datasets,
    key_column        text        NOT NULL,
    spec              jsonb       NOT NULL,      -- columns, types, required, allowed, units
    spec_checksum     text        NOT NULL,
    source_path       text        NOT NULL,      -- file in the repository
    registered_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (contract_id, version, dataset_id)
);

-- ------------------------------------------------------------- execution
CREATE TABLE reliability.pipeline_definitions (
    pipeline_id       text PRIMARY KEY,          -- batch_ingestion, dbt, airflow:<dag_id>, spark:<query>
    system            text        NOT NULL,      -- loader, dbt, airflow, spark, debezium
    description       text,
    criticality       text        NOT NULL DEFAULT 'standard'
                      CHECK (criticality IN ('critical','standard','low')),
    owner             text,
    registered_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE reliability.pipeline_runs (
    run_id            text PRIMARY KEY,          -- '<system>:<external id>'
    pipeline_id       text        NOT NULL REFERENCES reliability.pipeline_definitions,
    external_run_id   text        NOT NULL,
    status            text        NOT NULL CHECK (status IN
                          ('queued','running','success','failed','partial','skipped','unknown')),
    started_at        timestamptz,
    ended_at          timestamptz,
    duration_s        numeric,
    trigger           text,                      -- scheduled, manual, asset, cli
    correlation_id    text,
    artifact_checksum text,                      -- e.g. sha256 of dbt run_results.json
    details           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    collected_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON reliability.pipeline_runs (pipeline_id, started_at DESC);
CREATE INDEX ON reliability.pipeline_runs (status, started_at DESC);

CREATE TABLE reliability.task_runs (
    task_run_id       text PRIMARY KEY,          -- '<run_id>/<task id>[/<try>]'
    run_id            text        NOT NULL REFERENCES reliability.pipeline_runs ON DELETE CASCADE,
    task_id           text        NOT NULL,      -- dbt unique_id, Airflow task_id, table name
    dataset_id        text,                      -- dataset the task produces, when known
    status            text        NOT NULL CHECK (status IN
                          ('queued','running','success','failed','skipped','upstream_failed','unknown')),
    try_number        integer     NOT NULL DEFAULT 1,
    started_at        timestamptz,
    ended_at          timestamptz,
    duration_s        numeric,
    rows_affected     bigint,
    message           text,                      -- redacted, truncated
    details           jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX ON reliability.task_runs (run_id);
CREATE INDEX ON reliability.task_runs (dataset_id, started_at DESC);
CREATE INDEX ON reliability.task_runs (status) WHERE status = 'failed';

-- -------------------------------------------------------------- quality
CREATE TABLE reliability.quality_check_results (
    result_id         text PRIMARY KEY,          -- '<run_id>/<check_id>'
    check_id          text        NOT NULL,      -- dbt test unique_id or 'contract:<...>'
    check_type        text        NOT NULL,      -- unique, not_null, relationships, accepted_values, singular, contract, volume, quarantine
    dataset_id        text,
    column_name       text,
    run_id            text,
    status            text        NOT NULL CHECK (status IN ('pass','warn','fail','error','skipped')),
    failures          bigint,
    message           text,
    query_fingerprint text,
    observed_at       timestamptz NOT NULL,
    details           jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX ON reliability.quality_check_results (dataset_id, observed_at DESC);
CREATE INDEX ON reliability.quality_check_results (status, observed_at DESC);

CREATE TABLE reliability.freshness_observations (
    observation_id    uuid PRIMARY KEY,
    dataset_id        text        NOT NULL,
    measure           text        NOT NULL CHECK (measure IN ('data_lag','load_age')),
    -- data_lag: source high-water mark minus target high-water mark, in data time
    -- load_age: wall clock minus the last successful load/publication of the dataset
    lag_seconds       numeric,
    threshold_seconds numeric,
    status            text        NOT NULL CHECK (status IN ('fresh','stale','unknown')),
    source_high_water timestamptz,
    target_high_water timestamptz,
    query_fingerprint text        NOT NULL,
    observed_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON reliability.freshness_observations (dataset_id, observed_at DESC);

CREATE TABLE reliability.schema_observations (
    observation_id    uuid PRIMARY KEY,
    dataset_id        text        NOT NULL,
    columns           jsonb       NOT NULL,      -- [{name, type, nullable}] as observed
    schema_checksum   text        NOT NULL,
    previous_checksum text,
    changed           boolean     NOT NULL,
    contract_id       text,
    contract_version  integer,
    compatibility     text        NOT NULL CHECK (compatibility IN
                          ('matches_contract','compatible','breaking','no_contract')),
    differences       jsonb       NOT NULL DEFAULT '[]'::jsonb,
    observed_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON reliability.schema_observations (dataset_id, observed_at DESC);

CREATE TABLE reliability.stream_observations (
    observation_id    text PRIMARY KEY,          -- '<kind>:<scope>:<id>'
    kind              text        NOT NULL,      -- spark_progress, kafka_lag, connector_status, quarantine
    dataset_id        text,
    metrics           jsonb       NOT NULL,
    observed_at       timestamptz NOT NULL
);
CREATE INDEX ON reliability.stream_observations (kind, observed_at DESC);

-- -------------------------------------------------------------- lineage
CREATE TABLE reliability.lineage_observations (
    lineage_version   uuid PRIMARY KEY,
    sources           jsonb       NOT NULL,      -- {dbt_manifest: checksum, declared: checksum}
    edge_count        integer     NOT NULL,
    graph_checksum    text        NOT NULL,
    observed_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE reliability.lineage_edges (
    lineage_version   uuid        NOT NULL REFERENCES reliability.lineage_observations ON DELETE CASCADE,
    upstream_id       text        NOT NULL,
    downstream_id     text        NOT NULL,
    extraction        text        NOT NULL CHECK (extraction IN ('dbt_manifest','declared')),
    relationship      text        NOT NULL,      -- depends_on, ingests, captures, publishes, feeds_dashboard
    PRIMARY KEY (lineage_version, upstream_id, downstream_id)
);
CREATE INDEX ON reliability.lineage_edges (lineage_version, downstream_id);

-- ------------------------------------------------------------- incidents
CREATE TABLE reliability.incident_events (
    event_id          text PRIMARY KEY,          -- deterministic: '<rule_id>:<subject>:<evidence ref>'
    rule_id           text        NOT NULL,
    rule_version      text        NOT NULL,
    dataset_id        text,
    severity          text        NOT NULL CHECK (severity IN ('sev1','sev2','sev3')),
    failure_class     text        NOT NULL CHECK (failure_class IN
                          ('pipeline_execution','data_quality','source_freshness','reconciliation',
                           'schema_change','streaming')),
    summary           text        NOT NULL,
    evidence_ref      jsonb       NOT NULL,      -- {table, id} of the observation that fired
    observed_at       timestamptz NOT NULL,
    incident_id       uuid,
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON reliability.incident_events (incident_id);
CREATE INDEX ON reliability.incident_events (observed_at DESC);

CREATE TABLE reliability.incidents (
    incident_id          uuid PRIMARY KEY,
    fingerprint          text        NOT NULL,   -- dedup key of the root (rule family + root dataset)
    title                text        NOT NULL,
    severity             text        NOT NULL CHECK (severity IN ('sev1','sev2','sev3')),
    status               text        NOT NULL CHECK (status IN
                             ('open','investigating','mitigating','resolved','closed')),
    root_dataset_id      text,
    root_cause_status    text        NOT NULL DEFAULT 'inferred'
                         CHECK (root_cause_status IN ('unknown','inferred','confirmed')),
    root_cause_summary   text,
    impacted_datasets    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    lineage_version      uuid,
    detected_at          timestamptz NOT NULL,
    first_event_at       timestamptz NOT NULL,
    last_event_at        timestamptz NOT NULL,
    investigation_started_at timestamptz,
    recovery_started_at  timestamptz,
    verified_at          timestamptz,
    resolved_at          timestamptz,
    updated_at           timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- One active incident per fingerprint (deduplication).
CREATE UNIQUE INDEX incidents_one_active_per_fingerprint
    ON reliability.incidents (fingerprint) WHERE status IN ('open','investigating','mitigating');

CREATE TABLE reliability.incident_status_history (
    incident_id   uuid        NOT NULL REFERENCES reliability.incidents ON DELETE CASCADE,
    from_status   text,
    to_status     text        NOT NULL,
    reason        text        NOT NULL,
    actor         text        NOT NULL,          -- incident_engine, recovery_service, <user>
    changed_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON reliability.incident_status_history (incident_id, changed_at);

CREATE TABLE reliability.incident_evidence (
    evidence_id      text PRIMARY KEY,           -- 'ev:<source table>:<source id>' (stable, citable)
    incident_id      uuid        NOT NULL REFERENCES reliability.incidents ON DELETE CASCADE,
    evidence_type    text        NOT NULL,       -- task_run, quality_check, schema_observation, freshness, reconciliation, stream, lineage_path
    source_table     text        NOT NULL,
    source_id        text        NOT NULL,
    dataset_id       text,
    run_id           text,
    task_run_id      text,
    check_id         text,
    artifact_checksum text,
    observed_at      timestamptz NOT NULL,
    summary          text        NOT NULL,
    attached_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (incident_id, source_table, source_id)
);

-- Created now so later milestones extend rather than redefine the model.
CREATE TABLE reliability.investigation_runs (
    investigation_id uuid PRIMARY KEY,
    incident_id      uuid        NOT NULL REFERENCES reliability.incidents,
    mode             text        NOT NULL CHECK (mode IN ('model','fixture')),
    model            text,
    prompt_version   text        NOT NULL,
    status           text        NOT NULL CHECK (status IN ('running','completed','abstained','failed')),
    tool_calls       jsonb       NOT NULL DEFAULT '[]'::jsonb,
    output           jsonb,
    input_tokens     integer,
    output_tokens    integer,
    latency_ms       integer,
    error            text,
    started_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at      timestamptz
);

CREATE TABLE reliability.recovery_actions (
    action_id        uuid PRIMARY KEY,
    incident_id      uuid        REFERENCES reliability.incidents,
    action_type      text        NOT NULL,
    scope            jsonb       NOT NULL,
    status           text        NOT NULL CHECK (status IN
                         ('proposed','approved','rejected','running','succeeded','failed','verified')),
    preconditions    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    dry_run_output   jsonb,
    approved_by      text,
    approved_at      timestamptz,
    started_at       timestamptz,
    finished_at      timestamptz,
    verification     jsonb,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE reliability.collector_runs (
    collector_run_id uuid PRIMARY KEY,
    collector        text        NOT NULL,
    status           text        NOT NULL CHECK (status IN ('success','failed','unavailable')),
    records          integer     NOT NULL DEFAULT 0,
    message          text,
    started_at       timestamptz NOT NULL,
    finished_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON reliability.collector_runs (collector, started_at DESC);

-- Link reconciliation results to datasets already referenced by dataset_id (text).
CREATE INDEX ON reliability.reconciliation_results (status, observed_at DESC);
