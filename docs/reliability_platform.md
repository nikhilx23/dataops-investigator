# Reliability platform

Everything here runs on real collected metadata; nothing is hard-coded. All data is synthetic.

## Metadata model (`reliability` schema)

| Area | Tables |
|---|---|
| Catalog | `datasets`, `dataset_contract_versions` |
| Execution | `pipeline_definitions`, `pipeline_runs`, `task_runs`, `collector_runs` |
| Validation | `quality_check_results`, `freshness_observations`, `schema_observations`, `reconciliation_results`, `stream_observations` |
| Lineage | `lineage_observations` (versioned), `lineage_edges` |
| Incidents | `incident_events`, `incidents`, `incident_status_history`, `incident_evidence` |
| Later milestones | `investigation_runs`, `recovery_actions` (created now, used in M4/M5) |
| Current state | views `v_pipeline_health`, `v_last_successful_run`, `v_freshness_current`, `v_consumer_lag_current`, `v_quality_failures_recent`, `v_quarantine_recent`, `v_reconciliation_current`, `v_active_incidents` |

Identifiers are stable and external where possible (`dbt:<invocation_id>`,
`airflow:<dag>:<run_id>`, `batch_ingestion:<correlation_id>`); evidence ids are
`ev:<table>:<id>` and always resolve to a stored row (tested).

## Collectors (`dataops collect`)

| Collector | Interface | Produces |
|---|---|---|
| dbt | `run_results.json`, `manifest.json` | runs, model tasks, test results, lineage, dataset registry |
| ingestion | loader manifests `raw.ingestion_batches`, `cdc_pub.publications` | runs and per-batch tasks |
| airflow | REST API v2 with JWT (`/auth/token`) | DAG runs, task instances incl. retries |
| spark | `StreamingQueryProgress` + Silver batch log, written by the jobs as JSON lines | throughput, watermark drops, quarantine counts |
| kafka | Kafka watermarks vs the Bronze checkpoint (Structured Streaming does not commit group offsets); Connect REST | lag in messages and age; connector state |
| freshness | source vs warehouse high-water marks (data time) and last load age (wall clock), **only for the active input path** (`DATAOPS_SOURCE_PATH`) | freshness observations |
| schema | `information_schema` vs contract v1 | schema observations: matches / compatible / breaking |

Idempotent (natural keys, verified: second collection adds nothing except time-series
freshness and lag observations). An unreachable system is `unavailable`, not a failure.

## Lineage (`dataops lineage impact|upstream|path|summary`)

Model-level only (no column-level lineage). 96 datasets, 107 edges: 29 extracted from the dbt
manifest (models, sources, exposures = dashboards) and 78 declared in `catalog/declared.yaml`
(source table -> raw / Kafka topic -> Bronze -> Silver -> published table -> staging). Shared
landing tables are split into per-table logical datasets so a problem in `shop.payments` does
not appear to affect inventory. Every hop of a path reports whether it was extracted or
declared. Cycle detection runs on every build (the current graph has none).

## Incident engine (`dataops incidents evaluate|list|show|close`)

Rules (v1): failed critical task, downstream model skipped by dbt after an upstream failure,
failing quality check (duplicates, orphans, contract quarantine, other), stale dataset,
breaking schema change, failed reconciliation (financial = sev1), consumer lag over a
duration, failed Debezium connector. Signals superseded by a later successful run, or not
repeated by the latest freshness collection, do not fire.

Correlation: signals within the window are clustered when their datasets are connected in
lineage, or when they share a group (one lagging consumer; one stalled warehouse input
path). The incident's root is the most upstream affected dataset and is always marked
**inferred**; with several unrelated roots joined only by a group, the incident is keyed by
the group and names no single root. One active incident per fingerprint; a later, more
upstream signal re-roots the incident and the change is recorded in its history.
Failure classes are kept distinct: pipeline_execution, data_quality, source_freshness,
reconciliation, schema_change, streaming.

## Verified scenarios

| Scenario | Result |
|---|---|
| Healthy pipeline | 0 events, 0 incidents |
| Breaking change `shop.payments.amount_cents` bigint -> numeric (lightweight) | 1 sev1 incident, root `commerce_source.shop.payments` (inferred), signals: breaking schema change, errored dbt tests on staging, 4 marts skipped, revenue reconciliation failed (30 dates); impact 13 datasets, 3 dashboards with paths; re-evaluation adds nothing; after revert + rebuild no new signals |
| Consumer stopped while 1,069 changes arrive (full profile) | 1 sev1 incident grouping 16 signals: lag on 9 topics (oldest 83 s, threshold 60 s) + 7 stale published tables; 4 dashboards; after processing + publication: lag 0, 12/12 fresh, no new signals |
