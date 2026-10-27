-- Current-state views over the reliability metadata: the single place the API, dashboard
-- and metrics exporter (Milestone 5) read operational health from. Read-only for rel_reader.

CREATE VIEW reliability.v_last_successful_run AS
SELECT DISTINCT ON (pipeline_id) pipeline_id, run_id, ended_at, duration_s
FROM reliability.pipeline_runs WHERE status = 'success'
ORDER BY pipeline_id, ended_at DESC NULLS LAST;

CREATE VIEW reliability.v_pipeline_health AS
SELECT p.pipeline_id, p.system, p.criticality,
       count(r.*) FILTER (WHERE r.started_at > now() - interval '24 hours')       AS runs_24h,
       count(r.*) FILTER (WHERE r.status = 'failed'
                          AND r.started_at > now() - interval '24 hours')         AS failed_24h,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY r.duration_s)                  AS median_duration_s,
       max(r.ended_at) FILTER (WHERE r.status = 'success')                        AS last_success_at,
       (array_agg(r.status ORDER BY r.started_at DESC NULLS LAST))[1]             AS last_status
FROM reliability.pipeline_definitions p
LEFT JOIN reliability.pipeline_runs r USING (pipeline_id)
GROUP BY p.pipeline_id, p.system, p.criticality;

CREATE VIEW reliability.v_freshness_current AS
SELECT DISTINCT ON (dataset_id, measure) dataset_id, measure, lag_seconds, threshold_seconds,
       status, source_high_water, target_high_water, observed_at
FROM reliability.freshness_observations
ORDER BY dataset_id, measure, observed_at DESC;

CREATE VIEW reliability.v_consumer_lag_current AS
SELECT DISTINCT ON (dataset_id) dataset_id, metrics->>'topic' AS topic,
       (metrics->>'lag_messages')::bigint AS lag_messages,
       (metrics->>'oldest_unprocessed_age_s')::numeric AS oldest_unprocessed_age_s,
       observed_at
FROM reliability.stream_observations WHERE kind = 'kafka_lag'
ORDER BY dataset_id, observed_at DESC;

CREATE VIEW reliability.v_quality_failures_recent AS
SELECT result_id, check_id, check_type, dataset_id, column_name, status, failures, message,
       observed_at
FROM reliability.quality_check_results
WHERE status IN ('fail', 'error') AND observed_at > now() - interval '7 days';

CREATE VIEW reliability.v_quarantine_recent AS
SELECT dataset_id, sum(failures) AS quarantined_records, max(observed_at) AS last_seen
FROM reliability.quality_check_results
WHERE check_type = 'quarantine' AND observed_at > now() - interval '7 days'
GROUP BY dataset_id;

CREATE VIEW reliability.v_reconciliation_current AS
SELECT DISTINCT ON (check_id) check_id, dataset_id, comparison, status, compared_rows,
       mismatched_rows, observed_at
FROM reliability.reconciliation_results
ORDER BY check_id, observed_at DESC;

CREATE VIEW reliability.v_active_incidents AS
SELECT i.incident_id, i.title, i.severity, i.status, i.root_dataset_id, i.root_cause_status,
       i.detected_at, i.first_event_at, i.last_event_at,
       extract(epoch FROM (i.detected_at - i.first_event_at)) AS detection_delay_s,
       jsonb_array_length(i.impacted_datasets) AS impacted_count,
       (SELECT count(*) FROM reliability.incident_events e WHERE e.incident_id = i.incident_id) AS signals
FROM reliability.incidents i
WHERE i.status IN ('open', 'investigating', 'mitigating');
