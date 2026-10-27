---
runbook_id: RB-002
title: Stale dataset or stalled input path
service: warehouse
incident_types: [stale_dataset]
version: 1.1
effective_date: 2026-03-01
owner: data-platform
---
# Stale dataset or stalled input path

## Symptoms
Freshness observations report `data_lag` above the contract threshold (source high-water mark
ahead of the warehouse input) or `load_age` above the expected load interval. When many
tables of one input path are stale at once (all `warehouse.raw.*` or all
`warehouse.cdc_pub.*`), the path itself has stopped, not the individual tables.

## Confirm the cause
1. Compare `load_age` of the path with its schedule: batch extraction runs daily, Silver
   publication every 30 minutes.
2. Check the most recent pipeline runs of the path (`batch_ingestion`, `silver_publication`,
   `spark:*`) for failures or for no runs at all.
3. On the CDC path, check consumer lag: messages waiting in Kafka mean the consumer stopped;
   no lag but stale publication means publication stopped.
4. A single stale table on a healthy path usually means the source table itself received no
   changes; confirm with the source high-water mark before acting.

## Recovery
- Batch path: **retry the failed batch** or run a **bounded backfill** of the missing window.
- CDC path: restart the stream processing (available-now run), then publish Silver.
Preconditions: the lakehouse lock is free; Kafka still retains the unprocessed offsets.

## Verify
New freshness observations are `fresh` for every table of the path and the lag is below the
threshold.
