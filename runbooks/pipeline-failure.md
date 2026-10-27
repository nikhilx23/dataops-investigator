---
runbook_id: RB-006
title: Failed critical pipeline task
service: orchestration
incident_types: [failed_critical_task, downstream_model_skipped]
version: 1.1
effective_date: 2026-03-01
owner: data-platform
---
# Failed critical pipeline task

## Symptoms
A task of a critical pipeline (batch ingestion, dbt, Silver publication) failed and the run
failed. In dbt, models downstream of a failed node or failed test are skipped and keep their
previous data.

## Confirm the cause
1. Read the task message. Connection errors and timeouts are transient; type and cast errors
   point at data or schema problems (see RB-001); permission errors point at role changes.
2. Check whether a retry already succeeded: a later successful run of the same task
   supersedes the failure.
3. Check for overlapping writers: the lakehouse lock and the ingestion single-writer index
   reject a second writer with an explicit message.

## Recovery
- Transient errors: **retry the failed batch** (ingestion) or rerun the task.
- Data or schema errors: fix the cause first, then **rebuild the affected models**.

## Verify
The next run of the pipeline succeeds and no model is skipped.
