---
runbook_id: RB-003
title: Streaming consumer lag and CDC connector failures
service: kafka
incident_types: [consumer_lag, connector_failed, stale_dataset]
version: 1.0
effective_date: 2026-03-01
owner: data-platform
---
# Streaming consumer lag and CDC connector failures

## Symptoms
`kafka_lag` observations show unprocessed messages on CDC topics with the oldest message older
than the threshold. Lag on many topics at once points at the Bronze consumer, not at a topic.
A Debezium connector or task in state `FAILED` stops new changes from reaching Kafka at all;
then lag is zero but published data is stale.

## Confirm the cause
1. Lag present: check whether the streaming job is running and when it last made progress
   (Spark progress observations). A job that is not running explains lag on every topic.
2. Lag absent but data stale: check the connector status and its task trace.
3. Replication slot: a failed connector keeps the slot inactive; WAL accumulates up to
   `max_slot_wal_keep_size`, after which the slot is invalidated and a new snapshot is needed.

## Recovery
- Consumer stopped: start the Bronze/Silver/events jobs (available-now), then publish.
- Connector failed: fix the cause in the trace, then restart the connector task. Do not drop
  or recreate the replication slot unless it was invalidated; that forces a full snapshot.
- Never delete checkpoints to "unstick" a stream; replay from Bronze is the supported path.

## Verify
Lag returns to zero on all topics, Silver reconciles with the source, publication is fresh.
