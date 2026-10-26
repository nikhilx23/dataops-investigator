# ADR 0002: Generic JSONB raw landing with a transactional publication boundary

Status: accepted (Milestone 1)

## Context
Batch ingestion must preserve raw payloads, be idempotent across overlapping windows and
retries, and never expose a partially loaded batch to dbt.

## Decision
- One append-only table, `raw.source_records`, stores the full source row as JSONB with a
  content checksum and a content-addressed `record_id`.
- `raw.ingestion_batches` is the manifest. Records, the `committed` status and the watermark
  advance are written in one PostgreSQL transaction. dbt staging additionally joins to
  committed batches.
- Typed columns are produced in dbt staging, not at load time.

## Alternatives considered
- One raw table per source table with typed columns: earlier type errors, but a schema
  change in the source would fail the load before the raw payload is preserved, which
  defeats the incident-investigation goal (we want the bad payload kept as evidence).
- Staging files plus a separate "publish" step: more moving parts for no extra guarantee
  inside a single PostgreSQL instance.

## Consequences
- Source schema changes do not break landing; they surface in dbt staging/tests and in
  schema contracts (Milestone 3), with the offending payloads available.
- JSONB costs storage and cast work in staging; acceptable for demo volumes and measured in
  benchmarks (Milestone 6).
