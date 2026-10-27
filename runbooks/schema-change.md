---
runbook_id: RB-001
title: Breaking source schema change
service: commerce_source
incident_types: [breaking_schema_change, failed_critical_task, quality_check_failed, reconciliation_failed, downstream_model_skipped]
version: 1.2
effective_date: 2026-03-01
owner: data-platform
---
# Breaking source schema change

## Symptoms
A schema observation for a source table reports `compatibility = breaking` against its data
contract (column removed, type changed, or a required column became nullable). Downstream,
dbt staging casts fail, tests error instead of failing, marts that depend on the table are
skipped, and revenue or inventory reconciliation reports mismatched dates. On the CDC path
the Silver job quarantines the affected records with a reason such as
`amount_cents: expected integer`.

## Confirm the cause
1. Read the latest schema observation for the table and compare `differences` with the
   contract file `contracts/source/shop.v1.yaml`.
2. Check that the first failing dbt node is a staging model of the same table and that its
   error message is a cast or type error.
3. Look at the time of the schema observation: it should precede the first failing run.
4. If the type changed, compare a few raw payload values before and after the change. A
   switch from integer cents to decimal dollars shows values like `12.99` where `1299` was.

## Impact
Every model downstream of the table's staging model holds data from its last successful
build. Dashboards fed by those marts are stale or, for revenue, wrong once the new unit
reaches them. Use the lineage impact of the source table to list them.

## Recovery
Never coerce the new values in the warehouse. Options, in order of preference:
1. Ask the source owner to restore the contracted type (or ship a new contract version with
   an explicit migration). Escalate with the schema diff as evidence.
2. After the source is corrected, run a **bounded backfill** of the affected table over the
   window since the change, then **rebuild the affected models**.
3. Records quarantined on the CDC path are **reprocessed** only after the contract or the
   source is fixed.
Preconditions: the source schema matches the contract again; no other writer is running.

## Verify
Source-to-mart and ledger reconciliation pass for the affected dates; the latest schema
observation is `matches_contract`; dbt build succeeds with no skipped marts.
