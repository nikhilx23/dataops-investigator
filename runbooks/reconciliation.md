---
runbook_id: RB-004
title: Failed financial reconciliation
service: warehouse
incident_types: [reconciliation_failed]
version: 1.3
effective_date: 2026-03-01
owner: finance-analytics
---
# Failed financial reconciliation

## Symptoms
`revenue.daily.source_vs_mart` or `revenue.daily.ledger_vs_mart` reports mismatched dates, or
`inventory.position.ledger_vs_mart` reports mismatched positions. Financial mismatches are
severity 1 because they reach finance dashboards.

## Confirm the cause
Reconciliation failure is a symptom. Look upstream before touching the mart:
1. Were the marts rebuilt after the latest source changes? Skipped or failed dbt runs leave
   marts behind the source and every recent date mismatches.
2. Did a source schema change alter units (cents vs dollars)? Then captured or refunded
   amounts differ by a factor of about 100 on affected dates.
3. Late-arriving payments or refunds after the last incremental run shift amounts between
   dates: the totals agree but individual dates do not.
4. Duplicate business keys inflate totals: check uniqueness tests on staging models.

## Recovery
Rebuild only after the upstream cause is fixed: **rebuild the affected models**; if the
incremental lookback did not cover the changed dates, run a **bounded backfill** and a full
refresh of the affected facts.

## Verify
All reconciliation checks pass for the affected dates; booked revenue and net collected cash
equal the source and the expected-results ledger.
