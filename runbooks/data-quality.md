---
runbook_id: RB-005
title: Duplicate business keys, orphan records and contract quarantine
service: warehouse
incident_types: [duplicate_business_keys, orphan_foreign_keys, contract_quarantine, quality_check_failed]
version: 1.0
effective_date: 2026-03-01
owner: analytics-engineering
---
# Duplicate business keys, orphan records and contract quarantine

## Symptoms
A dbt `unique` test fails (duplicate business keys), a `relationships` test fails (orphan
foreign keys), or the Silver job quarantines records that violate the source contract.

## Confirm the cause
- Duplicates: distinguish **transport duplicates** (the same change delivered twice; same key
  and LSN, harmless after de-duplication) from **business duplicates** (two different records
  with the same business key; a source or modeling defect).
- Orphans: compare event times. A child that arrives before its parent within one batch window
  is a timing artifact and clears on the next run; a child whose parent never exists is a
  source defect.
- Quarantine: read the quarantine reasons; they name the column and the violated rule.

## Recovery
- Business duplicates: fix at the source or in the staging model's de-duplication rule, then
  **rebuild the affected models**.
- Quarantined records: **reprocess quarantined records** only after the contract or the
  source is corrected. Never edit quarantined values by hand.

## Verify
The failing tests pass on the next build; quarantine counts stop growing.
