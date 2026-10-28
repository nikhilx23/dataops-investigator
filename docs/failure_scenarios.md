# Failure scenario guide

Eight deterministic, reversible scenarios on synthetic data. Ground truth lives in code
(`src/dataops/scenarios/`) and this guide; the investigator's tools cannot read it.
Actual run results: [results/scenarios/README.md](results/scenarios/README.md).

Common commands (lightweight profile, demo databases from `.env`):

```bash
python -m dataops scenario list
python -m dataops scenario run <name> --actor "Your Name"   # inject → cycle → detect → investigate → recover → verify → clean up
python -m dataops scenario inject <name>                    # inject only
python -m dataops scenario cycle                            # one pipeline cycle: extract, dbt build, reconcile, collect, evaluate
python -m dataops scenario cleanup <name>
```

`run` approves every recovery step in the name passed with `--actor` (the person running the
demo); in normal operation a person approves each step in the dashboard or CLI.

| # | Name | Injection (seed / scope) | Expected detection | Expected impact | Known cause | Recovery procedure | Verification | Cleanup |
|---|---|---|---|---|---|---|---|---|
| 1 | `schema_change` (`scripts/demo_m3.py`) | `shop.payments.amount_cents` bigint → numeric(12,2) dollars; payments of the last 24 h touched | breaking schema observation; `stg_payments` build fails; 4 marts skipped; revenue reconciliation fails | 13 datasets, 3 dashboards | source type change | escalate to owner → owner reverts → `bounded_backfill` payments → `rebuild_models` stg_payments+ | dbt build passes; reconciliation passes; no remaining signals | `schema_change.revert()` |
| 2 | `duplicate_business_events` | 5 latest refunds duplicated with new ids (same payment, amount, time) | `assert_no_duplicate_refund_events` fails; refunds exceed capture; downstream skipped; reconciliation fails | payment facts, revenue mart, 3 dashboards | refunds service double-submitted | escalate to owner → owner removes duplicates → `rebuild_models` stg_refunds+ (full refresh) | as above | delete the 5 rows in source and their raw records (harness; batch cannot see deletes) |
| 3 | `late_arriving_payments` | 6 pending payments captured, committed 1 s after their previous version (behind the watermark) | revenue source↔mart reconciliation fails; row counts still pass | revenue mart, revenue dashboard | watermark-based extraction misses back-dated commits | `bounded_backfill` orders+payments over the commit window → `rebuild_models` stg_orders+, stg_payments+ (full refresh) | reconciliation passes | revert to pending with a fresh timestamp + full refresh |
| 4 | `streaming_interruption` (full profile) | stop the Spark consumer while the simulator produces changes | consumer lag on CDC topics + stale `cdc_pub` tables, grouped into one incident | 4 dashboards | consumer not running | start the stream jobs (available-now), publish | lag 0, all published tables fresh | none |
| 5 | `stale_upstream_source` | `REVOKE SELECT ON shop.orders FROM src_reader`; 25 orders touched | `batch_ingestion` orders task fails (permission denied); row-count reconciliation errors | orders models and dashboards | extraction role lost access | escalate to owner → owner restores the grant → `retry_failed_batch` → `rebuild_models` stg_orders+ | reconciliation passes; no remaining signals | `GRANT SELECT` back |
| 6 | `orphan_foreign_key` | 4 new orders (+items, payments) in the source; the next cycle skips extracting orders and payments | `relationships` test stg_order_items→stg_orders fails; row counts differ for orders | order facts and marts, 3 dashboards | parent table not extracted for the window | `bounded_backfill` orders+payments (last day) → `rebuild_models` stg_orders, stg_order_items, stg_payments | as above | remove the inserted rows (harness) + full refresh |
| 7 | `incomplete_publication` | 30 new orders; the orders loader is SIGKILLed after its first page | orders batch stuck `running`, nothing published; row counts differ; orphan tests fail | orders models, 3 dashboards | loader process died mid-batch | `retry_failed_batch` (marks the orphaned batch abandoned only if no loader session is live) → `rebuild_models` | as above | remove the inserted rows (harness) + full refresh |
| 8 | `refund_revenue_discrepancy` | 5 refunds halved, written 1 s after their previous version (behind the watermark) | revenue source↔mart fails on `refunded_cents` | revenue mart, finance dashboard | back-dated corrections missed by extraction | `bounded_backfill` refunds over the commit window → `rebuild_models` stg_refunds+ (full refresh) | reconciliation passes | restore amounts with a fresh timestamp |

What these runs taught (all fixed and recorded in PROGRESS):
- A correction written with a timestamp *older* than the version already loaded can never win
  "latest version" ordering; no backfill fixes that. Scenarios 3 and 8 now write the change
  1 s after the previous version, which is the realistic failure (behind the watermark, but
  newer than what was loaded). The recovery for the older-timestamp case would be a source fix.
- Moving data backwards (captured → pending) or deleting rows is invisible to incremental
  facts (they do not retract); cleanups end with a full refresh.
- A revoked SELECT made `information_schema` hide every column, which the schema collector
  first reported as a breaking change; unreadable tables are no longer observed.
