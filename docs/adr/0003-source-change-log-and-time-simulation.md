# ADR 0003: Ordered product change log and a time-simulated source

Status: accepted (Milestone 2)

## Context
1. SCD Type 2 product history must keep **every** intermediate version. Periodic extraction
   of `shop.products` only sees the state at extraction time.
2. Loading the generator's final state in one shot makes incremental pipelines unrealistic:
   an order placed on day 1 and delivered on day 5 would first appear on day 5, so its
   payments (extracted on day 1) look like orphans. This was observed in a day-by-day test
   run (13 `stg_payments → stg_orders` relationship failures) before this decision.

## Decision
- The source keeps an ordered change log, `shop.product_versions`, written by an `AFTER
  INSERT OR UPDATE` trigger in the same transaction as each product change (no-op updates are
  skipped). `dim_product_history` is built from it; `fct_order_item` joins the version valid
  at order time.
- A simulator (`dataops simulate`) derives each row's state history from the generated data
  and applies all changes effective up to a simulated clock (`sim.clock`, not extracted).
  Replaying to the simulation end reproduces the final dataset exactly (unit-tested for the
  `tiny` and `small` profiles).
- The one-shot loader (`generate --load`) remains for fast reconciliation demos.

## Consequences
- Incremental extraction, incremental dbt models and (later) CDC see a source that evolves
  like an operational database, including intermediate states.
- During historical bulk load/replay the change-log trigger is disabled for that transaction
  and the generator supplies the log rows itself, so history is not rewritten with load times.
- Within one simulation step, multiple changes to the same row collapse to the latest state,
  exactly like a real source between two extractions; the product change log is unaffected
  because its rows are inserted, not updated.
