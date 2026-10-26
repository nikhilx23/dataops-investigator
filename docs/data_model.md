# Warehouse data model

Grains, keys and measures are defined here **before** the models. All money in USD cents,
all dates UTC. Rules referenced: [business_rules.md](business_rules.md).

## Layers

| Layer | Schema | Materialization | Owner |
|---|---|---|---|
| raw landing | `raw` | tables (append-only) | loader (`wh_loader`) |
| staging | `staging` | views; current version per source key from committed batches | dbt |
| intermediate | `intermediate` | views | dbt |
| dimensions / facts / marts | `marts` | tables; facts incremental | dbt |

## Dimensions

| Model | Grain | Key | Type | Notes |
|---|---|---|---|---|
| `dim_date` | calendar date | `date_key` (yyyymmdd int) | static | 2020-01-01 … 2027-12-31 |
| `dim_customer` | customer | `customer_id` | SCD1 (current attributes) | |
| `dim_warehouse` | warehouse | `warehouse_id` | SCD1 | |
| `dim_product_history` | product version | `product_version_key` = `product_id`-`version` | **SCD2** | intervals `[valid_from, valid_to)`; built from the ordered source change log `shop.product_versions`, so every intermediate version is kept even when several changes happen between two extractions |

## Facts

| Model | Grain | Key | Measures | Date attribution | Incremental |
|---|---|---|---|---|---|
| `fct_order_item` | order line | `order_item_id` | quantity, gross, discount, net line amount | order placed date | yes, `delete+insert` on key with lookback |
| `fct_payment_transaction` | money movement (one capture **or** one refund) | `transaction_id` (`capture-<payment_id>` / `refund-<refund_id>`) | signed amount (+capture, −refund) | movement date | yes |
| `fct_inventory_movement` | stock movement | `movement_id` | on_hand_delta, allocated_delta | occurred date | yes |
| `fct_shipment_event` | shipment event | `shipment_event_id` | arrival lag seconds | event date (event time) | yes |

`fct_order_item` joins the product version valid **at order time** (point-in-time join on
`placed_at`), so historical lines keep the attributes that applied when they were sold.

## Marts

| Model | Grain | Key |
|---|---|---|
| `mart_daily_revenue` | UTC date | `revenue_date` |
| `mart_order_fulfillment` | order | `order_id` |
| `mart_inventory_position` | product × warehouse | (`product_id`, `warehouse_id`) |
| `mart_payment_reconciliation` | order | `order_id` |

## Update and deletion policy

- Source rows are updated in place; the warehouse keeps every captured version in `raw` and
  exposes the latest in staging. Incremental facts re-process keys whose source version
  changed within a lookback window (default 3 days of `source_updated_at`) and replace them
  (`delete+insert`), so late updates are applied, not duplicated.
- Hard deletes: the operational contract for this source is **no hard deletes** of orders,
  payments, refunds, movements or shipment events (cancellations and refunds are state
  changes). Batch extraction cannot observe deletes; CDC (full profile) carries tombstones.
- Full-refresh vs incremental equivalence is checked by fingerprinting each fact after
  windowed incremental runs and again after `--full-refresh`.

## Avoiding double counting

Each measure is aggregated at its own fact grain and date attribution. Line amounts are never
summed after a join that fans out (order → payments, payment → refunds, order → shipment
events). Cross-model tests assert that `fct_payment_transaction` and `mart_daily_revenue`
agree on net collected cash, and that `fct_order_item` and `mart_daily_revenue` agree on
booked revenue.
