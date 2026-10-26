{{ config(materialized='incremental', unique_key='order_item_id',
          incremental_strategy='delete+insert', on_schema_change='fail') }}
-- Grain: one row per order line. Key: order_item_id.
-- Product attributes are joined point-in-time: the version valid when the order was placed.
-- change_cursor = latest source change to the line OR its order (status changes update the
-- order row only), so incremental runs pick up both.
with items as (
    select i.*, o.customer_id, o.placed_at, o.order_status,
           greatest(i.source_updated_at, o.source_updated_at) as change_cursor
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }} o on o.order_id = i.order_id
)
select
    items.order_item_id,
    items.order_id,
    items.line_number,
    items.customer_id,
    items.product_id,
    p.product_version_key,
    to_char(items.placed_at at time zone 'UTC', 'YYYYMMDD')::integer  as order_date_key,
    items.placed_at,
    items.order_status,
    items.order_status = 'cancelled'                                  as is_cancelled_order,
    items.quantity,
    items.unit_price_cents,
    (items.quantity::bigint * items.unit_price_cents)                 as gross_amount_cents,
    items.discount_cents,
    items.line_amount_cents                                           as net_amount_cents,
    items.change_cursor
from items
left join {{ ref('dim_product_history') }} p
  on p.product_id = items.product_id
 and items.placed_at >= p.valid_from
 and (p.valid_to is null or items.placed_at < p.valid_to)
{% if is_incremental() %}
where items.change_cursor >= {{ incremental_cutoff() }}
{% endif %}
