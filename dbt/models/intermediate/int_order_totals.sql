-- Grain: one row per order. Key: order_id. Order total = sum of its line amounts.
select
    order_id,
    count(*)                    as line_count,
    sum(quantity)               as units,
    sum(line_amount_cents)      as order_total_cents,
    max(source_updated_at)      as items_source_updated_at
from {{ ref('stg_order_items') }}
group by order_id
