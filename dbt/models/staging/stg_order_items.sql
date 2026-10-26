-- Grain: one row per order line (current version). Key: order_item_id.
with latest as ({{ latest_committed_records('order_items') }})
select
    (payload ->> 'order_item_id')::bigint       as order_item_id,
    (payload ->> 'order_id')::bigint            as order_id,
    (payload ->> 'line_number')::integer        as line_number,
    (payload ->> 'product_id')::bigint          as product_id,
    (payload ->> 'quantity')::integer           as quantity,
    (payload ->> 'unit_price_cents')::bigint    as unit_price_cents,
    (payload ->> 'discount_cents')::bigint      as discount_cents,
    (payload ->> 'quantity')::bigint * (payload ->> 'unit_price_cents')::bigint
        - (payload ->> 'discount_cents')::bigint as line_amount_cents,
    (payload ->> 'created_at')::timestamptz     as created_at,
    (payload ->> 'updated_at')::timestamptz     as source_updated_at,
    record_id                                   as raw_record_id
from latest
