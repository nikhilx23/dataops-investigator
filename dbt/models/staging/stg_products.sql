-- Grain: one row per product (current version). Key: product_id.
with latest as ({{ latest_committed_records('products') }})
select
    (payload ->> 'product_id')::bigint         as product_id,
    payload ->> 'sku'                          as sku,
    payload ->> 'name'                         as product_name,
    payload ->> 'category'                     as category,
    (payload ->> 'list_price_cents')::bigint   as list_price_cents,
    (payload ->> 'is_active')::boolean         as is_active,
    (payload ->> 'created_at')::timestamptz    as created_at,
    (payload ->> 'updated_at')::timestamptz    as source_updated_at
from latest
