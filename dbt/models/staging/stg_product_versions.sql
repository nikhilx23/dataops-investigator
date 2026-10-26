-- Grain: one row per product version from the ordered source change log. Key: product_version_id.
with latest as ({{ latest_committed_records('product_versions') }})
select
    (payload ->> 'product_version_id')::bigint as product_version_id,
    (payload ->> 'product_id')::bigint         as product_id,
    (payload ->> 'version')::integer           as version,
    payload ->> 'sku'                          as sku,
    payload ->> 'name'                         as product_name,
    payload ->> 'category'                     as category,
    (payload ->> 'list_price_cents')::bigint   as list_price_cents,
    (payload ->> 'is_active')::boolean         as is_active,
    (payload ->> 'valid_from')::timestamptz    as valid_from,
    (payload ->> 'recorded_at')::timestamptz   as recorded_at,
    (payload ->> 'updated_at')::timestamptz    as source_updated_at
from latest
