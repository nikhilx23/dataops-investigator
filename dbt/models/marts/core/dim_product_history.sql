-- Grain: one row per product version. Key: product_version_key. SCD Type 2.
-- Built from the ordered source change log (every intermediate version), not from periodic
-- snapshots of shop.products. Interval semantics: [valid_from, valid_to); valid_to is null
-- for the current version.
select
    product_id::text || '-' || version::text                          as product_version_key,
    product_id,
    version,
    sku,
    product_name,
    category,
    list_price_cents,
    is_active,
    valid_from,
    lead(valid_from) over (partition by product_id order by version) as valid_to,
    lead(valid_from) over (partition by product_id order by version) is null
                                                                      as is_current,
    product_version_id                                                as source_version_id,
    recorded_at
from {{ ref('stg_product_versions') }}
