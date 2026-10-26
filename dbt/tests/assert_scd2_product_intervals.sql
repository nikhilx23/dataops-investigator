-- SCD2 integrity: versions are contiguous, ordered and non-overlapping; exactly one current
-- version per product; version numbers have no gaps.
with v as (
    select *, lag(valid_to) over (partition by product_id order by version) as prev_valid_to,
              lag(version)  over (partition by product_id order by version) as prev_version
    from {{ ref('dim_product_history') }}
)
select product_id, version, 'valid_to <= valid_from' as problem
from v where valid_to is not null and valid_to <= valid_from
union all
select product_id, version, 'version gap' from v where prev_version is not null and version <> prev_version + 1
union all
select product_id, version, 'first version is not 1' from v where prev_version is null and version <> 1
union all
select product_id, null, 'not exactly one current version'
from {{ ref('dim_product_history') }} group by product_id having count(*) filter (where is_current) <> 1
