-- Grain: one row per warehouse (current version). Key: warehouse_id.
with latest as ({{ latest_committed_records('warehouses') }})
select
    (payload ->> 'warehouse_id')::integer      as warehouse_id,
    payload ->> 'code'                         as warehouse_code,
    payload ->> 'name'                         as warehouse_name,
    payload ->> 'region'                       as region,
    (payload ->> 'updated_at')::timestamptz    as source_updated_at
from latest
