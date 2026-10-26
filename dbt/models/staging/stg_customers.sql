-- Grain: one row per customer (current version). Key: customer_id.
with latest as ({{ latest_committed_records('customers') }})
select
    (payload ->> 'customer_id')::bigint        as customer_id,
    payload ->> 'email'                        as email,
    payload ->> 'full_name'                    as full_name,
    payload ->> 'country_code'                 as country_code,
    (payload ->> 'created_at')::timestamptz    as created_at,
    (payload ->> 'updated_at')::timestamptz    as source_updated_at
from latest
