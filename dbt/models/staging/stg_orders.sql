-- Grain: one row per order (current version). Key: order_id.
with latest as ({{ latest_committed_records('orders') }})
select
    (payload ->> 'order_id')::bigint            as order_id,
    (payload ->> 'customer_id')::bigint         as customer_id,
    payload ->> 'status'                        as order_status,
    payload ->> 'currency'                      as currency,
    (payload ->> 'placed_at')::timestamptz      as placed_at,
    (payload ->> 'cancelled_at')::timestamptz   as cancelled_at,
    (payload ->> 'updated_at')::timestamptz     as source_updated_at,
    record_id                                   as raw_record_id,
    batch_id                                    as raw_batch_id
from latest
