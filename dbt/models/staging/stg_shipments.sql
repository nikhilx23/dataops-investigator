-- Grain: one row per shipment (current version). Key: shipment_id.
with latest as ({{ latest_committed_records('shipments') }})
select
    (payload ->> 'shipment_id')::bigint        as shipment_id,
    (payload ->> 'order_id')::bigint           as order_id,
    (payload ->> 'warehouse_id')::integer      as warehouse_id,
    payload ->> 'carrier'                      as carrier,
    payload ->> 'status'                       as shipment_status,
    (payload ->> 'created_at')::timestamptz    as created_at,
    (payload ->> 'updated_at')::timestamptz    as source_updated_at
from latest
