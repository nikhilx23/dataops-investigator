-- Grain: one row per shipment event. Key: shipment_event_id.
-- event_at = when it happened; received_at = when the source learned of it.
with latest as ({{ latest_committed_records('shipment_events') }})
select
    (payload ->> 'shipment_event_id')::bigint  as shipment_event_id,
    (payload ->> 'shipment_id')::bigint        as shipment_id,
    payload ->> 'event_type'                   as event_type,
    (payload ->> 'event_at')::timestamptz      as event_at,
    (payload ->> 'received_at')::timestamptz   as received_at,
    (payload ->> 'updated_at')::timestamptz    as source_updated_at
from latest
