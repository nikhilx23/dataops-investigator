{{ config(materialized='incremental', unique_key='shipment_event_id',
          incremental_strategy='delete+insert', on_schema_change='fail') }}
-- Grain: one row per shipment event. Key: shipment_event_id. Dated by event time.
-- arrival_lag_seconds measures how late the source learned of the event.
select
    e.shipment_event_id, e.shipment_id, s.order_id, s.warehouse_id, e.event_type,
    e.event_at, e.received_at,
    to_char(e.event_at at time zone 'UTC', 'YYYYMMDD')::integer           as event_date_key,
    extract(epoch from (e.received_at - e.event_at))::bigint              as arrival_lag_seconds,
    (e.received_at - e.event_at) > interval '1 hour'                      as is_late_arrival,
    e.source_updated_at                                                   as change_cursor
from {{ ref('stg_shipment_events') }} e
join {{ ref('stg_shipments') }} s on s.shipment_id = e.shipment_id
{% if is_incremental() %}
where e.source_updated_at >= {{ incremental_cutoff() }}
{% endif %}
