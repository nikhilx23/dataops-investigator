-- Grain: one row per inventory movement. Key: movement_id.
with latest as ({{ latest_committed_records('inventory_movements') }})
select
    (payload ->> 'movement_id')::bigint        as movement_id,
    (payload ->> 'product_id')::bigint         as product_id,
    (payload ->> 'warehouse_id')::integer      as warehouse_id,
    payload ->> 'movement_type'                as movement_type,
    (payload ->> 'on_hand_delta')::integer     as on_hand_delta,
    (payload ->> 'allocated_delta')::integer   as allocated_delta,
    (payload ->> 'reference_order_id')::bigint as reference_order_id,
    (payload ->> 'occurred_at')::timestamptz   as occurred_at,
    (payload ->> 'updated_at')::timestamptz    as source_updated_at
from latest
