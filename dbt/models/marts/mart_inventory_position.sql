-- Grain: one row per product x warehouse. Key: (product_id, warehouse_id).
-- available = on_hand - allocated (docs/business_rules.md).
select
    m.product_id,
    m.warehouse_id,
    sum(m.on_hand_delta)::bigint                           as on_hand_units,
    sum(m.allocated_delta)::bigint                         as allocated_units,
    (sum(m.on_hand_delta) - sum(m.allocated_delta))::bigint as available_units,
    max(m.occurred_at)                                     as last_movement_at
from {{ ref('fct_inventory_movement') }} m
group by m.product_id, m.warehouse_id
