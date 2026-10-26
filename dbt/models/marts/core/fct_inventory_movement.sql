{{ config(materialized='incremental', unique_key='movement_id',
          incremental_strategy='delete+insert', on_schema_change='fail') }}
-- Grain: one row per stock movement. Key: movement_id.
select
    movement_id, product_id, warehouse_id, movement_type, on_hand_delta, allocated_delta,
    reference_order_id, occurred_at,
    to_char(occurred_at at time zone 'UTC', 'YYYYMMDD')::integer as date_key,
    source_updated_at as change_cursor
from {{ ref('stg_inventory_movements') }}
{% if is_incremental() %}
where source_updated_at >= {{ incremental_cutoff() }}
{% endif %}
