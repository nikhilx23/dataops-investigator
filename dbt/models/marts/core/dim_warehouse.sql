-- Grain: one row per warehouse. Key: warehouse_id. SCD Type 1.
select warehouse_id, warehouse_code, warehouse_name, region
from {{ ref('stg_warehouses') }}
