-- Stock rules: allocated and available units are never negative.
select product_id, warehouse_id, on_hand_units, allocated_units, available_units
from {{ ref('mart_inventory_position') }}
where allocated_units < 0 or available_units < 0 or on_hand_units < 0
