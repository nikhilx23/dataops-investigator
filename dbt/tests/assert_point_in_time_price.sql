-- Point-in-time join correctness: each order line's unit price equals the list price of the
-- product version valid when the order was placed (business rule: price at order time).
select f.order_item_id, f.unit_price_cents, p.list_price_cents
from {{ ref('fct_order_item') }} f
join {{ ref('dim_product_history') }} p on p.product_version_key = f.product_version_key
where f.unit_price_cents <> p.list_price_cents
