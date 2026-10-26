-- Monetary and quantity checks. Invalid values are reported, never coerced to zero.
select 'order_item' as entity, order_item_id::text as id, 'non-positive quantity' as problem
from {{ ref('stg_order_items') }} where quantity <= 0
union all
select 'order_item', order_item_id::text, 'negative line amount'
from {{ ref('stg_order_items') }} where line_amount_cents < 0 or unit_price_cents < 0
union all
select 'payment', payment_id::text, 'non-positive amount'
from {{ ref('stg_payments') }} where amount_cents <= 0
union all
select 'refund', refund_id::text, 'non-positive amount'
from {{ ref('stg_refunds') }} where amount_cents <= 0
