-- Finance rules: no order captured twice, captures equal the order total, refunds never
-- exceed captures. (A failure here is a data quality incident, not a modeling bug.)
select order_id, capture_amount_mismatch, over_refunded, multiple_captures
from {{ ref('mart_payment_reconciliation') }}
where capture_amount_mismatch or over_refunded or multiple_captures
