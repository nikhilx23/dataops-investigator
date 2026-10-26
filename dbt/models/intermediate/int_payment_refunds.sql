-- Grain: one row per refunded payment. Key: payment_id.
select
    payment_id,
    count(*)                    as refund_count,
    sum(amount_cents)           as refunded_cents,
    max(refunded_at)            as last_refunded_at
from {{ ref('stg_refunds') }}
group by payment_id
