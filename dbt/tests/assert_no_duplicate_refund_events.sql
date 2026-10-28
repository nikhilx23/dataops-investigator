-- Business rule: one refund event per (payment, amount, refund timestamp). Two refunds of the
-- same payment for the same amount at the same instant are the same business event delivered
-- twice (a source defect), not two refunds.
select payment_id, amount_cents, refunded_at, count(*) as copies
from {{ ref('stg_refunds') }}
group by payment_id, amount_cents, refunded_at
having count(*) > 1
