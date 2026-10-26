-- Business rule: the refunds on a payment never exceed the amount it captured,
-- and only captured payments can be refunded. Returns violating payments.
select p.payment_id, p.payment_status, p.amount_cents, sum(r.amount_cents) as refunded_cents
from {{ ref('stg_refunds') }} r
join {{ ref('stg_payments') }} p on p.payment_id = r.payment_id
group by p.payment_id, p.payment_status, p.amount_cents
having sum(r.amount_cents) > p.amount_cents
    or p.payment_status not in ('succeeded', 'refunded')
