-- Cross-model consistency (guards against double counting): booked revenue from the order-line
-- fact and net cash from the payment-transaction fact must equal mart_daily_revenue per date.
with booked as (
    select (placed_at at time zone 'UTC')::date as d, sum(net_amount_cents) as v
    from {{ ref('fct_order_item') }} where not is_cancelled_order group by 1
), cash as (
    select (occurred_at at time zone 'UTC')::date as d, sum(amount_cents) as v
    from {{ ref('fct_payment_transaction') }} group by 1
)
select m.revenue_date, m.booked_revenue_cents, b.v as fact_booked,
       m.net_collected_cents, c.v as fact_cash
from {{ ref('mart_daily_revenue') }} m
left join booked b on b.d = m.revenue_date
left join cash c on c.d = m.revenue_date
where m.booked_revenue_cents <> coalesce(b.v, 0)
   or m.net_collected_cents <> coalesce(c.v, 0)
