-- Grain: one row per UTC calendar date. Key: revenue_date.
-- Metric definitions: docs/business_rules.md. Each measure is aggregated from its OWN fact
-- grain on its OWN date attribution, then joined on date. Order lines are never summed
-- after a join to payments or refunds, which is what prevents double counting.

with order_totals as (
    select order_id, sum(line_amount_cents) as order_total_cents
    from {{ ref('stg_order_items') }}
    group by order_id
),

orders_by_day as (
    select
        (o.placed_at at time zone 'UTC')::date                                   as revenue_date,
        count(*)                                                                 as orders_placed,
        count(*) filter (where o.order_status = 'cancelled')                     as orders_cancelled,
        coalesce(sum(t.order_total_cents) filter (where o.order_status <> 'cancelled'), 0)
                                                                                 as booked_revenue_cents
    from {{ ref('stg_orders') }} o
    left join order_totals t on t.order_id = o.order_id
    group by 1
),

captures_by_day as (
    select (captured_at at time zone 'UTC')::date as revenue_date,
           sum(amount_cents)                     as captured_cents
    from {{ ref('stg_payments') }}
    where payment_status in ('succeeded', 'refunded')
    group by 1
),

refunds_by_day as (
    select (refunded_at at time zone 'UTC')::date as revenue_date,
           sum(amount_cents)                     as refunded_cents
    from {{ ref('stg_refunds') }}
    group by 1
),

bounds as (
    select min(revenue_date) as first_date, max(revenue_date) as last_date
    from (
        select revenue_date from orders_by_day
        union all select revenue_date from captures_by_day
        union all select revenue_date from refunds_by_day
    ) d
),

date_spine as (
    select generate_series(first_date, last_date, interval '1 day')::date as revenue_date
    from bounds
    where first_date is not null
)

select
    s.revenue_date,
    coalesce(o.orders_placed, 0)::integer                         as orders_placed,
    coalesce(o.orders_cancelled, 0)::integer                      as orders_cancelled,
    coalesce(o.booked_revenue_cents, 0)::bigint                   as booked_revenue_cents,
    coalesce(c.captured_cents, 0)::bigint                         as captured_cents,
    coalesce(r.refunded_cents, 0)::bigint                         as refunded_cents,
    (coalesce(c.captured_cents, 0) - coalesce(r.refunded_cents, 0))::bigint
                                                                  as net_collected_cents,
    'USD'::text                                                   as currency
from date_spine s
left join orders_by_day   o on o.revenue_date = s.revenue_date
left join captures_by_day c on c.revenue_date = s.revenue_date
left join refunds_by_day  r on r.revenue_date = s.revenue_date
order by s.revenue_date
