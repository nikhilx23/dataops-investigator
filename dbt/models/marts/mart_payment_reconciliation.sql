-- Grain: one row per order. Key: order_id.
-- Compares what the order is worth with the money actually captured and refunded.
with money as (
    select order_id,
           coalesce(sum(amount_cents) filter (where transaction_type = 'capture'), 0)  as captured_cents,
           coalesce(-sum(amount_cents) filter (where transaction_type = 'refund'), 0)  as refunded_cents,
           count(*) filter (where transaction_type = 'capture')                        as capture_count
    from {{ ref('fct_payment_transaction') }}
    group by order_id
)
select
    o.order_id,
    o.order_status,
    t.order_total_cents,
    coalesce(m.captured_cents, 0)                                   as captured_cents,
    coalesce(m.refunded_cents, 0)                                   as refunded_cents,
    coalesce(m.captured_cents, 0) - coalesce(m.refunded_cents, 0)   as net_collected_cents,
    coalesce(m.capture_count, 0)                                    as capture_count,
    coalesce(m.capture_count, 0) > 0
      and coalesce(m.captured_cents, 0) <> t.order_total_cents      as capture_amount_mismatch,
    coalesce(m.refunded_cents, 0) > coalesce(m.captured_cents, 0)   as over_refunded,
    coalesce(m.capture_count, 0) > 1                                as multiple_captures
from {{ ref('stg_orders') }} o
join {{ ref('int_order_totals') }} t on t.order_id = o.order_id
left join money m on m.order_id = o.order_id
