-- Grain: one row per order. Key: order_id.
-- Fulfillment timeline from facts; each fan-out (payments, events) is pre-aggregated to the
-- order grain before joining, so no measure is duplicated.
with captures as (
    select order_id, min(occurred_at) as captured_at
    from {{ ref('fct_payment_transaction') }}
    where transaction_type = 'capture'
    group by order_id
),
shipment_milestones as (
    select order_id,
           min(event_at) filter (where event_type = 'label_created') as label_created_at,
           min(event_at) filter (where event_type = 'picked_up')     as picked_up_at,
           min(event_at) filter (where event_type = 'delivered')     as delivered_at,
           count(*) filter (where is_late_arrival)                    as late_arriving_events
    from {{ ref('fct_shipment_event') }}
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.placed_at,
    c.captured_at,
    m.label_created_at,
    m.picked_up_at,
    m.delivered_at,
    round(extract(epoch from (m.label_created_at - c.captured_at)) / 3600.0, 2)
                                                         as hours_capture_to_label,
    round(extract(epoch from (m.delivered_at - o.placed_at)) / 3600.0, 2)
                                                         as hours_order_to_delivery,
    coalesce(m.late_arriving_events, 0)                  as late_arriving_events,
    t.order_total_cents
from {{ ref('stg_orders') }} o
left join {{ ref('int_order_totals') }} t on t.order_id = o.order_id
left join captures c on c.order_id = o.order_id
left join shipment_milestones m on m.order_id = o.order_id
