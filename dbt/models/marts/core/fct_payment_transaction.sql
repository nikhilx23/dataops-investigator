{{ config(materialized='incremental', unique_key='transaction_id',
          incremental_strategy='delete+insert', on_schema_change='fail') }}
-- Grain: one row per money movement: a capture (+amount) or a refund (-amount).
-- Key: transaction_id. Failed and pending attempts move no money and are excluded.
with movements as (
    select
        'capture-' || payment_id                 as transaction_id,
        'capture'                                as transaction_type,
        payment_id, order_id, payment_method,
        null::bigint                             as refund_id,
        captured_at                              as occurred_at,
        amount_cents                             as amount_cents,
        source_updated_at                        as change_cursor
    from {{ ref('stg_payments') }}
    where payment_status in ('succeeded', 'refunded')
    union all
    select
        'refund-' || r.refund_id,
        'refund',
        r.payment_id, p.order_id, p.payment_method,
        r.refund_id,
        r.refunded_at,
        -r.amount_cents,
        r.source_updated_at
    from {{ ref('stg_refunds') }} r
    join {{ ref('stg_payments') }} p on p.payment_id = r.payment_id
)
select
    transaction_id, transaction_type, payment_id, refund_id, order_id, payment_method,
    occurred_at,
    to_char(occurred_at at time zone 'UTC', 'YYYYMMDD')::integer as date_key,
    amount_cents,
    change_cursor
from movements
{% if is_incremental() %}
where change_cursor >= {{ incremental_cutoff() }}
{% endif %}
