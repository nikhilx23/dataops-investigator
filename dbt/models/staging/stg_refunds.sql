-- Grain: one row per refund (current version). Key: refund_id.
with latest as ({{ latest_committed_records('refunds') }})
select
    (payload ->> 'refund_id')::bigint           as refund_id,
    (payload ->> 'payment_id')::bigint          as payment_id,
    (payload ->> 'amount_cents')::bigint        as amount_cents,
    payload ->> 'reason'                        as refund_reason,
    (payload ->> 'refunded_at')::timestamptz    as refunded_at,
    (payload ->> 'updated_at')::timestamptz     as source_updated_at,
    record_id                                   as raw_record_id
from latest
