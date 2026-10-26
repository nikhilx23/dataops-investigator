-- Grain: one row per payment attempt (current version). Key: payment_id.
with latest as ({{ latest_committed_records('payments') }})
select
    (payload ->> 'payment_id')::bigint          as payment_id,
    (payload ->> 'order_id')::bigint            as order_id,
    (payload ->> 'attempt_number')::integer     as attempt_number,
    payload ->> 'status'                        as payment_status,
    payload ->> 'method'                        as payment_method,
    (payload ->> 'amount_cents')::bigint        as amount_cents,
    (payload ->> 'created_at')::timestamptz     as created_at,
    (payload ->> 'captured_at')::timestamptz    as captured_at,
    payload ->> 'failure_reason'                as failure_reason,
    (payload ->> 'updated_at')::timestamptz     as source_updated_at,
    record_id                                   as raw_record_id
from latest
