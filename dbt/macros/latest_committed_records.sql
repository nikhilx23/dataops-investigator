{#
  Current version of every source row of `table_name`, read only from COMMITTED batches.

  - Publication boundary: records are joined to raw.ingestion_batches and filtered on
    status = 'committed', so a batch that is still running, failed or abandoned is never read.
  - Version resolution: the latest source_updated_at wins; ties (two changes inside the same
    microsecond) are broken by ingestion time, then record_id, so the result is deterministic.
#}
{% macro latest_committed_records(table_name) %}
    select distinct on (r.source_pk)
        r.source_pk,
        r.source_updated_at,
        r.payload,
        r.record_id,
        r.batch_id,
        r.ingested_at
    from {{ source('raw', 'source_records') }} r
    join {{ source('raw', 'ingestion_batches') }} b
      on b.batch_id = r.batch_id
     and b.status = 'committed'
    where r.source_system = 'commerce_source'
      and r.source_table = '{{ table_name }}'
    order by r.source_pk, r.source_updated_at desc, r.ingested_at desc, r.record_id desc
{% endmacro %}
