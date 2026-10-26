{#
  Current version of every source row of `table_name`, from the configured ingestion path.

  var source_path = 'batch' (default, lightweight profile):
    raw.source_records joined to raw.ingestion_batches, status = 'committed' only, so a
    running, failed or abandoned batch is never read. Latest source_updated_at wins; ties
    are broken by ingestion time, then record_id (deterministic).
  var source_path = 'cdc' (full profile):
    cdc_pub.records of the latest committed Silver publication (one row per live key,
    already resolved by LSN in Silver; deletes are absent).
  Both produce the same columns, so every staging model works on either path.
#}
{% macro latest_committed_records(table_name) %}
{% if var('source_path', 'batch') == 'cdc' %}
    select
        r.source_pk,
        r.source_updated_at,
        r.payload,
        r.record_id,
        r.publication_id                 as batch_id,
        p.committed_at                   as ingested_at
    from {{ source('cdc_pub', 'records') }} r
    join {{ source('cdc_pub', 'publications') }} p
      on p.publication_id = r.publication_id
    where r.source_table = '{{ table_name }}'
      and p.publication_id = (
            select publication_id from {{ source('cdc_pub', 'publications') }}
            order by committed_at desc limit 1)
{% else %}
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
{% endif %}
{% endmacro %}
