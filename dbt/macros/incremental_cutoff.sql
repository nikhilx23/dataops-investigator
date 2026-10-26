{# Lower bound on the change cursor for an incremental run: latest processed cursor minus the
   configured lookback. Only meaningful inside `{% if is_incremental() %}`. #}
{% macro incremental_cutoff(cursor_column='change_cursor') %}
    (select coalesce(max({{ cursor_column }}), '1900-01-01'::timestamptz)
            - interval '{{ var("incremental_lookback_days") }} days'
     from {{ this }})
{% endmacro %}
