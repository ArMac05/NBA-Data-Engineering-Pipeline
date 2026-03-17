{% macro export_to_parquet(model) %}
    COPY (
        SELECT * FROM {{ model }}
    )
    TO './table-exports/{{ model.name }}.parquet'
    (FORMAT PARQUET);
{% endmacro %}
