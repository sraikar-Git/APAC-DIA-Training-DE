{% macro normalize_timestamp(column_name) %}
    CAST({{ column_name }} AS TIMESTAMP) AT TIME ZONE 'UTC'
{% endmacro %}