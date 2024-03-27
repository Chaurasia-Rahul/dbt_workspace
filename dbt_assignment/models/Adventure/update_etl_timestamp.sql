-- update_etl_timestamp.sql

{% for table in models %}
    {% set schema_name = table.split('.')[0] %}
    {% set table_name = table.split('.')[1] %}

    ALTER TABLE {{ schema_name }}.{{ table_name }}
    ADD COLUMN etl_timestamp timestamp without time zone DEFAULT current_timestamp;
{% endfor %}
