{% macro update_etl_time() %}
{%- set tables = run_query("SELECT table_name FROM information_schema.tables WHERE table_schema='dwh' and table_type ='BASE TABLE' and table_name !='targetsales'") -%}
{% for table in tables %}
    {%- set model_name = table[0] -%}
    {%- set full_table_name = "dwh." ~ model_name -%}
    {%- set update_sql = "UPDATE " ~ full_table_name ~ " SET etl_time = CURRENT_TIMESTAMP" -%}
    {%- do run_query(update_sql) -%}
{% endfor %}
{% endmacro %}
