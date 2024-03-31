{% macro update_etl_time1() %}
    {%- set tables = run_query("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='dwh' and table_type ='BASE TABLE'") -%}
    {% for table in tables %}
        {%- set table_name = table[0] -%}
        {%- set update_sql = "UPDATE Adventure." ~ table_name ~ " SET etl_time = CURRENT_TIMESTAMP" -%}
        {%- do run_query(update_sql) -%}
    {% endfor %}
{% endmacro %}
