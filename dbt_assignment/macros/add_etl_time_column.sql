-- macros/add_etl_time_column.sql
{% macro add_etl_time_column(database, schema) %}
  {% set tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" ~ schema ~ "' and table_type ='BASE TABLE'" %}
  {% set tables = run_query(tables_query, fetch='all', database=database) %}

  {% for row in tables %}
    {% set table_name = row[0] %}
    ALTER TABLE "{{ schema }}"."{{ table_name }}" ADD COLUMN etl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
  {% endfor %}
{% endmacro %}
