{%- set distinct_years = run_query("SELECT DISTINCT EXTRACT(YEAR FROM order_date) AS year FROM stg.salesorderheader") -%}

{%- for year_record in distinct_years -%}
    {%- set year = year_record.year -%}
    {{ config(
        materialized='table',
        alias='salesorder_' ~ year
    ) }}
    
    SELECT sales_order_id, order_date
    FROM {{ source('stg', 'salesorderheader') }}
    WHERE EXTRACT(YEAR FROM order_date) = {{ year }}
{%- endfor -%}



---try it with DLThub