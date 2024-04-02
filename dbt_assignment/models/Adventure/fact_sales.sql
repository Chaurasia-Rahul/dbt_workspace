{%- set years = ['2011', '2012', '2013', '2014'] -%}

{%- for year in years -%}
    {{ config(materialized='incremental', unique_key=['sales_order_id', 'sales_order_detail_id'], alias='fact_sales_order') }}

        select *
        from dwh.sales_order_item_{{ year }} 
        where 1=1
         {% if is_incremental() %}
           and MODIFIED_DATE::timestamp > (select max(MODIFIED_DATE) from {{ this }} )
        {% endif %}
    {% if not loop.last -%}
        union all
    {%- endif -%}
{%- endfor -%}
