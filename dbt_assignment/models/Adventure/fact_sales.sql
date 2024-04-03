{{ config(materialized='incremental', unique_key=['sales_order_id','sales_order_detail_id'], alias='fact_sales_order') }}

WITH cte_sales_order AS (
 
{%- set years = run_query("SELECT DISTINCT EXTRACT(YEAR FROM order_date)::INT AS year FROM stg.salesorderheader ") %}

{%- for year in years -%}
      
        select *
        from dwh.sales_order_item_{{ year.year }}
        where 1=1
            {% if is_incremental() %}
                and modified_date::timestamp > (select max(modified_date) from {{ this }} )
            {%- endif -%}
        {% if not loop.last %}
                union all
        {% endif %}
{%- endfor -%}
       ),
       salesorderheaders AS (
         SELECT *
         FROM {{ source('stg', 'salesorderheader') }}
       ),
       final_draft as (
       select a.sales_order_id::int
             ,a.sales_order_detail_id::int
            ,carrier_tracking_number::varchar(50)
            ,order_qty::int
            ,product_id::int
            ,special_offer_id::int
            ,unit_price::numeric(38, 9)
            ,unit_price_discount::numeric(38, 9)
            ,line_total::numeric(38, 9)
            ,a.modified_date::timestamp
            ,b.sub_total::numeric(38, 9) as Gross_sales
            ,b.Total_due::numeric(38, 9) as Net_Sales
            ,cast('01/01/1999' as date) as etl_time
        from cte_sales_order a
        inner join salesorderheaders b on a.sales_order_id = b.sales_order_id
        )
        select *
        from final_draft
