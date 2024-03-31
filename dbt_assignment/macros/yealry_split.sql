{% macro yearly_split(year) %}

{{ config(materialized='table', alias='sales_order_item_'~year) }}
SELECT s.*, s2.sub_total, s2.total_due, EXTRACT(YEAR FROM Order_date) AS "Year"
FROM STG.salesorderdetail s 
INNER JOIN STG.salesorderheader s2 
ON s.sales_order_id = s2.sales_order_id 
WHERE EXTRACT(YEAR FROM ORDER_DATE) = {{ year }}

{% endmacro %}
