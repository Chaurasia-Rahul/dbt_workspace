{{ config(materialized='table', alias='pivot_sales_person') }}

SELECT
   currentyearsales
  {{ dbt_utils.pivot('sales_person_id',dbt_utils.get_column_values(ref("yearly_sales_person"),"sales_person_id")) }}
FROM {{ ref('yearly_sales_person') }}
GROUP BY currentyearsales