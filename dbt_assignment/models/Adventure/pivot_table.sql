{{ config(alias='net_sales_pivot') }}

select
    sales_person_id,
    {{ dbt_utils.pivot("year", dbt_utils.get_column_values(ref("yearly_sales_person"), "year"), then_value="currentyearsales") }},
    TO_TIMESTAMP('01/01/1999', 'MM/DD/YYYY') AS etl_time
from {{ ref('yearly_sales_person') }}
group by sales_person_id