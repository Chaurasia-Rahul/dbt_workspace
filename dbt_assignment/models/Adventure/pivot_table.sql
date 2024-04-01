{{ config(alias='Net_sales_pivot') }}

select
    sales_person_id,
    {{ dbt_utils.pivot("year", dbt_utils.get_column_values(ref("yearly_sales_person"), "year"), then_value="currentyearsales") }}
from {{ ref('yearly_sales_person') }}
group by sales_person_id

