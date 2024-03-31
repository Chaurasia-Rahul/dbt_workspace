{{ config(materialized='incremental',
   unique_key='customer_id',
    pre_hook='{% if is_incremental() %} DELETE FROM {{ this }} WHERE MODIFIED_DATE <> CURRENT_DATE {% endif %}') }}  

with cust AS(
select *
from {{ source('stg', 'customer') }}
),
Territory as (
select *
from {{ source('stg', 'salesterritory') }} 
),
final_cust as(
select customer_id::int,
store_id::int,
account_number::varchar(50),
c.modified_date::timestamp,
person_id::int,
c.territory_id::int,
name::varchar(50),
country_region_code::varchar(50),
"group"::varchar(50),
sales_ytd::numeric(38, 9),
sales_last_year::numeric(38, 9),
cost_ytd::numeric(38, 9),
cost_last_year::numeric(38, 9),
cast('01/01/1999' as date)::timestamp as etl_time
from cust c
left join territory b on c.TERRITORY_ID = b.TERRITORY_ID 
)
select *
from final_cust
where 1=1

 -- {% if is_incremental() %}
--  and MODIFIED_DATE::timestamp > (select max(MODIFIED_DATE) from {{this}})
 -- {% endif %}