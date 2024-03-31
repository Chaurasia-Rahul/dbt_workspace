{{config(materialized='view',alias= 'dim_product_view',unique_key='product_id')}}

with product_flag as (
select *
, CASE WHEN make_flag = '1' and size is null then 1 ELSE 0 end as is_make_no_size
, CASE WHEN finished_goods_flag = '1' and weight > 25 then 1 ELSE 0 end as is_finished_and_over_25
from {{ source('stg','product') }}  
),
product AS (
select *,
case when is_make_no_size = 1 or is_finished_and_over_25 = 1 then 1 else 0 end as is_rel
from product_flag  
),
Productcategory as (
select *
from {{ source('stg', 'productcategory') }} 
),
subcategory AS (
select *
from {{ source('stg', 'productsubcategory') }}
),
final_product AS (
select product_id::int
,a.name::varchar(30) as Product_name 
,product_number::varchar(30)
,MAKE_FLAG::bool
,is_make_no_size::int
,finished_goods_flag::bool
,is_finished_and_over_25::int
,is_rel::int
,safety_stock_level::int
,reorder_point::int
,standard_cost::numeric(38, 9)
,list_price::numeric(38, 9)
,days_to_manufacture:: int
,sell_start_date::timestamp
,a.modified_date::timestamp
,color::varchar(30)
,"class"::varchar(30)
,weight_unit_measure_code::varchar(30)
,weight::numeric(38, 9)
,size::varchar(30)
,size_unit_measure_code::varchar(30)
,product_line::varchar(30)
,style::varchar(30)
,product_model_id::int
,sell_end_date::timestamp
,b.product_category_id::int
,b.name::varchar(30) as Category_name
,c.product_subcategory_id::int
,c.name::varchar(30) as Subcategory_name 
from product a
inner join Productcategory b on a.product_id = b.product_category_id
inner join subcategory c on b.product_category_id = c.product_category_id
)
select *
from final_product
where 1=1

  {% if is_incremental() %}
  and MODIFIED_DATE::timestamp > (select max(MODIFIED_DATE) from {{this}})
  {% endif %}