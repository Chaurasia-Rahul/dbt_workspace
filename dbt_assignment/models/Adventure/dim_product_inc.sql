{{config(materialized='incremental',alias= 'dim_product_inc',unique_key='product_id')}}

with product AS (
select product_id
,Product_name
,product_number
,MAKE_FLAG
,finished_goods_flag
,safety_stock_level
,reorder_point
,standard_cost
,list_price
,days_to_manufacture
,sell_start_date
,modified_date
,color
,class
,weight_unit_measure_code
,weight
,size
,size_unit_measure_code
,product_line
,style
,product_subcategory_id
,product_model_id
,sell_end_date
, CASE WHEN make_flag = '1' and size = null then 1 ELSE 0 end as is_make_no_size
, CASE WHEN finished_goods_flag = '1' and weight > 25 then 1 ELSE 0 end as is_finished_and_over_25
from {{ ref('product_mod') }}
),
Productcategory as (
select product_category_id,
       name as Productcat_name
from {{ source('stg', 'productcategory') }} 
),
subcategory AS (
select product_subcategory_id,
       product_category_id,
       name as Subcategory_name
from {{ source('stg', 'productsubcategory') }}
)
select product_id
,Product_name
,product_number
,MAKE_FLAG
,is_make_no_size
,finished_goods_flag
,is_finished_and_over_25
,case when is_make_no_size = 1 or is_finished_and_over_25 = 1 then 1 else 0 end as is_rel
,safety_stock_level
,reorder_point
,standard_cost
,list_price
,days_to_manufacture
,sell_start_date
,modified_date
,color
,"class"
,weight_unit_measure_code
,weight
,size
,size_unit_measure_code
,product_line
,style
,product_model_id
,sell_end_date
,b.product_category_id
,Productcat_name
,c.product_subcategory_id
,Subcategory_name 
,'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as etl_time
from product a
inner join Productcategory b on a.product_id = b.product_category_id
inner join subcategory c on b.product_category_id = c.product_category_id


  {% if is_incremental() %}
  and MODIFIED_DATE::timestamp > (select max(MODIFIED_DATE) from {{this}})
  {% endif %}