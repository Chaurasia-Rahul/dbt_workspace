{{config(materialized='table')}}

select product_id
,name as Product_name
,product_number
,case when make_flag = '0' then 0 else 1 end as MAKE_FLAG
,case when finished_goods_flag ='0' then 0 else 1 end as finished_goods_flag
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
from {{ source('stg', 'product') }}