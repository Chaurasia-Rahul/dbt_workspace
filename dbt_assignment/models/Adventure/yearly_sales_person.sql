{{ config(materialized='view',alias = 'Yearly_sales_growth_person') }}

WITH sales_by_year AS (
    SELECT 
        sales_person_id,
        EXTRACT(YEAR FROM Order_Date) AS Year,
        SUM(sub_total) AS YearlySales
    FROM 
       {{ source('stg', 'salesorderheader') }}
    GROUP BY 
        sales_person_id, EXTRACT(YEAR FROM Order_Date)
),
sale_growth_person as (
SELECT 
    current_year.sales_person_id,
    current_year.Year,
    current_year.YearlySales AS CurrentYearSales,
    COALESCE(previous_year.YearlySales, 0) AS LastYearSales,
    case when previous_year.YearlySales is null then NULL else
    round((((current_year.YearlySales- COALESCE(previous_year.YearlySales, 0))/current_year.YearlySales)*100),1)
    end as percent_Sales_Growth
FROM 
    sales_by_year AS current_year
LEFT JOIN 
    sales_by_year AS previous_year ON current_year.sales_person_id = previous_year.sales_person_id
                                     AND current_year.Year = previous_year.Year + 1
                                     )
select s.sales_person_id,
       s.year,
       s.CurrentYearSales,
       s.LastYearSales,
       s.percent_Sales_Growth
from sale_growth_person s
ORDER BY 
    s.Year,s.sales_person_id