{{ config(materialized='view',alias = 'Yearly_sales_growth') }}

WITH sales_by_year AS (
    SELECT 
        Territory_ID,
        EXTRACT(YEAR FROM Order_Date) AS Year,
        SUM(sub_total) AS YearlySales
    FROM 
       {{ source('stg', 'salesorderheader') }}
    GROUP BY 
        Territory_ID, EXTRACT(YEAR FROM Order_Date)
),
sale_growth_terriroty as (
SELECT 
    current_year.Territory_ID,
    current_year.Year,
    current_year.YearlySales AS CurrentYearSales,
    COALESCE(previous_year.YearlySales, 0) AS LastYearSales,
    case when previous_year.YearlySales is null then NULL else
    round((((current_year.YearlySales- COALESCE(previous_year.YearlySales, 0))/current_year.YearlySales)*100),1)
    end as percent_Sales_Growth
FROM 
    sales_by_year AS current_year
LEFT JOIN 
    sales_by_year AS previous_year ON current_year.Territory_ID = previous_year.Territory_ID
                                     AND current_year.Year = previous_year.Year + 1
                                     )
select s.Territory_id,
       s.year,
       s.CurrentYearSales,
       s.LastYearSales,
       s.percent_Sales_Growth
from sale_growth_terriroty s
ORDER BY 
    s.Year,s.Territory_ID