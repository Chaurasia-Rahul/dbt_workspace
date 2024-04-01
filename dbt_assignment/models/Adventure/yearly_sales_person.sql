{{ config(materialized='view',alias = 'Yearly_sales_growth_person') }}

WITH sales_by_year AS (
    SELECT 
        sales_person_id,
        EXTRACT(YEAR FROM Order_Date) AS Year,
        SUM(sub_total) AS YearlySales,
        "Target"
    FROM 
       {{ source('stg', 'salesorderheader') }} s
    LEFT JOIN dwh.targetsales t ON s.sales_person_id = t."SalesPersonID"
    GROUP BY 
        sales_person_id, EXTRACT(YEAR FROM Order_Date), "Target"
),
sale_growth_person AS (
    SELECT 
        sales_person_id,
        Year,
        ROUND(YearlySales, 2) AS CurrentYearSales,
        COALESCE(LAG(YearlySales) OVER (PARTITION BY sales_person_id ORDER BY Year), 0) AS LastYearSales,
        CASE 
            WHEN LAG(YearlySales) OVER (PARTITION BY sales_person_id ORDER BY Year) IS NULL THEN NULL 
            ELSE ROUND((((YearlySales - COALESCE(LAG(YearlySales) OVER (PARTITION BY sales_person_id ORDER BY Year), 0))
            / COALESCE(LAG(YearlySales) OVER (PARTITION BY sales_person_id ORDER BY Year), 0)) * 100), 1)
        END AS percent_Sales_Growth,
        "Target",
        CASE 
            WHEN LAG(YearlySales) OVER (PARTITION BY sales_person_id ORDER BY Year) IS NULL THEN NULL 
            ELSE 
            ROUND((YearlySales::numeric - "Target"::numeric), 2)
        END AS Target_Gap
    FROM 
        sales_by_year
)
SELECT 
    sales_person_id,
    Year,
    CurrentYearSales,
    LastYearSales,
    percent_Sales_Growth,
    "Target",
    Target_Gap
FROM 
    sale_growth_person
--where sales_person_id is not null
ORDER BY 
    sales_person_id,Year