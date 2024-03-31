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
        Territory_ID,
        Year,
        YearlySales AS CurrentYearSales,
        COALESCE(LAG(YearlySales) OVER (PARTITION BY Territory_ID ORDER BY Year), 0) AS LastYearSales,
        CASE 
            WHEN LAG(YearlySales) OVER (PARTITION BY Territory_ID ORDER BY Year) IS NULL THEN NULL 
            ELSE ROUND(((YearlySales - COALESCE(LAG(YearlySales) OVER (PARTITION BY Territory_ID ORDER BY Year), 0)) / COALESCE(LAG(YearlySales) OVER (PARTITION BY Territory_ID ORDER BY Year), 0)) * 100, 1)
        END AS percent_Sales_Growth
    FROM 
        sales_by_year
)
SELECT 
    Territory_ID,
    Year,
    CurrentYearSales,
    LastYearSales,
    percent_Sales_Growth
FROM 
    sale_growth_terriroty
ORDER BY 
    Year, Territory_ID
