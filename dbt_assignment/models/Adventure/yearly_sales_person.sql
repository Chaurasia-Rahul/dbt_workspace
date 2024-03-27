{{ config(materialized='view',alias = 'Yearly_sales_growth_person') }}

WITH sales_by_year AS (
    SELECT 
        sales_person_id,
        EXTRACT(YEAR FROM Order_Date) AS Year,
        SUM(sub_total) AS YearlySales,
        "Target"
    FROM 
       stg.salesorderheader s
    left join dwh.targetsales t on s.sales_person_id = t."SalesPersonID"
    GROUP BY 
        sales_person_id, EXTRACT(YEAR FROM Order_Date), "Target"
),
sale_growth_person as (
SELECT 
    current_year.sales_person_id,
    current_year.Year,
    round(current_year.YearlySales,2) AS CurrentYearSales,
    COALESCE(previous_year.YearlySales, 0) AS LastYearSales,
    case when previous_year.YearlySales is null then NULL else
    round((((current_year.YearlySales- COALESCE(previous_year.YearlySales, 0))/current_year.YearlySales)*100),1)
    end as percent_Sales_Growth,
    current_year."Target",
    case when previous_year.YearlySales is null then NULL else
    current_year.YearlySales - current_year."Target" end as Target_Gap
FROM 
    sales_by_year AS current_year
LEFT JOIN 
    sales_by_year AS previous_year ON current_year.sales_person_id = previous_year.sales_person_id
                                     AND current_year.Year = previous_year.Year + 1
                                     ),
final_draft as (
select s.sales_person_id::int,
       s.year::int,
       s.CurrentYearSales::int,
       s.LastYearSales::int,
       s.percent_Sales_Growth::numeric(10, 2),
       s."Target"::int,
       s.Target_Gap::int
from sale_growth_person s
ORDER BY 
    s.Year,s.sales_person_id
    )
select *
from final_draft