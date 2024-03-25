{{ config(materialized='incremental',unique_key='customer_id') }}

with CUST AS(
select CUSTOMER_ID,
	   PERSON_ID,
	   TERRITORY_ID as CUST_TERRITORY_ID,
	   ACCOUNT_NUMBER,
	   ROWGUID as CUST_ROWGUID,
	   MODIFIED_DATE as CUST_MODIFIED_DATE
from {{ source('stg', 'customer') }}
),
TERRITORY as (
select TERRITORY_ID as TERR_TERRITORY_ID,
	   NAME,
	   COUNTRY_REGION_CODE,
	   "group",
	   SALES_YTD,
	   SALES_LAST_YEAR,
	   COST_YTD,
	   COST_LAST_YEAR,
	   ROWGUID as TERRITORY_ROWGUID,
	   MODIFIED_DATE as TERR_MODIFIED_DATE
from{{ source('stg', 'salesterritory') }}
)
select CUSTOMER_ID,
	   PERSON_ID,
	   CUST_TERRITORY_ID,
	   ACCOUNT_NUMBER,
	   CUST_ROWGUID,
	   CUST_MODIFIED_DATE,
	   NAME,
	   COUNTRY_REGION_CODE,
	   "group",
	   SALES_YTD,
	   SALES_LAST_YEAR,
	   COST_YTD,
	   COST_LAST_YEAR,
	   TERRITORY_ROWGUID,
	   TERR_MODIFIED_DATE,
	   '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
from CUST 
left join TERRITORY on CUST.CUST_TERRITORY_ID = TERRITORY.TERR_TERRITORY_ID


  {% if is_incremental() %}
  and cust.CUST_MODIFIED_DATE::timestamp > (select max(CUST_MODIFIED_DATE) from {{this}})
  {% endif %}