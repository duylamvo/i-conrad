{{ config(materialized='table') }}

with 
nearest_centers as (
    select
        *
    from  
        {{ ref('stg__customer_nearest_centers') }}
)

, customer_profit as (
    select 
        * 
    from 
        {{ ref("stg__customer_profit") }}
)

-- here we get only customer having order within X years
, customer_analysis as (
    select 
        nc.user_id
        , nc.center_id as nearest_center_id
        , nc.distance_km
        , cp.return_rate
        , cp.total_sales
        , cp.total_spending
        , cp.profit_level
    from 
        customer_profit cp
        left join  nearest_centers as nc on nc.user_id = cp.user_id
)
select * from customer_analysis

