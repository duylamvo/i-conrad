{{ config(materialized='table') }}

{% set SINCE_N_YEARS = 2 %}


with 
all_sales as (
    select
        id as order_item_id
        , product_id
        , order_id
        , user_id
        , datetime(substr(created_at, 1, 19)) as created_at
        , status
        , sale_price
    from  
        {{ source('raw__thelook_ecommerce', 'order_items') }}
)

, sales_previous_years as (
    select 
        * 
    from 
        all_sales
    where 
        created_at >= date('now', '-{{ SINCE_N_YEARS }} year')
)

, returned_orders as (
    select
        user_id
        , count(
            case 
                when status = 'Returned' then 1
                else NULL
            end
        ) as return_count
        , count(*) as total_sales
        , SUM(sale_price) as total_spending
    from 
        sales_previous_years
    group by 
        user_id
)

, return_rates as (
    select 
        user_id
        , return_count
        , total_sales
        , round(
            ((cast(return_count as float) / total_sales))
            , 2
        ) as return_rate
        , total_spending
        , case
            when total_spending <= 50 then 'Level 1'
            when total_spending > 50 and total_spending <= 150 then 'Level 2'
            when total_spending > 150 then 'Level 3'
        END AS profit_level
    from 
        returned_orders
)
select * from return_rates 
