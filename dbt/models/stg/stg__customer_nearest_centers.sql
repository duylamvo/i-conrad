{{ config(materialized='table') }}

with 
user_locations as (
    select 
        id as user_id
        , age
        , state
        , country
        , latitude as user_lat
        , longitude as user_lon
    from 
        {{ source('raw__thelook_ecommerce', 'users') }}

)

, distribution_center_locations as (
    select
        id as center_id
        , name as center_name
        , latitude as center_lat
        , longitude as center_lon
    from {{ source('raw__thelook_ecommerce', 'distribution_centers') }}
)

, distances as (
    select
        u.user_id
        , u.age
        , u.state
        , u.country
        , d.center_id
        , d.center_name
        , {{ haversine_distance(
            'u.user_lat'
            , 'u.user_lon'
            , 'd.center_lat'
            , 'd.center_lon') 
        }} as distance_km
    from user_locations u
    cross join distribution_center_locations d
)

, nearest_centers as (
    select
        d.*
        , row_number() over ( partition by user_id order by distance_km ) as rn
    from distances as d
)

select
    user_id
    , age
    , state
    , country
    , center_id
    , center_name
    , distance_km
from nearest_centers
where rn = 1