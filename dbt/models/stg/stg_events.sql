{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'events') }}

),

renamed as (

    select
        cast(event_id            as varchar)   as event_id,
        cast(event_name          as varchar)   as event_name,
        cast(event_timestamp     as timestamp) as event_ts,
        cast(player_id           as bigint)    as player_id,
        cast(session_id          as varchar)   as session_id,
        cast(platform            as varchar)   as platform,
        cast(country_code        as varchar)   as country_code,
        cast(app_version         as varchar)   as app_version,
        cast(player_level        as integer)   as player_level,
        cast(arena_id            as integer)   as arena_id,
        cast(registration_method as varchar)   as registration_method,
        cast(device_type         as varchar)   as device_type,
        cast(acquisition_channel as varchar)   as acquisition_channel,
        date_trunc('day', cast(event_timestamp as timestamp)) as event_date

    from source

)

select * from renamed
