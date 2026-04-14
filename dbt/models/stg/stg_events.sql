WITH source AS (

    SELECT * FROM {{ source('raw', 'events') }}

),

renamed AS (

    SELECT
        cast(event_id AS varchar) AS event_id,
        cast(event_name AS varchar) AS event_name,
        cast(event_timestamp AS timestamp) AS event_ts,
        cast(player_id AS bigint) AS player_id,
        cast(session_id AS varchar) AS session_id,
        cast(platform AS varchar) AS platform,
        cast(country_code AS varchar) AS country_code,
        cast(app_version AS varchar) AS app_version,
        cast(player_level AS integer) AS player_level,
        cast(arena_id AS integer) AS arena_id,
        cast(registration_method AS varchar) AS registration_method,
        cast(device_type AS varchar) AS device_type,
        cast(acquisition_channel AS varchar) AS acquisition_channel,
        date_trunc('day', cast(event_timestamp AS timestamp)) AS event_date
    FROM source

)

SELECT * FROM renamed
