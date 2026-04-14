-- Початок ігрової сесії (event_name='session_started').

SELECT
    event_id,
    session_id,
    player_id,
    event_ts AS session_started_ts,
    event_date AS session_started_date,
    platform,
    country_code,
    app_version,
    device_type
FROM {{ ref('stg_events') }}
WHERE event_name = 'session_started'
