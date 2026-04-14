-- Кінець ігрової сесії (event_name='session_ended').

SELECT
    event_id,
    session_id,
    player_id,
    event_ts AS session_ended_ts,
    event_date AS session_ended_date,
    platform,
    country_code
FROM {{ ref('stg_events') }}
WHERE event_name = 'session_ended'
