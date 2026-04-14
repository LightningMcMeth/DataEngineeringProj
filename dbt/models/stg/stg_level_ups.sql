-- Подія level up (event_name='level_up').

SELECT
    event_id,
    player_id,
    session_id,
    event_ts AS level_up_ts,
    event_date AS level_up_date,
    player_level AS new_level,
    platform,
    country_code
FROM {{ ref('stg_events') }}
WHERE event_name = 'level_up'
