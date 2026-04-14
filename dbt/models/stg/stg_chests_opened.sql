-- Відкриття скриньки (event_name='chest_opened').

SELECT
    event_id,
    player_id,
    session_id,
    event_ts AS chest_opened_ts,
    event_date AS chest_opened_date,
    player_level,
    platform,
    country_code
FROM {{ ref('stg_events') }}
WHERE event_name = 'chest_opened'
