-- Кінець матчу (event_name='match_ended').

SELECT
    event_id,
    session_id,
    player_id,
    event_ts AS match_ended_ts,
    event_date AS match_ended_date,
    arena_id,
    player_level,
    platform,
    country_code
FROM {{ ref('stg_events') }}
WHERE event_name = 'match_ended'
