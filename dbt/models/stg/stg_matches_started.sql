-- Початок матчу (event_name='match_started').

SELECT
    event_id,
    session_id,
    player_id,
    event_ts AS match_started_ts,
    event_date AS match_started_date,
    arena_id,
    player_level,
    platform,
    country_code
FROM {{ ref('stg_events') }}
WHERE event_name = 'match_started'
