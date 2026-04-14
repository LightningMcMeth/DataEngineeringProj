-- Повний перегляд рекламного ролика (event_name='ad_watched').

SELECT
    event_id,
    player_id,
    session_id,
    event_ts AS ad_watched_ts,
    event_date AS ad_watched_date,
    platform,
    country_code,
    player_level
FROM {{ ref('stg_events') }}
WHERE event_name = 'ad_watched'
