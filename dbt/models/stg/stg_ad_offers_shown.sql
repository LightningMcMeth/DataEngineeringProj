-- Показ оферу реклами (event_name='ad_offer_shown').

SELECT
    event_id,
    player_id,
    session_id,
    event_ts AS ad_offer_shown_ts,
    event_date AS ad_offer_shown_date,
    platform,
    country_code,
    arena_id,
    player_level
FROM {{ ref('stg_events') }}
WHERE event_name = 'ad_offer_shown'
