-- Перегляд оферу в магазині (event_name='shop_offer_viewed').

SELECT
    event_id,
    player_id,
    session_id,
    event_ts AS shop_offer_viewed_ts,
    event_date AS shop_offer_viewed_date,
    player_level,
    platform,
    country_code
FROM {{ ref('stg_events') }}
WHERE event_name = 'shop_offer_viewed'
