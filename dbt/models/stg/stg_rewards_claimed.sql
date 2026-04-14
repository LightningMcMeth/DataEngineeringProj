-- Отримання нагороди (event_name='reward_claimed').

SELECT
    event_id,
    player_id,
    session_id,
    event_ts AS reward_claimed_ts,
    event_date AS reward_claimed_date,
    player_level,
    platform,
    country_code
FROM {{ ref('stg_events') }}
WHERE event_name = 'reward_claimed'
