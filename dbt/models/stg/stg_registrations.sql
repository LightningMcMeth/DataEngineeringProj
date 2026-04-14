-- Подія реєстрації гравця (event_name='player_registered').
-- Одна подія на гравця (очікувано).

SELECT
    event_id,
    player_id,
    event_ts AS registered_ts,
    event_date AS registered_date,
    platform,
    country_code,
    app_version,
    registration_method,
    device_type,
    acquisition_channel
FROM {{ ref('stg_events') }}
WHERE event_name = 'player_registered'
