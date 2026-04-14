-- Вимір гравців. Склеює гравців з обох джерел (events + purchases)
-- і позначає, чи гравець є payer.

WITH events_players AS (

    SELECT
        player_id,
        min(event_ts) AS first_seen_ts,
        max(event_ts) AS last_seen_ts,
        any_value(country_code) AS country_code,
        any_value(platform) AS platform,
        any_value(acquisition_channel) AS acquisition_channel
    FROM {{ ref('stg_events') }}
    GROUP BY player_id

),

purchase_players AS (

    SELECT
        player_id,
        count(*) AS purchase_count,
        sum(gross_amount) AS total_gross_usd,
        min(transaction_ts) AS first_purchase_ts
    FROM {{ ref('stg_purchases') }}
    WHERE payment_status = 'completed'
    GROUP BY player_id

),

joined AS (

    SELECT
        coalesce(e.player_id, p.player_id) AS player_id,
        e.first_seen_ts,
        e.last_seen_ts,
        e.country_code,
        e.platform,
        e.acquisition_channel,
        coalesce(p.purchase_count, 0) AS purchase_count,
        coalesce(p.total_gross_usd, 0) AS total_gross_usd,
        p.first_purchase_ts,
        CASE WHEN p.purchase_count > 0 THEN true ELSE false END AS is_payer
    FROM events_players AS e
    FULL OUTER JOIN purchase_players AS p ON e.player_id = p.player_id

)

SELECT
    j.player_id,
    j.first_seen_ts,
    j.last_seen_ts,
    cast(j.first_seen_ts AS date) AS first_seen_date,
    j.country_code,
    c.country_name,
    c.region,
    j.platform,
    pf.os_family,
    pf.is_mobile,
    j.acquisition_channel,
    j.is_payer,
    j.purchase_count,
    j.total_gross_usd,
    j.first_purchase_ts
FROM joined AS j
LEFT JOIN {{ ref('country_codes') }} AS c ON j.country_code = c.country_code
LEFT JOIN {{ ref('platform_os_family') }} AS pf ON j.platform = pf.platform
