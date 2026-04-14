-- Топ-гравці за country. WINDOW FUNCTION: rank() over (partition by ... order by ...).
-- Плюс частка revenue гравця від загального revenue країни.

WITH player_revenue AS (

    SELECT
        dp.player_id,
        dp.country_code,
        dp.country_name,
        dp.region,
        sum(ltv.gross_usd) AS total_gross_usd,
        count(*) AS purchase_count
    FROM {{ ref('dim_players') }} AS dp
    INNER JOIN {{ ref('fct_player_ltv') }} AS ltv ON dp.player_id = ltv.player_id
    GROUP BY
        dp.player_id,
        dp.country_code,
        dp.country_name,
        dp.region

),

ranked AS (

    SELECT
        player_id,
        country_code,
        country_name,
        region,
        total_gross_usd,
        purchase_count,

        rank() OVER (
            PARTITION BY country_code
            ORDER BY total_gross_usd DESC
        ) AS country_rank,

        sum(total_gross_usd) OVER (PARTITION BY country_code) AS country_total_usd
    FROM player_revenue

)

SELECT
    player_id,
    country_code,
    country_name,
    region,
    total_gross_usd,
    purchase_count,
    country_rank,
    country_total_usd,
    round(total_gross_usd / nullif(country_total_usd, 0) * 100, 2) AS pct_of_country_revenue
FROM ranked
