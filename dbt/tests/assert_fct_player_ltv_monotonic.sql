WITH ordered_ltv AS (
    SELECT
        player_id,
        transaction_id,
        transaction_ts,
        cumulative_ltv_usd,
        lag(cumulative_ltv_usd) OVER (
            PARTITION BY player_id
            ORDER BY transaction_ts, transaction_id
        ) AS previous_ltv
    FROM {{ ref('fct_player_ltv') }}
)

SELECT
    player_id,
    transaction_id,
    transaction_ts,
    previous_ltv,
    cumulative_ltv_usd
FROM ordered_ltv
WHERE previous_ltv IS NOT NULL
    AND cumulative_ltv_usd < previous_ltv
