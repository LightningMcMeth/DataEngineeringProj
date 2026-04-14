SELECT
    player_id,
    is_payer,
    purchase_count,
    total_gross_usd,
    first_purchase_ts
FROM {{ ref('dim_players') }}
WHERE
    (is_payer = true AND (purchase_count <= 0 OR total_gross_usd <= 0 OR first_purchase_ts IS NULL))
    OR
    (is_payer = false AND (purchase_count <> 0 OR total_gross_usd <> 0 OR first_purchase_ts IS NOT NULL))