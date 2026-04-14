SELECT
    date_day,
    orders,
    unique_payers,
    gross_revenue_usd,
    avg_order_value_usd
FROM {{ ref('fct_daily_revenue') }}
WHERE
    orders <= 0
    OR unique_payers <= 0
    OR unique_payers > orders
    OR gross_revenue_usd <= 0
    OR avg_order_value_usd <= 0