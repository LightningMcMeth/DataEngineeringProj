-- 7-денна ковзна сума revenue. WINDOW FUNCTION: rolling window
-- (sum() over ... rows between 6 preceding and current row).
-- Забезпечує безперервність ряду за рахунок dim_dates (left join).

WITH daily AS (

    SELECT
        date_day,
        gross_revenue_usd,
        orders,
        unique_payers
    FROM {{ ref('fct_daily_revenue') }}

),

dense AS (

    SELECT
        d.date_day,
        coalesce(daily.gross_revenue_usd, 0) AS gross_revenue_usd,
        coalesce(daily.orders, 0) AS orders,
        coalesce(daily.unique_payers, 0) AS unique_payers
    FROM {{ ref('dim_dates') }} AS d
    LEFT JOIN daily ON d.date_day = daily.date_day

)

SELECT
    date_day,
    gross_revenue_usd,
    orders,
    unique_payers,

    -- rolling 7-day revenue
    sum(gross_revenue_usd) OVER (
        ORDER BY date_day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_revenue_usd,

    -- rolling 7-day orders
    sum(orders) OVER (
        ORDER BY date_day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_orders,

    -- день-до-дня приріст (lag)
    gross_revenue_usd - lag(gross_revenue_usd, 1) OVER (ORDER BY date_day) AS revenue_delta_vs_prev_day
FROM dense
ORDER BY date_day
