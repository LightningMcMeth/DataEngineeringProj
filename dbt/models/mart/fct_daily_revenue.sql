-- Щоденний revenue. Базова метрика для всіх revenue-дашбордів.
-- Всі транзакції вже в USD, але gross_usd рахуємо через fx_rates для правильності.
-- Тег 'hourly' додається до успадкованого 'daily' — це "операційна" модель,
-- яку хочемо оновлювати щогодини, щоб бачити revenue поточного дня "на зараз".

{{ config(tags=['hourly']) }}

WITH completed_tx AS (

    SELECT
        p.transaction_date,
        p.transaction_id,
        p.player_id,
        p.gross_amount * fx.rate_to_usd AS gross_usd
    FROM {{ ref('stg_purchases') }} AS p
    LEFT JOIN {{ ref('fx_rates') }} AS fx ON p.currency_code = fx.currency_code
    WHERE p.payment_status = 'completed'

)

SELECT
    transaction_date AS date_day,
    count(DISTINCT transaction_id) AS orders,
    count(DISTINCT player_id) AS unique_payers,
    sum(gross_usd) AS gross_revenue_usd,
    avg(gross_usd) AS avg_order_value_usd
FROM completed_tx
GROUP BY transaction_date
ORDER BY transaction_date
