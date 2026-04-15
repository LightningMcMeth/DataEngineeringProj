-- LTV по гравцях з використанням WINDOW FUNCTION:
-- cumulative sum (sum() over ... rows between unbounded preceding and current row).
-- Кожен рядок — одна транзакція, з поточним кумулятивним LTV цього гравця.

WITH completed_tx AS (

    SELECT
        p.player_id,
        p.transaction_id,
        p.transaction_ts,
        p.transaction_date,
        p.gross_amount * fx.rate_to_usd AS gross_usd
    FROM {{ ref('stg_purchases') }} AS p
    LEFT JOIN {{ source('raw', 'fx_rates') }} AS fx ON p.currency_code = fx.currency_code
    WHERE p.payment_status = 'completed'

)

SELECT
    player_id,
    transaction_id,
    transaction_ts,
    transaction_date,
    gross_usd,

    -- кумулятивна сума по гравцю в хронологічному порядку
    sum(gross_usd) OVER (
        PARTITION BY player_id
        ORDER BY transaction_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_ltv_usd,

    -- порядковий номер транзакції цього гравця
    row_number() OVER (
        PARTITION BY player_id
        ORDER BY transaction_ts
    ) AS purchase_sequence_number
FROM completed_tx
