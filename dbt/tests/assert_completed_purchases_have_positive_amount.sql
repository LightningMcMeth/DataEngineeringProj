SELECT
    transaction_id,
    player_id,
    quantity,
    gross_amount,
    payment_status
FROM {{ ref('stg_purchases') }}
WHERE payment_status = 'completed'
    AND (
        quantity <= 0
        OR gross_amount <= 0
)