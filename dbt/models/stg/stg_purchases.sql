WITH source AS (

    SELECT * FROM {{ source('raw', 'purchase_transactions') }}

),

renamed AS (

    SELECT
        cast(transaction_id AS varchar) AS transaction_id,
        cast(player_id AS bigint) AS player_id,
        cast(transaction_timestamp AS timestamp) AS transaction_ts,
        cast(session_id AS varchar) AS session_id,
        cast(store_offer_id AS varchar) AS store_offer_id,
        cast(product_sku AS varchar) AS product_sku,
        cast(product_name AS varchar) AS product_name,
        cast(product_category AS varchar) AS product_category,
        cast(quantity AS integer) AS quantity,
        cast(gross_amount AS decimal(10,2)) AS gross_amount,
        cast(currency_code AS varchar) AS currency_code,
        cast(payment_provider AS varchar) AS payment_provider,
        cast(payment_status AS varchar) AS payment_status,
        cast(platform AS varchar) AS platform,
        cast(country_code AS varchar) AS country_code,
        cast(is_first_purchase AS boolean) AS is_first_purchase,
        cast(created_at AS timestamp) AS created_at,
        cast(updated_at AS timestamp) AS updated_at,
        date_trunc('day', cast(transaction_timestamp AS timestamp)) AS transaction_date
    FROM source

)

SELECT * FROM renamed
