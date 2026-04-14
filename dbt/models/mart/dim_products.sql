-- Вимір продуктів (SKU). Унікальні SKU з транзакцій + збагачення категорією.

WITH sku_base AS (

    SELECT
        product_sku,
        any_value(product_name) AS product_name,
        any_value(product_category) AS product_category,
        min(transaction_ts) AS first_sold_ts,
        count(*) AS times_sold,
        sum(gross_amount) AS total_gross_usd
    FROM {{ ref('stg_purchases') }}
    WHERE payment_status = 'completed'
    GROUP BY product_sku

)

SELECT
    s.product_sku,
    s.product_name,
    s.product_category,
    g.category_group,
    g.is_subscription,
    s.first_sold_ts,
    s.times_sold,
    s.total_gross_usd
FROM sku_base AS s
LEFT JOIN {{ ref('product_category_groups') }} AS g ON s.product_category = g.product_category
