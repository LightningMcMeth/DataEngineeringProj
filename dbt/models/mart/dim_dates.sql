-- Календарний вимір. Генерується від найранішої події до сьогодні.
-- Матеріалізація: table (з dbt_project.yml).

WITH bounds AS (

    SELECT
        least(
            (SELECT min(event_date) FROM {{ ref('stg_events') }}),
            (SELECT min(transaction_date) FROM {{ ref('stg_purchases') }})
        ) AS min_date,
        greatest(
            (SELECT max(event_date) FROM {{ ref('stg_events') }}),
            (SELECT max(transaction_date) FROM {{ ref('stg_purchases') }})
        ) AS max_date

),

calendar AS (

    SELECT
        cast(unnest(
            generate_series(
                (SELECT min_date FROM bounds),
                (SELECT max_date FROM bounds),
                INTERVAL 1 DAY
            )
        ) AS date) AS date_day

)

SELECT
    date_day,
    extract(YEAR FROM date_day) AS year,
    extract(MONTH FROM date_day) AS month,
    extract(DAY FROM date_day) AS day_of_month,
    extract(DOW FROM date_day) AS day_of_week,
    extract(WEEK FROM date_day) AS iso_week,
    CASE WHEN extract(DOW FROM date_day) IN (0, 6) THEN true ELSE false END AS is_weekend
FROM calendar
