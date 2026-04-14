{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'purchase_transactions') }}

),

renamed as (

    select
        cast(transaction_id        as varchar)   as transaction_id,
        cast(player_id             as bigint)    as player_id,
        cast(transaction_timestamp as timestamp) as transaction_ts,
        cast(session_id            as varchar)   as session_id,
        cast(store_offer_id        as varchar)   as store_offer_id,
        cast(product_sku           as varchar)   as product_sku,
        cast(product_name          as varchar)   as product_name,
        cast(product_category      as varchar)   as product_category,
        cast(quantity              as integer)   as quantity,
        cast(gross_amount          as decimal(10,2)) as gross_amount,
        cast(currency_code         as varchar)   as currency_code,
        cast(payment_provider      as varchar)   as payment_provider,
        cast(payment_status        as varchar)   as payment_status,
        cast(platform              as varchar)   as platform,
        cast(country_code          as varchar)   as country_code,
        cast(is_first_purchase     as boolean)   as is_first_purchase,
        cast(created_at            as timestamp) as created_at,
        cast(updated_at            as timestamp) as updated_at,
        date_trunc('day', cast(transaction_timestamp as timestamp)) as transaction_date

    from source

)

select * from renamed
