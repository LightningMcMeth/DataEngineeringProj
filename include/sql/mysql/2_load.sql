USE game_db;

LOAD DATA INFILE '/docker-entrypoint-initdb.d/purchase_transactions.csv'
INTO TABLE purchase_transactions
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(transaction_id, player_id, @ts, @sid, store_offer_id, product_sku, product_name,
 product_category, quantity, gross_amount, currency_code, payment_provider,
 payment_status, platform, country_code, is_first_purchase, @cat, @upd)
SET
  transaction_timestamp = STR_TO_DATE(@ts,  '%Y-%m-%dT%H:%i:%sZ'),
  session_id            = NULLIF(@sid, ''),
  created_at            = STR_TO_DATE(@cat, '%Y-%m-%dT%H:%i:%sZ'),
  updated_at            = STR_TO_DATE(@upd, '%Y-%m-%dT%H:%i:%sZ');