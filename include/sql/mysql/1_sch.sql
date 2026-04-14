USE game_db;

CREATE TABLE IF NOT EXISTS purchase_transactions (
  transaction_id         VARCHAR(40)    NOT NULL PRIMARY KEY,
  player_id              BIGINT         NOT NULL,
  transaction_timestamp  DATETIME       NOT NULL,
  session_id             VARCHAR(40)    NULL,
  store_offer_id         VARCHAR(40)    NOT NULL,
  product_sku            VARCHAR(80)    NOT NULL,
  product_name           VARCHAR(120)   NOT NULL,
  product_category       VARCHAR(32)    NOT NULL,
  quantity               INT            NOT NULL,
  gross_amount           DECIMAL(10,2)  NOT NULL,
  currency_code          CHAR(3)        NOT NULL,
  payment_provider       VARCHAR(32)    NOT NULL,
  payment_status         VARCHAR(16)    NOT NULL,
  platform               VARCHAR(16)    NOT NULL,
  country_code           CHAR(2)        NOT NULL,
  is_first_purchase      TINYINT(1)     NOT NULL,
  created_at             DATETIME       NOT NULL,
  updated_at             DATETIME       NOT NULL,
  INDEX idx_player (player_id),
  INDEX idx_ts (transaction_timestamp)
);