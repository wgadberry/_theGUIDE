-- tx_token_price table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_token_price` (
  `token_id` bigint unsigned NOT NULL,
  `date` date NOT NULL,
  `price` decimal(24,12) NOT NULL,
  PRIMARY KEY (`token_id`,`date`),
  KEY `idx_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
