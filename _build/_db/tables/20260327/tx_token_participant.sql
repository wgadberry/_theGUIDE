-- tx_token_participant table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_token_participant` (
  `token_id` bigint NOT NULL,
  `address_id` int unsigned NOT NULL,
  `first_seen` bigint unsigned DEFAULT NULL,
  `last_seen` bigint unsigned DEFAULT NULL,
  `buy_count` int unsigned DEFAULT '0',
  `sell_count` int unsigned DEFAULT '0',
  `transfer_in_count` int unsigned DEFAULT '0',
  `transfer_out_count` int unsigned DEFAULT '0',
  `buy_volume` decimal(30,9) DEFAULT '0.000000000',
  `sell_volume` decimal(30,9) DEFAULT '0.000000000',
  `net_position` decimal(30,9) DEFAULT '0.000000000',
  `created_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`token_id`,`address_id`),
  KEY `idx_address` (`address_id`),
  KEY `idx_token_sellers` (`token_id`,`sell_count` DESC),
  KEY `idx_token_buyers` (`token_id`,`buy_count` DESC),
  KEY `idx_token_volume` (`token_id`,`sell_volume` DESC),
  KEY `idx_last_seen` (`last_seen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
