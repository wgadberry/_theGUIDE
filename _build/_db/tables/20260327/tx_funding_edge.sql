-- tx_funding_edge table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_funding_edge` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `from_address_id` int unsigned NOT NULL,
  `to_address_id` int unsigned NOT NULL,
  `total_sol` decimal(30,9) DEFAULT '0.000000000',
  `total_tokens` decimal(38,9) DEFAULT '0.000000000',
  `transfer_count` int unsigned DEFAULT '0',
  `first_transfer_time` bigint unsigned DEFAULT NULL,
  `last_transfer_time` bigint unsigned DEFAULT NULL,
  `created_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_edge` (`from_address_id`,`to_address_id`),
  KEY `idx_from` (`from_address_id`),
  KEY `idx_to` (`to_address_id`),
  KEY `idx_total_sol` (`total_sol`),
  KEY `idx_last_transfer` (`last_transfer_time`)
) ENGINE=InnoDB AUTO_INCREMENT=29690 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
