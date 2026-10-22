-- tx_token_market table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_token_market` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `pool_id` bigint unsigned NOT NULL,
  `program_id` bigint unsigned DEFAULT NULL,
  `token1_id` bigint unsigned DEFAULT NULL,
  `token2_id` bigint unsigned DEFAULT NULL,
  `token_account1_id` bigint unsigned DEFAULT NULL,
  `token_account2_id` bigint unsigned DEFAULT NULL,
  `total_tvl` decimal(24,6) DEFAULT NULL,
  `total_volume_24h` decimal(24,6) DEFAULT NULL,
  `total_volume_prev_24h` decimal(24,6) DEFAULT NULL,
  `total_trades_24h` int unsigned DEFAULT NULL,
  `total_trades_prev_24h` int unsigned DEFAULT NULL,
  `num_traders_24h` int unsigned DEFAULT NULL,
  `updated_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pool` (`pool_id`),
  KEY `idx_token1` (`token1_id`),
  KEY `idx_token2` (`token2_id`),
  KEY `idx_program` (`program_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1829 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
