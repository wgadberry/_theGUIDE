-- tx_pool table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_pool` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `pool_address_id` int unsigned NOT NULL COMMENT 'FK to tx_address - pool/amm address',
  `program_id` bigint unsigned DEFAULT NULL COMMENT 'FK to tx_program - DEX program',
  `token1_id` bigint DEFAULT NULL COMMENT 'FK to tx_token - token 1',
  `token2_id` bigint DEFAULT NULL COMMENT 'FK to tx_token - token 2',
  `token_account1_id` int unsigned DEFAULT NULL COMMENT 'FK to tx_address - vault/token account 1',
  `token_account2_id` int unsigned DEFAULT NULL COMMENT 'FK to tx_address - vault/token account 2',
  `lp_token_id` bigint DEFAULT NULL COMMENT 'FK to tx_token - LP token mint',
  `pool_label` varchar(200) DEFAULT NULL COMMENT 'Human-readable pool label from tx_address.label',
  `first_seen_tx_id` bigint DEFAULT NULL COMMENT 'FK to tx - first transaction seen',
  `created_utc` datetime NOT NULL DEFAULT (UTC_TIMESTAMP()),
  `updated_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_address` (`pool_address_id`),
  KEY `idx_program` (`program_id`),
  KEY `idx_token1` (`token1_id`),
  KEY `idx_token2` (`token2_id`),
  KEY `tx_pool_ibfk_tx` (`first_seen_tx_id`),
  KEY `idx_lp_token` (`lp_token_id`),
  CONSTRAINT `tx_pool_ibfk_address` FOREIGN KEY (`pool_address_id`) REFERENCES `tx_address` (`id`),
  CONSTRAINT `tx_pool_ibfk_program` FOREIGN KEY (`program_id`) REFERENCES `tx_program` (`id`),
  CONSTRAINT `tx_pool_ibfk_token1` FOREIGN KEY (`token1_id`) REFERENCES `tx_token` (`id`),
  CONSTRAINT `tx_pool_ibfk_token2` FOREIGN KEY (`token2_id`) REFERENCES `tx_token` (`id`),
  CONSTRAINT `tx_pool_ibfk_tx` FOREIGN KEY (`first_seen_tx_id`) REFERENCES `tx` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1162 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
