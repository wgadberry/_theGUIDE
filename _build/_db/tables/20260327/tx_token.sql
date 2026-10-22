-- tx_token table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_token` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `mint_address_id` int unsigned NOT NULL COMMENT 'FK to tx_address - token mint',
  `token_name` varchar(256) DEFAULT NULL,
  `token_symbol` varchar(256) DEFAULT NULL,
  `token_icon` varchar(500) DEFAULT NULL,
  `decimals` tinyint unsigned DEFAULT NULL,
  `supply` bigint unsigned DEFAULT NULL COMMENT 'Total supply from Solscan API',
  `token_type` enum('fungible','semi_fungible','nft','unknown') DEFAULT NULL COMMENT 'Derived: fungible (dec>=1), semi_fungible (dec=0 supply>1), nft (dec=0 supply<=1 no mint auth)',
  `created_utc` datetime NOT NULL DEFAULT (UTC_TIMESTAMP()),
  `updated_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_address` (`mint_address_id`),
  KEY `idx_symbol` (`token_symbol`),
  CONSTRAINT `tx_token_ibfk_address` FOREIGN KEY (`mint_address_id`) REFERENCES `tx_address` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=28292 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
