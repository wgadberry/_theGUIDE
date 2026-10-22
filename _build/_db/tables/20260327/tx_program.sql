-- tx_program table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_program` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `program_address_id` int unsigned NOT NULL COMMENT 'FK to tx_address',
  `name` varchar(100) DEFAULT NULL,
  `program_type` enum('dex','lending','nft','token','system','compute','router','other') DEFAULT 'other',
  `is_verified` tinyint(1) DEFAULT '0',
  `created_utc` datetime NOT NULL DEFAULT (UTC_TIMESTAMP()),
  `updated_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_address_id` (`program_address_id`),
  KEY `idx_name` (`name`),
  KEY `idx_type` (`program_type`),
  CONSTRAINT `tx_program_ibfk_address` FOREIGN KEY (`program_address_id`) REFERENCES `tx_address` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8433 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
