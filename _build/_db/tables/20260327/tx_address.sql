-- tx_address table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_address` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `address` varchar(44) NOT NULL,
  `address_type` enum('program','pool','mint','vault','wallet','ata','unknown') DEFAULT NULL,
  `parent_id` int unsigned DEFAULT NULL,
  `program_id` int unsigned DEFAULT NULL,
  `label` varchar(200) DEFAULT NULL,
  `label_source_method` varchar(256) DEFAULT NULL,
  `created_utc` datetime DEFAULT (utc_timestamp()),
  `funded_by_address_id` int unsigned DEFAULT NULL COMMENT 'FK to tx_address - wallet that first funded this address with SOL',
  `funding_tx_id` bigint DEFAULT NULL COMMENT 'FK to tx - transaction that funded this address',
  `funding_amount` bigint unsigned DEFAULT NULL COMMENT 'Amount of SOL received in funding tx (lamports)',
  `first_seen_block_time` bigint unsigned DEFAULT NULL COMMENT 'Block time of first observed transaction',
  `funding_checked_at` timestamp NULL DEFAULT NULL,
  `init_tx_fetched` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `address` (`address`),
  KEY `idx_program` (`program_id`),
  KEY `idx_type` (`address_type`),
  KEY `idx_funded_by` (`funded_by_address_id`),
  KEY `idx_first_seen` (`first_seen_block_time`),
  KEY `idx_funding_checked` (`funding_checked_at`),
  KEY `idx_funding_lookup` (`address_type`,`funded_by_address_id`,`funding_checked_at`),
  KEY `idx_init_tx_fetched` (`init_tx_fetched`),
  CONSTRAINT `tx_address_ibfk_funded_by` FOREIGN KEY (`funded_by_address_id`) REFERENCES `tx_address` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=90145 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
