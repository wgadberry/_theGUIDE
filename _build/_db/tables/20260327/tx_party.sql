-- tx_party table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_party` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `tx_id` bigint NOT NULL,
  `owner_id` int unsigned NOT NULL,
  `token_account_id` int unsigned DEFAULT NULL,
  `mint_id` int unsigned NOT NULL,
  `account_index` smallint unsigned DEFAULT NULL COMMENT 'Index in account_keys array',
  `party_type` enum('party','counterparty') NOT NULL DEFAULT 'party',
  `balance_type` enum('SOL','TOKEN') NOT NULL DEFAULT 'TOKEN',
  `action_type` enum('fee','rent','transfer','transferChecked','burn','mint','swap','createAccount','closeAccount','stake','unstake','reward','airdrop','unknown') DEFAULT NULL COMMENT 'Type of action that caused this balance change',
  `counterparty_owner_id` int unsigned DEFAULT NULL COMMENT 'Links to the counterparty owner address',
  `pre_amount` bigint DEFAULT NULL,
  `post_amount` bigint DEFAULT NULL,
  `amount_change` bigint DEFAULT NULL,
  `decimals` tinyint unsigned DEFAULT NULL,
  `pre_ui_amount` decimal(30,9) DEFAULT NULL,
  `post_ui_amount` decimal(30,9) DEFAULT NULL,
  `ui_amount_change` decimal(30,9) DEFAULT NULL,
  `created_utc` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tx_owner_mint_acct` (`tx_id`,`owner_id`,`mint_id`,`account_index`),
  KEY `idx_owner` (`owner_id`),
  KEY `idx_mint` (`mint_id`),
  KEY `idx_token_account` (`token_account_id`),
  KEY `idx_owner_mint` (`owner_id`,`mint_id`),
  KEY `idx_amount_change` (`amount_change`),
  KEY `idx_party_type` (`party_type`),
  KEY `idx_counterparty` (`counterparty_owner_id`),
  KEY `idx_balance_type` (`balance_type`),
  KEY `idx_action_type` (`action_type`),
  CONSTRAINT `tx_party_ibfk_1` FOREIGN KEY (`tx_id`) REFERENCES `tx` (`id`) ON DELETE CASCADE,
  CONSTRAINT `tx_party_ibfk_2` FOREIGN KEY (`owner_id`) REFERENCES `tx_address` (`id`) ON DELETE RESTRICT,
  CONSTRAINT `tx_party_ibfk_3` FOREIGN KEY (`token_account_id`) REFERENCES `tx_address` (`id`) ON DELETE RESTRICT,
  CONSTRAINT `tx_party_ibfk_4` FOREIGN KEY (`mint_id`) REFERENCES `tx_address` (`id`) ON DELETE RESTRICT,
  CONSTRAINT `tx_party_ibfk_5` FOREIGN KEY (`counterparty_owner_id`) REFERENCES `tx_address` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
