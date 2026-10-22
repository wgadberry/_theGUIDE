CREATE TABLE `tx_token_balance_change` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `tx_id` bigint NOT NULL,
  `block_time` bigint unsigned DEFAULT NULL,
  `token_account_address_id` int unsigned NOT NULL,
  `owner_address_id` int unsigned NOT NULL,
  `token_id` bigint NOT NULL,
  `decimals` tinyint unsigned NOT NULL,
  `pre_balance` decimal(38,0) NOT NULL,
  `post_balance` decimal(38,0) NOT NULL,
  `change_amount` decimal(38,0) NOT NULL,
  `change_type` enum('inc','dec') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_tx_token_account` (`tx_id`,`token_account_address_id`),
  KEY `idx_tx_owner_token` (`tx_id`,`owner_address_id`,`token_id`),
  KEY `idx_tbc_acct_token_blocktime` (`token_account_address_id`,`token_id`,`block_time` DESC,`post_balance`),
  KEY `idx_tbc_owner_token_blocktime` (`owner_address_id`,`token_id`,`block_time` DESC,`post_balance`)
) ENGINE=InnoDB AUTO_INCREMENT=35248 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
