CREATE TABLE `tx_sol_balance_change` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `tx_id` bigint NOT NULL,
  `block_time` bigint unsigned DEFAULT NULL,
  `address_id` int unsigned NOT NULL,
  `pre_balance` bigint unsigned NOT NULL,
  `post_balance` bigint unsigned NOT NULL,
  `change_amount` bigint NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_tx_address` (`tx_id`,`address_id`),
  KEY `idx_sbc_addr_blocktime` (`address_id`,`block_time` DESC,`post_balance`)
) ENGINE=InnoDB AUTO_INCREMENT=140656 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
