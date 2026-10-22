-- tx_token_holder table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_token_holder` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `token_id` bigint unsigned NOT NULL,
  `owner_id` bigint unsigned NOT NULL,
  `token_account_id` bigint unsigned NOT NULL,
  `amount` decimal(38,0) NOT NULL,
  `decimals` tinyint unsigned NOT NULL,
  `rank` int unsigned DEFAULT NULL,
  `updated_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_token_owner` (`token_id`,`owner_id`),
  KEY `idx_owner` (`owner_id`),
  KEY `idx_token_rank` (`token_id`,`rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
