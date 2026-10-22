CREATE TABLE `tx_api_key` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `api_key` varchar(64) NOT NULL,
  `client_id` varchar(64) NOT NULL DEFAULT 'demo',
  `client_secret` varchar(64) NOT NULL DEFAULT 'a3f1b2c4-d5e6-7890-abcd-ef1234567890',
  `name` varchar(100) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `permissions` json DEFAULT NULL,
  `rate_limit` int unsigned DEFAULT '100',
  `active` tinyint(1) DEFAULT '1',
  `feature_mask` int unsigned DEFAULT '0',
  `created_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `last_used_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `api_key` (`api_key`),
  KEY `idx_active` (`active`),
  KEY `idx_client` (`client_id`,`client_secret`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
