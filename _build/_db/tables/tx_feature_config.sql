CREATE TABLE `tx_feature_config` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `feature_name` varchar(50) NOT NULL,
  `feature_mask` int unsigned NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `is_billable` tinyint(1) NOT NULL DEFAULT '1',
  `created_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `feature_name` (`feature_name`),
  KEY `idx_feature_mask` (`feature_mask`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
