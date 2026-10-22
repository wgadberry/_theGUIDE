CREATE TABLE `tx_state_phase` (
  `bit_position` tinyint unsigned NOT NULL,
  `bit_value` bigint unsigned NOT NULL,
  `phase_code` varchar(32) NOT NULL,
  `phase_name` varchar(64) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `worker_name` varchar(64) DEFAULT NULL,
  `created_utc` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`bit_position`),
  UNIQUE KEY `phase_code` (`phase_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
