CREATE TABLE `tx_guide_stage` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `seq` bigint unsigned NOT NULL COMMENT 'Monotonic sequence from config table â€” groups rows from one SP call',
  `guide_id` bigint unsigned NOT NULL COMMENT 'FK to tx_guide.id â€” the surviving edge',
  `tx_id` bigint unsigned NOT NULL COMMENT 'FK to tx.id â€” denormalized for fast tx grouping',
  `block_time` bigint unsigned DEFAULT NULL COMMENT 'Denormalized from tx.block_time',
  `direction` varchar(4) DEFAULT NULL COMMENT 'in/out/swap (wallet mode) or NULL (graph mode)',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_seq` (`seq`),
  KEY `idx_seq_guide` (`seq`,`guide_id`),
  KEY `idx_seq_tx` (`seq`,`tx_id`),
  KEY `idx_created` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=24652 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
