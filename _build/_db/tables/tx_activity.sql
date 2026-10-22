CREATE TABLE `tx_activity` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `tx_id` bigint NOT NULL COMMENT 'FK to tx.id',
  `ins_index` smallint DEFAULT NULL,
  `outer_ins_index` smallint DEFAULT NULL,
  `name` varchar(50) DEFAULT NULL,
  `activity_type` varchar(50) DEFAULT NULL,
  `program_id` bigint unsigned DEFAULT NULL COMMENT 'FK to tx_program',
  `outer_program_id` bigint unsigned DEFAULT NULL COMMENT 'FK to tx_program',
  `account_address_id` int unsigned DEFAULT NULL COMMENT 'FK to tx_address - actor wallet',
  `guide_loaded` tinyint DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tx_ins` (`tx_id`,`ins_index`,`outer_ins_index`),
  KEY `idx_activity_tx` (`tx_id`,`activity_type`,`account_address_id`)
) ENGINE=InnoDB AUTO_INCREMENT=26897 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
