-- tx_activity table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

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
  `data_json` json DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_tx` (`tx_id`),
  KEY `idx_activity_type` (`activity_type`),
  KEY `idx_program` (`program_id`),
  KEY `idx_account` (`account_address_id`),
  KEY `idx_tx_ins` (`tx_id`,`ins_index`),
  KEY `tx_activity_ibfk_outer_program` (`outer_program_id`),
  CONSTRAINT `tx_activity_ibfk_account` FOREIGN KEY (`account_address_id`) REFERENCES `tx_address` (`id`),
  CONSTRAINT `tx_activity_ibfk_outer_program` FOREIGN KEY (`outer_program_id`) REFERENCES `tx_program` (`id`),
  CONSTRAINT `tx_activity_ibfk_program` FOREIGN KEY (`program_id`) REFERENCES `tx_program` (`id`),
  CONSTRAINT `tx_activity_ibfk_tx` FOREIGN KEY (`tx_id`) REFERENCES `tx` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=6436 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
