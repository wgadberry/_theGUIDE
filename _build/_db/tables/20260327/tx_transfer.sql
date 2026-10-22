-- tx_transfer table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_transfer` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `tx_id` bigint NOT NULL COMMENT 'FK to tx.id',
  `ins_index` smallint DEFAULT NULL,
  `outer_ins_index` smallint DEFAULT NULL,
  `transfer_type` varchar(50) DEFAULT NULL,
  `program_id` bigint unsigned DEFAULT NULL COMMENT 'FK to tx_program',
  `outer_program_id` bigint unsigned DEFAULT NULL COMMENT 'FK to tx_program',
  `token_id` bigint DEFAULT NULL COMMENT 'FK to tx_token',
  `decimals` tinyint unsigned DEFAULT NULL,
  `amount` bigint unsigned DEFAULT NULL,
  `source_address_id` int unsigned DEFAULT NULL COMMENT 'FK to tx_address - source token account',
  `source_owner_address_id` int unsigned DEFAULT NULL COMMENT 'FK to tx_address - source owner wallet',
  `destination_address_id` int unsigned DEFAULT NULL COMMENT 'FK to tx_address - dest token account',
  `destination_owner_address_id` int unsigned DEFAULT NULL COMMENT 'FK to tx_address - dest owner wallet',
  `base_token_id` bigint DEFAULT NULL COMMENT 'FK to tx_token - base value token',
  `base_decimals` tinyint unsigned DEFAULT NULL,
  `base_amount` bigint unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_tx` (`tx_id`),
  KEY `idx_token` (`token_id`),
  KEY `idx_source_owner` (`source_owner_address_id`),
  KEY `idx_dest_owner` (`destination_owner_address_id`),
  KEY `idx_tx_ins` (`tx_id`,`ins_index`),
  KEY `tx_transfer_ibfk_program` (`program_id`),
  KEY `tx_transfer_ibfk_outer_program` (`outer_program_id`),
  KEY `tx_transfer_ibfk_source` (`source_address_id`),
  KEY `tx_transfer_ibfk_dest` (`destination_address_id`),
  KEY `tx_transfer_ibfk_base_token` (`base_token_id`),
  CONSTRAINT `tx_transfer_ibfk_base_token` FOREIGN KEY (`base_token_id`) REFERENCES `tx_token` (`id`),
  CONSTRAINT `tx_transfer_ibfk_dest` FOREIGN KEY (`destination_address_id`) REFERENCES `tx_address` (`id`),
  CONSTRAINT `tx_transfer_ibfk_dest_owner` FOREIGN KEY (`destination_owner_address_id`) REFERENCES `tx_address` (`id`),
  CONSTRAINT `tx_transfer_ibfk_outer_program` FOREIGN KEY (`outer_program_id`) REFERENCES `tx_program` (`id`),
  CONSTRAINT `tx_transfer_ibfk_program` FOREIGN KEY (`program_id`) REFERENCES `tx_program` (`id`),
  CONSTRAINT `tx_transfer_ibfk_source` FOREIGN KEY (`source_address_id`) REFERENCES `tx_address` (`id`),
  CONSTRAINT `tx_transfer_ibfk_source_owner` FOREIGN KEY (`source_owner_address_id`) REFERENCES `tx_address` (`id`),
  CONSTRAINT `tx_transfer_ibfk_token` FOREIGN KEY (`token_id`) REFERENCES `tx_token` (`id`),
  CONSTRAINT `tx_transfer_ibfk_tx` FOREIGN KEY (`tx_id`) REFERENCES `tx` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5734 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
