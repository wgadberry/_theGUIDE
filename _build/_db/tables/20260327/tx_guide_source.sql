-- tx_guide_source table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_guide_source` (
  `id` tinyint unsigned NOT NULL AUTO_INCREMENT,
  `source_code` varchar(30) NOT NULL COMMENT 'Table name or source identifier',
  `source_name` varchar(50) DEFAULT NULL COMMENT 'Human-readable name',
  `description` varchar(200) DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_code` (`source_code`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
