-- tx_guide_type table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `tx_guide_type` (
  `id` tinyint unsigned NOT NULL AUTO_INCREMENT,
  `type_code` varchar(30) NOT NULL COMMENT 'Machine-readable code',
  `type_name` varchar(50) DEFAULT NULL COMMENT 'Human-readable name',
  `category` enum('transfer','swap','fee','account','lending','staking','liquidity','bridge','perp','nft','other') NOT NULL,
  `direction` enum('outflow','inflow','neutral') DEFAULT NULL COMMENT 'From perspective of from_address',
  `risk_weight` tinyint unsigned DEFAULT '10' COMMENT 'Risk score 0-100 (higher = more suspicious)',
  `indicator` bigint unsigned DEFAULT '0',
  `description` varchar(200) DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_code` (`type_code`),
  KEY `idx_category` (`category`),
  KEY `idx_risk` (`risk_weight`)
) ENGINE=InnoDB AUTO_INCREMENT=43 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
