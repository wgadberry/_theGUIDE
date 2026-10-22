-- config table
-- Generated from t16o_db instance

DROP TABLE IF EXISTS ;

CREATE TABLE `config` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `config_type` varchar(64) NOT NULL COMMENT 'Category: rabbitmq, worker, rpc, database, fetcher, logging, feature',
  `config_key` varchar(64) NOT NULL COMMENT 'Configuration key name',
  `config_value` varchar(1024) NOT NULL COMMENT 'Configuration value (stored as string)',
  `value_type` enum('string','int','decimal','bool','json') NOT NULL DEFAULT 'string' COMMENT 'Data type for parsing',
  `description` varchar(512) DEFAULT NULL COMMENT 'Human-readable description',
  `default_value` varchar(1024) DEFAULT NULL COMMENT 'Default value if not set',
  `min_value` varchar(32) DEFAULT NULL COMMENT 'Minimum value for numeric types',
  `max_value` varchar(32) DEFAULT NULL COMMENT 'Maximum value for numeric types',
  `is_sensitive` tinyint NOT NULL DEFAULT '0' COMMENT 'If true, value should be masked in logs/UI',
  `is_runtime_editable` tinyint NOT NULL DEFAULT '1' COMMENT 'If true, can be changed without restart',
  `requires_restart` tinyint NOT NULL DEFAULT '0' COMMENT 'If true, requires service restart to take effect',
  `version` int NOT NULL DEFAULT '1' COMMENT 'Increments on each update for change detection',
  `created_utc` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_utc` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `updated_by` varchar(64) DEFAULT NULL COMMENT 'User/service that made the last update',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_config_type_key` (`config_type`,`config_key`),
  KEY `idx_config_type` (`config_type`),
  KEY `idx_updated_utc` (`updated_utc`)
) ENGINE=InnoDB AUTO_INCREMENT=166 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Runtime configuration storage for T16O services';
