CREATE TABLE `prj_analysis_score` (
  `id` char(36) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT (uuid()),
  `snapshot_id` char(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `score_type` varchar(48) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'sybil | bot | activity | wash_trade | anomaly',
  `score` decimal(6,4) NOT NULL COMMENT 'Normalised 0.0000–1.0000',
  `confidence` decimal(5,4) DEFAULT NULL COMMENT 'Model confidence 0.0000–1.0000',
  `evidence` json DEFAULT NULL COMMENT 'Supporting signals used by the scoring model',
  `model_version` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Scoring model version — enables re-score comparisons',
  `scored_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `archived_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `prj_as_uq_score` (`snapshot_id`,`score_type`,`model_version`),
  KEY `prj_as_idx_snapshot_id` (`snapshot_id`),
  KEY `prj_as_idx_score_type` (`score_type`),
  KEY `prj_as_idx_score` (`score`),
  KEY `prj_as_idx_model_version` (`model_version`),
  KEY `prj_as_idx_archived_at` (`archived_at`),
  CONSTRAINT `prj_as_fk_snapshot` FOREIGN KEY (`snapshot_id`) REFERENCES `prj_result_snapshot` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Automated detection scores per snapshot — sybil, bot, activity, etc.';
