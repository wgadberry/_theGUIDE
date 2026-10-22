CREATE TABLE `prj_annotation` (
  `id` char(36) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT (uuid()),
  `snapshot_id` char(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` char(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `annotation_type` varchar(48) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'note | flag | link | sybil | bot | forensic',
  `body` text COLLATE utf8mb4_unicode_ci,
  `metadata` json DEFAULT NULL COMMENT 'Type-specific structured payload',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `deleted_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `prj_an_idx_snapshot_id` (`snapshot_id`),
  KEY `prj_an_idx_user_id` (`user_id`),
  KEY `prj_an_idx_annotation_type` (`annotation_type`),
  KEY `prj_an_idx_deleted_at` (`deleted_at`),
  CONSTRAINT `prj_an_fk_snapshot` FOREIGN KEY (`snapshot_id`) REFERENCES `prj_result_snapshot` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='User and system annotations attached to result snapshots';
