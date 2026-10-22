CREATE TABLE `tx_activity_type_map` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `activity_type` varchar(50) NOT NULL,
  `guide_type_id` tinyint unsigned DEFAULT NULL,
  `creates_edge` tinyint DEFAULT '1' COMMENT '1=creates tx_guide edge, 0=skip',
  `edge_direction` enum('in','out','both','none') DEFAULT 'out',
  `description` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `activity_type` (`activity_type`),
  KEY `guide_type_id` (`guide_type_id`),
  CONSTRAINT `tx_activity_type_map_ibfk_1` FOREIGN KEY (`guide_type_id`) REFERENCES `tx_guide_type` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
