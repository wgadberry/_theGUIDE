-- vw_tx_funding_tree view
-- Generated from t16o_db instance

DROP VIEW IF EXISTS `vw_tx_funding_tree`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `vw_tx_funding_tree` AS select `w`.`id` AS `wallet_id`,`w`.`address` AS `wallet_address`,`w`.`address_type` AS `wallet_type`,`w`.`label` AS `wallet_label`,`f`.`id` AS `funder_id`,`f`.`address` AS `funder_address`,`f`.`address_type` AS `funder_type`,`f`.`label` AS `funder_label`,(`w`.`funding_amount` / 1e9) AS `funding_sol`,`w`.`funding_tx_id` AS `funding_tx_id`,from_unixtime(`w`.`first_seen_block_time`) AS `first_seen_utc`,`t`.`signature` AS `funding_tx_signature`,CAST(`t`.`tx_state` AS UNSIGNED) AS `tx_state` from ((`tx_address` `w` left join `tx_address` `f` on((`w`.`funded_by_address_id` = `f`.`id`))) left join `tx` `t` on((`w`.`funding_tx_id` = `t`.`id`))) where (`w`.`address_type` in ('wallet','unknown'));
