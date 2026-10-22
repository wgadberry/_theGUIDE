-- vw_tx_funding_chain view
-- Generated from t16o_db instance

DROP VIEW IF EXISTS `vw_tx_funding_chain`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `vw_tx_funding_chain` AS select `w`.`id` AS `wallet_id`,`w`.`address` AS `wallet_address`,`w`.`label` AS `wallet_label`,`f1`.`id` AS `funder_1_id`,`f1`.`address` AS `funder_1_address`,`f1`.`label` AS `funder_1_label`,`f2`.`id` AS `funder_2_id`,`f2`.`address` AS `funder_2_address`,`f2`.`label` AS `funder_2_label`,(`w`.`funding_amount` / 1e9) AS `funding_sol`,`t1`.`signature` AS `funding_tx_signature`,CAST(`t1`.`tx_state` AS UNSIGNED) AS `tx_state`,from_unixtime(`w`.`first_seen_block_time`) AS `first_seen_utc` from (((`tx_address` `w` left join `tx_address` `f1` on((`w`.`funded_by_address_id` = `f1`.`id`))) left join `tx_address` `f2` on((`f1`.`funded_by_address_id` = `f2`.`id`))) left join `tx` `t1` on((`w`.`funding_tx_id` = `t1`.`id`))) where ((`w`.`address_type` in ('wallet','unknown')) and (`w`.`funded_by_address_id` is not null));
