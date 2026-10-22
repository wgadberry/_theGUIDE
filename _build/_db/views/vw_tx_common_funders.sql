-- vw_tx_common_funders view
-- Generated from t16o_db instance

DROP VIEW IF EXISTS `vw_tx_common_funders`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `vw_tx_common_funders` AS select `f`.`id` AS `funder_id`,`f`.`address` AS `funder_address`,`f`.`label` AS `funder_label`,count(`w`.`id`) AS `wallets_funded`,(sum(`w`.`funding_amount`) / 1e9) AS `total_sol_distributed`,min(`w`.`first_seen_block_time`) AS `first_funding_time`,max(`w`.`first_seen_block_time`) AS `last_funding_time` from (`tx_address` `w` join `tx_address` `f` on((`w`.`funded_by_address_id` = `f`.`id`))) where (`w`.`address_type` in ('wallet','unknown')) group by `f`.`id`,`f`.`address`,`f`.`label` having (count(`w`.`id`) > 1) order by `wallets_funded` desc;
