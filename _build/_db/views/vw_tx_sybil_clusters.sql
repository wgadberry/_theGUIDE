-- vw_tx_sybil_clusters view
-- Generated from t16o_db instance

DROP VIEW IF EXISTS `vw_tx_sybil_clusters`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `vw_tx_sybil_clusters` AS select `f`.`address` AS `funder_address`,`f`.`label` AS `funder_label`,count(distinct `w`.`id`) AS `wallets_funded`,group_concat(distinct `w`.`address` order by `w`.`first_seen_block_time` ASC separator ', ') AS `funded_wallets`,(sum(`w`.`funding_amount`) / 1e9) AS `total_sol_distributed`,min(`w`.`first_seen_block_time`) AS `first_funding`,max(`w`.`first_seen_block_time`) AS `last_funding`,((max(`w`.`first_seen_block_time`) - min(`w`.`first_seen_block_time`)) / 60) AS `minutes_span` from (`tx_address` `w` join `tx_address` `f` on((`w`.`funded_by_address_id` = `f`.`id`))) where (`w`.`address_type` in ('wallet','unknown')) group by `f`.`id`,`f`.`address`,`f`.`label` having (count(distinct `w`.`id`) >= 3);
