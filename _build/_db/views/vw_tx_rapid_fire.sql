-- vw_tx_rapid_fire view
-- Generated from t16o_db instance

DROP VIEW IF EXISTS `vw_tx_rapid_fire`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `vw_tx_rapid_fire` AS select `a`.`address` AS `address`,cast(from_unixtime(`g`.`block_time`) as date) AS `activity_date`,hour(from_unixtime(`g`.`block_time`)) AS `activity_hour`,count(0) AS `tx_count`,count(distinct `g`.`token_id`) AS `tokens_touched`,sum((case when (`gt`.`type_code` = 'swap_in') then 1 else 0 end)) AS `swaps`,sum((case when (`gt`.`type_code` = 'spl_transfer') then 1 else 0 end)) AS `transfers`,min(`g`.`block_time`) AS `first_tx`,max(`g`.`block_time`) AS `last_tx`,(max(`g`.`block_time`) - min(`g`.`block_time`)) AS `seconds_span` from ((`tx_address` `a` join `tx_guide` `g` on((`a`.`id` = `g`.`from_address_id`))) join `tx_guide_type` `gt` on((`g`.`edge_type_id` = `gt`.`id`))) group by `a`.`id`,`a`.`address`,cast(from_unixtime(`g`.`block_time`) as date),hour(from_unixtime(`g`.`block_time`)) having (count(0) >= 10);
