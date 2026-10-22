-- sp_tx_parse_decode stored procedure
-- Processes decoded (tx_state=8) JSON data into tx tables
-- FULLY BATCH - no loops, all operations use JSON_TABLE
-- Receives JSON directly from caller — no staging table dependency
--
-- Feature flags are retrieved from tx_request_log.features (linked via request_log_id)
-- and control data collection behavior:
--   FEATURE_ALL_ADDRESSES (2): Collect extended addresses (ATAs, vaults, pools)
--   FEATURE_SWAP_ROUTING (4): Collect all swap hops vs only top-level swaps

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_parse_decode`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_parse_decode`(
    IN p_txs_json JSON,
    IN p_request_log_id BIGINT UNSIGNED,
    IN p_tx_origin TINYINT UNSIGNED,
    OUT p_tx_count INT,
    OUT p_transfer_count INT,
    OUT p_swap_count INT,
    OUT p_activity_count INT,
    OUT p_skipped_count INT
)
BEGIN
    DECLARE v_features INT UNSIGNED DEFAULT 31;

    SET p_tx_count = 0;
    SET p_transfer_count = 0;
    SET p_swap_count = 0;
    SET p_activity_count = 0;
    SET p_skipped_count = 0;

    IF p_request_log_id IS NOT NULL THEN
        SELECT COALESCE(features, 31) INTO v_features
        FROM tx_request_log
        WHERE id = p_request_log_id;
    END IF;

    IF p_txs_json IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No JSON data provided';
    END IF;

    IF JSON_LENGTH(p_txs_json, '$.data') IS NULL OR JSON_LENGTH(p_txs_json, '$.data') = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No transactions in JSON data';
    END IF;

    CALL sp_tx_prepopulate_lookups(p_txs_json, p_request_log_id, v_features);
    CALL sp_tx_insert_txs_batch(p_txs_json, p_request_log_id, p_tx_origin, p_tx_count, p_skipped_count);
    CALL sp_tx_insert_transfers(p_txs_json, p_transfer_count);
    CALL sp_tx_insert_swaps(p_txs_json, v_features, p_swap_count);
    CALL sp_tx_insert_activities(p_txs_json, v_features, p_activity_count);

    UPDATE tx_transfer t
    JOIN tmp_batch_tx_signatures b ON b.tx_id = t.tx_id AND b.is_new = 1
    JOIN tx_activity a ON a.tx_id = t.tx_id
                       AND a.ins_index = t.ins_index
                       AND a.outer_ins_index = t.outer_ins_index
    SET t.activity_id = a.id
    WHERE t.activity_id IS NULL;

    UPDATE tx_swap s
    JOIN tmp_batch_tx_signatures b ON b.tx_id = s.tx_id AND b.is_new = 1
    JOIN tx_activity a ON a.tx_id = s.tx_id
                       AND a.ins_index = s.ins_index
                       AND a.outer_ins_index = s.outer_ins_index
    SET s.activity_id = a.id
    WHERE s.activity_id IS NULL;

    UPDATE tx t
    JOIN tmp_batch_tx_signatures b ON b.tx_id = t.id AND b.is_new = 1
    SET t.tx_state = t.tx_state | 4;

    DROP TEMPORARY TABLE IF EXISTS tmp_batch_tx_signatures;
END;;

DELIMITER ;
