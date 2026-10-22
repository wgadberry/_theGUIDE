-- sp_tx_parse_detail_v2: Process detail data from pre-populated temp tables
-- Zero JSON_TABLE calls — Python pre-resolves all IDs and populates:
--   tmp_detail_txs    (tx_hash, block_id, block_time, fee_payer_id)
--   tmp_detail_sol    (tx_hash, block_time, address_id, pre/post/change)
--   tmp_detail_token  (tx_hash, block_time, address_id, owner_id, token_id, decimals, pre/post/change, change_type)

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_parse_detail_v2`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_parse_detail_v2`(
    IN p_request_log_id      BIGINT UNSIGNED,
    OUT p_tx_count            INT,
    OUT p_sol_balance_count   INT,
    OUT p_token_balance_count INT,
    OUT p_skipped_count       INT
)
BEGIN
    -- Feature flag constant
    DECLARE FEATURE_BALANCE_CHANGES INT UNSIGNED DEFAULT 1;

    DECLARE v_features INT UNSIGNED DEFAULT 31;
    DECLARE v_searched_addresses JSON DEFAULT NULL;
    DECLARE v_collect_all_balances BOOLEAN;
    DECLARE v_total_input INT DEFAULT 0;

    -- ----------------------------------------------------------------
    -- 1. Get features and searched_addresses from tx_request_log
    -- ----------------------------------------------------------------
    IF p_request_log_id IS NOT NULL THEN
        SELECT COALESCE(features, 31),
               JSON_EXTRACT(payload_summary, '$.filters.addresses')
          INTO v_features, v_searched_addresses
          FROM tx_request_log
         WHERE id = p_request_log_id;
    END IF;

    SET v_collect_all_balances = (v_features & FEATURE_BALANCE_CHANGES) = FEATURE_BALANCE_CHANGES;

    -- Count input rows for skipped calculation
    SELECT COUNT(*) INTO v_total_input FROM tmp_detail_txs;

    -- ----------------------------------------------------------------
    -- 2. Skeleton tx INSERT (detailer may arrive before decoder)
    -- ----------------------------------------------------------------
    INSERT IGNORE INTO tx (signature, block_id, block_time, block_time_utc, tx_state)
    SELECT tx_hash, block_id, block_time, FROM_UNIXTIME(block_time), 0
      FROM tmp_detail_txs;

    -- ----------------------------------------------------------------
    -- 3. Build tx_id lookup in a tracking temp table
    -- ----------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_tx_resolved;
    CREATE TEMPORARY TABLE tmp_detail_tx_resolved (
        tx_hash VARCHAR(88) NOT NULL PRIMARY KEY,
        tx_id   BIGINT UNSIGNED NOT NULL
    ) ENGINE=MEMORY;

    INSERT INTO tmp_detail_tx_resolved (tx_hash, tx_id)
    SELECT d.tx_hash, t.id
      FROM tmp_detail_txs d
      JOIN tx t ON t.signature = d.tx_hash;

    SET p_tx_count = ROW_COUNT();

    -- ----------------------------------------------------------------
    -- 4. Insert SOL balance changes from tmp_detail_sol
    -- ----------------------------------------------------------------
    INSERT IGNORE INTO tx_sol_balance_change (
        tx_id, block_time, address_id,
        pre_balance, post_balance, change_amount
    )
    SELECT r.tx_id,
           s.block_time,
           s.address_id,
           CAST(s.pre_balance AS UNSIGNED),
           CAST(s.post_balance AS UNSIGNED),
           CAST(s.change_amount AS SIGNED)
      FROM tmp_detail_sol s
      JOIN tmp_detail_tx_resolved r ON r.tx_hash = s.tx_hash
     WHERE s.address_id IS NOT NULL
       AND (v_collect_all_balances
            OR v_searched_addresses IS NULL
            OR EXISTS (
                SELECT 1
                  FROM tx_address a
                 WHERE a.id = s.address_id
                   AND JSON_CONTAINS(v_searched_addresses, JSON_QUOTE(a.address))
            ));

    SET p_sol_balance_count = ROW_COUNT();

    -- ----------------------------------------------------------------
    -- 5. Insert token balance changes from tmp_detail_token
    -- ----------------------------------------------------------------
    INSERT IGNORE INTO tx_token_balance_change (
        tx_id, block_time, token_account_address_id, owner_address_id,
        token_id, decimals, pre_balance, post_balance,
        change_amount, change_type
    )
    SELECT r.tx_id,
           t.block_time,
           t.address_id,
           t.owner_id,
           t.token_id,
           COALESCE(t.decimals, 0),
           t.pre_balance,
           t.post_balance,
           t.change_amount,
           CASE
               WHEN t.change_type IS NOT NULL THEN t.change_type
               WHEN t.change_amount >= 0 THEN 'inc'
               ELSE 'dec'
           END
      FROM tmp_detail_token t
      JOIN tmp_detail_tx_resolved r ON r.tx_hash = t.tx_hash
     WHERE t.address_id IS NOT NULL
       AND t.owner_id IS NOT NULL
       AND t.token_id IS NOT NULL
       AND (v_collect_all_balances
            OR v_searched_addresses IS NULL
            OR EXISTS (
                SELECT 1
                  FROM tx_address a
                 WHERE a.id = t.owner_id
                   AND JSON_CONTAINS(v_searched_addresses, JSON_QUOTE(a.address))
            ));

    SET p_token_balance_count = ROW_COUNT();

    -- ----------------------------------------------------------------
    -- 6. Fix signer_address_id from fee_payer
    -- ----------------------------------------------------------------
    UPDATE tx t
      JOIN tmp_detail_tx_resolved r ON r.tx_id = t.id
      JOIN tmp_detail_txs d ON d.tx_hash = r.tx_hash
       SET t.signer_address_id = d.fee_payer_id
     WHERE d.fee_payer_id IS NOT NULL;

    -- ----------------------------------------------------------------
    -- 7. Stamp tx_state bit 16 (SWAPS / detail complete)
    -- ----------------------------------------------------------------
    UPDATE tx t
      JOIN tmp_detail_tx_resolved r ON r.tx_id = t.id
       SET t.tx_state = t.tx_state | 16;

    -- ----------------------------------------------------------------
    -- 8. Set skipped count and cleanup
    -- ----------------------------------------------------------------
    SET p_skipped_count = v_total_input - p_tx_count;

    DROP TEMPORARY TABLE IF EXISTS tmp_detail_tx_resolved;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_txs;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_sol;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_token;

END;;

DELIMITER ;
