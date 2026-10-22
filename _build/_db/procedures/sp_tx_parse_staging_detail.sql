-- sp_tx_parse_staging_detail stored procedure
-- Processes detailed (tx_state=16) staging data into balance change tables
-- FULLY BATCH - no loops, all operations use JSON_TABLE
--
-- Builds MEMORY temp tables (tmp_detail_tx, tmp_detail_addr) to pre-resolve
-- VARCHAR lookups once, then sub-SPs JOIN against those instead of InnoDB.
--
-- Feature flags control what balance data is collected:
--   FEATURE_BALANCE_CHANGES (1): Collect ALL balance changes vs only searched addresses

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_parse_staging_detail`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_parse_staging_detail`(
    IN p_staging_id INT,
    OUT p_tx_count INT,
    OUT p_sol_balance_count INT,
    OUT p_token_balance_count INT,
    OUT p_skipped_count INT
)
BEGIN
    DECLARE v_txs_json JSON;
    DECLARE v_shredded_state INT;
    DECLARE v_total_txs INT;
    DECLARE v_request_log_id BIGINT UNSIGNED;
    DECLARE v_features INT UNSIGNED DEFAULT 0;
    DECLARE v_searched_addresses JSON DEFAULT NULL;

    SET p_tx_count = 0;
    SET p_sol_balance_count = 0;
    SET p_token_balance_count = 0;
    SET p_skipped_count = 0;

    SET v_shredded_state = CAST(fn_get_config('tx_state', 'shredded') AS UNSIGNED);

    SELECT txs, request_log_id INTO v_txs_json, v_request_log_id
    FROM t16o_db_staging.txs
    WHERE id = p_staging_id;

    IF v_request_log_id IS NOT NULL THEN
        SELECT
            COALESCE(features, 0),
            JSON_EXTRACT(payload_summary, '$.filters.addresses')
        INTO v_features, v_searched_addresses
        FROM tx_request_log
        WHERE id = v_request_log_id;
    END IF;

    IF v_txs_json IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Staging row not found';
    END IF;

    SET v_total_txs = JSON_LENGTH(v_txs_json, '$.data');

    IF v_total_txs IS NULL OR v_total_txs = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No transactions in staging row';
    END IF;

    -- Ensure fee payer addresses exist before building lookup tables
    INSERT IGNORE INTO tx_address (address, address_type)
    SELECT DISTINCT j.fee_payer, 'wallet'
    FROM JSON_TABLE(
        v_txs_json,
        '$.data[*]' COLUMNS (
            fee_payer VARCHAR(44) PATH '$.signer[0]'
        )
    ) j
    WHERE j.fee_payer IS NOT NULL AND j.fee_payer != 'null';

    -- =========================================================================
    -- Pre-resolve lookups into MEMORY temp tables (one InnoDB pass each)
    -- Sub-SPs JOIN against these instead of hitting tx/tx_address per row
    -- =========================================================================

    DROP TEMPORARY TABLE IF EXISTS tmp_detail_tx;
    CREATE TEMPORARY TABLE tmp_detail_tx (
        tx_hash VARCHAR(88) NOT NULL PRIMARY KEY,
        tx_id BIGINT UNSIGNED NOT NULL
    ) ENGINE=MEMORY;

    INSERT INTO tmp_detail_tx (tx_hash, tx_id)
    SELECT j.tx_hash, tx.id
    FROM JSON_TABLE(
        v_txs_json,
        '$.data[*]' COLUMNS (
            tx_hash VARCHAR(88) PATH '$.tx_hash'
        )
    ) AS j
    JOIN tx ON tx.signature = j.tx_hash;

    SET p_tx_count = ROW_COUNT();
    SET p_skipped_count = v_total_txs - p_tx_count;

    DROP TEMPORARY TABLE IF EXISTS tmp_detail_addr;
    CREATE TEMPORARY TABLE tmp_detail_addr (
        address VARCHAR(44) NOT NULL PRIMARY KEY,
        address_id INT UNSIGNED NOT NULL
    ) ENGINE=MEMORY;

    INSERT INTO tmp_detail_addr (address, address_id)
    SELECT a.address, a.id
    FROM tx_address a
    WHERE a.address IN (
        SELECT DISTINCT addr FROM (
            -- SOL balance addresses
            SELECT j.address AS addr
            FROM JSON_TABLE(v_txs_json, '$.data[*].sol_bal_change[*]' COLUMNS (
                address VARCHAR(44) PATH '$.address'
            )) j WHERE j.address IS NOT NULL

            UNION

            -- Token balance: ATA addresses
            SELECT j.address
            FROM JSON_TABLE(v_txs_json, '$.data[*].token_bal_change[*]' COLUMNS (
                address VARCHAR(44) PATH '$.address'
            )) j WHERE j.address IS NOT NULL

            UNION

            -- Token balance: owner addresses
            SELECT j.owner
            FROM JSON_TABLE(v_txs_json, '$.data[*].token_bal_change[*]' COLUMNS (
                owner VARCHAR(44) PATH '$.owner'
            )) j WHERE j.owner IS NOT NULL

            UNION

            -- Token balance: mint addresses
            SELECT j.token_address
            FROM JSON_TABLE(v_txs_json, '$.data[*].token_bal_change[*]' COLUMNS (
                token_address VARCHAR(44) PATH '$.token_address'
            )) j WHERE j.token_address IS NOT NULL

            UNION

            -- Fee payer (signer)
            SELECT j.fee_payer
            FROM JSON_TABLE(v_txs_json, '$.data[*]' COLUMNS (
                fee_payer VARCHAR(44) PATH '$.signer[0]'
            )) j WHERE j.fee_payer IS NOT NULL AND j.fee_payer != 'null'
        ) AS all_addrs
    );

    -- Clone addr table: MySQL can't reopen same temp table multiple times in one query
    -- Token balance needs 3 addr joins (ata, owner, mint) so we need 2 copies
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_addr2;
    CREATE TEMPORARY TABLE tmp_detail_addr2 (
        address VARCHAR(44) NOT NULL PRIMARY KEY,
        address_id INT UNSIGNED NOT NULL
    ) ENGINE=MEMORY;
    INSERT INTO tmp_detail_addr2 SELECT * FROM tmp_detail_addr;

    DROP TEMPORARY TABLE IF EXISTS tmp_detail_addr3;
    CREATE TEMPORARY TABLE tmp_detail_addr3 (
        address VARCHAR(44) NOT NULL PRIMARY KEY,
        address_id INT UNSIGNED NOT NULL
    ) ENGINE=MEMORY;
    INSERT INTO tmp_detail_addr3 SELECT * FROM tmp_detail_addr;

    -- =========================================================================
    -- Call sub-SPs (they JOIN against tmp_detail_tx + tmp_detail_addr/2/3)
    -- =========================================================================

    CALL sp_tx_insert_sol_balance(v_txs_json, v_features, v_searched_addresses, p_sol_balance_count);
    CALL sp_tx_insert_token_balance(v_txs_json, v_features, v_searched_addresses, p_token_balance_count);

    -- Fix signer_address_id from the actual fee payer (signer[0] in detailed JSON).
    -- The decode stage sets it from one_line_summary.data.account which is the activity
    -- account, not the tx signer. The detailed RPC response has the true signer.
    UPDATE tx t
    JOIN JSON_TABLE(
        v_txs_json,
        '$.data[*]' COLUMNS (
            tx_hash VARCHAR(88) PATH '$.tx_hash',
            fee_payer VARCHAR(44) PATH '$.signer[0]'
        )
    ) j ON 1=1
    JOIN tmp_detail_tx dt ON dt.tx_hash = j.tx_hash
    JOIN tmp_detail_addr da ON da.address = j.fee_payer
    SET t.signer_address_id = da.address_id
    WHERE t.id = dt.tx_id
      AND j.fee_payer IS NOT NULL;

    UPDATE tx t
    JOIN tmp_detail_tx dt ON dt.tx_id = t.id
    SET t.tx_state = t.tx_state | 16;

    DROP TEMPORARY TABLE IF EXISTS tmp_detail_tx;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_addr;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_addr2;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_addr3;

    UPDATE t16o_db_staging.txs
    SET tx_state = v_shredded_state
    WHERE id = p_staging_id;
END;;

DELIMITER ;
