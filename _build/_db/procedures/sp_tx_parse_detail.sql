-- sp_tx_parse_detail stored procedure
-- Processes detailed (tx_state=16) JSON data into balance change tables
-- FULLY BATCH - no loops, all operations use JSON_TABLE
-- Receives JSON directly from caller -- no staging table dependency
--
-- Builds MEMORY temp tables (tmp_detail_tx, tmp_detail_addr) to pre-resolve
-- VARCHAR lookups once, then sub-SPs JOIN against those instead of InnoDB.
--
-- Feature flags control what balance data is collected:
--   FEATURE_BALANCE_CHANGES (1): Collect ALL balance changes vs only searched addresses

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_parse_detail`;;

CREATE DEFINER=`root`@`%` PROCEDURE `sp_tx_parse_detail`(
    IN p_txs_json JSON,
    IN p_request_log_id BIGINT UNSIGNED,
    OUT p_tx_count INT,
    OUT p_sol_balance_count INT,
    OUT p_token_balance_count INT,
    OUT p_skipped_count INT
)
BEGIN
    DECLARE v_total_txs INT;
    DECLARE v_features INT UNSIGNED DEFAULT 31;
    DECLARE v_searched_addresses JSON DEFAULT NULL;

    SET p_tx_count = 0;
    SET p_sol_balance_count = 0;
    SET p_token_balance_count = 0;
    SET p_skipped_count = 0;

    IF p_request_log_id IS NOT NULL THEN
        SELECT
            COALESCE(features, 31),
            JSON_EXTRACT(payload_summary, '$.filters.addresses')
        INTO v_features, v_searched_addresses
        FROM tx_request_log
        WHERE id = p_request_log_id;
    END IF;

    IF p_txs_json IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No JSON data provided';
    END IF;

    SET v_total_txs = JSON_LENGTH(p_txs_json, '$.data');

    IF v_total_txs IS NULL OR v_total_txs = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No transactions in JSON data';
    END IF;

    -- Ensure fee payer addresses exist before building lookup tables
    INSERT IGNORE INTO tx_address (address, address_type)
    SELECT DISTINCT j.fee_payer, 'wallet'
    FROM JSON_TABLE(
        p_txs_json,
        '$.data[*]' COLUMNS (
            fee_payer VARCHAR(44) PATH '$.signer[0]'
        )
    ) j
    WHERE j.fee_payer IS NOT NULL AND j.fee_payer != 'null';

    -- =========================================================================
    -- Skeleton tx insert: if detailer arrives before decoder, create minimal
    -- tx records so balance data is not lost. Decoder will fill in agg fields
    -- and fee/priority_fee via ON DUPLICATE KEY UPDATE when it runs later.
    -- =========================================================================
    INSERT IGNORE INTO tx (signature, block_id, block_time, block_time_utc, tx_state)
    SELECT DISTINCT j.tx_hash, j.block_id, j.block_time, FROM_UNIXTIME(j.block_time), 0
    FROM JSON_TABLE(
        p_txs_json,
        '$.data[*]' COLUMNS (
            tx_hash VARCHAR(88) PATH '$.tx_hash',
            block_id BIGINT UNSIGNED PATH '$.block_id',
            block_time BIGINT UNSIGNED PATH '$.block_time'
        )
    ) j
    WHERE j.tx_hash IS NOT NULL;

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
        p_txs_json,
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
            SELECT j.address AS addr
            FROM JSON_TABLE(p_txs_json, '$.data[*].sol_bal_change[*]' COLUMNS (
                address VARCHAR(44) PATH '$.address'
            )) j WHERE j.address IS NOT NULL

            UNION

            SELECT j.address
            FROM JSON_TABLE(p_txs_json, '$.data[*].token_bal_change[*]' COLUMNS (
                address VARCHAR(44) PATH '$.address'
            )) j WHERE j.address IS NOT NULL

            UNION

            SELECT j.owner
            FROM JSON_TABLE(p_txs_json, '$.data[*].token_bal_change[*]' COLUMNS (
                owner VARCHAR(44) PATH '$.owner'
            )) j WHERE j.owner IS NOT NULL

            UNION

            SELECT j.token_address
            FROM JSON_TABLE(p_txs_json, '$.data[*].token_bal_change[*]' COLUMNS (
                token_address VARCHAR(44) PATH '$.token_address'
            )) j WHERE j.token_address IS NOT NULL

            UNION

            SELECT j.fee_payer
            FROM JSON_TABLE(p_txs_json, '$.data[*]' COLUMNS (
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

    CALL sp_tx_insert_sol_balance(p_txs_json, v_features, v_searched_addresses, p_sol_balance_count);
    CALL sp_tx_insert_token_balance(p_txs_json, v_features, v_searched_addresses, p_token_balance_count);

    -- Fix signer_address_id from the actual fee payer (signer[0] in detailed JSON).
    UPDATE tx t
    JOIN JSON_TABLE(
        p_txs_json,
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
END;;

DELIMITER ;
