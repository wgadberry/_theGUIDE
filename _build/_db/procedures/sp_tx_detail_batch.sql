-- sp_tx_detail_batch stored procedure
-- Processes decoded + detail JSON from Solscan to populate balance change tables
--
-- Input JSON structure:
-- {
--   "decoded": { "success": true, "data": [...] },
--   "detail": { "success": true, "data": [
--     {
--       "tx_hash": "...",
--       "sol_bal_change": [ { "address": "...", "pre_balance": N, "post_balance": N, "change_amount": N } ],
--       "token_bal_change": [ { "address": "...", "owner": "...", "token_address": "...", "decimals": N,
--                               "pre_balance": "N", "post_balance": "N", "change_amount": "N" } ]
--     }
--   ]}
-- }

DELIMITER ;;

DROP PROCEDURE IF EXISTS `sp_tx_detail_batch`;;

CREATE PROCEDURE `sp_tx_detail_batch`(
    IN p_json LONGTEXT,
    OUT p_tx_count INT,
    OUT p_detail_count INT
)
BEGIN
    DECLARE v_sol_count INT DEFAULT 0;
    DECLARE v_token_count INT DEFAULT 0;

    SET p_tx_count = 0;
    SET p_detail_count = 0;

    -- =========================================================================
    -- PHASE 1: Extract transactions from detail response
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_tx;
    CREATE TEMPORARY TABLE tmp_detail_tx (
        tx_hash VARCHAR(90) NOT NULL,
        tx_id BIGINT,
        PRIMARY KEY (tx_hash)
    ) ENGINE=MEMORY;

    -- Get tx_hash from detail.data
    INSERT IGNORE INTO tmp_detail_tx (tx_hash)
    SELECT tx_hash
    FROM JSON_TABLE(p_json, '$.detail.data[*]' COLUMNS (
        tx_hash VARCHAR(90) PATH '$.tx_hash'
    )) AS jt
    WHERE tx_hash IS NOT NULL;

    SELECT COUNT(*) INTO p_tx_count FROM tmp_detail_tx;

    -- Lookup tx_id for each signature
    UPDATE tmp_detail_tx tdt
    JOIN tx t ON t.signature = tdt.tx_hash
    SET tdt.tx_id = t.id;

    -- =========================================================================
    -- PHASE 2: Extract and ensure all addresses exist
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_address;
    CREATE TEMPORARY TABLE tmp_detail_address (
        address VARCHAR(44) NOT NULL,
        address_type ENUM('program','pool','mint','vault','wallet','ata','unknown') DEFAULT 'wallet',
        address_id INT UNSIGNED,
        PRIMARY KEY (address)
    ) ENGINE=MEMORY;

    -- Collect all addresses from SOL balance changes
    INSERT IGNORE INTO tmp_detail_address (address, address_type)
    SELECT DISTINCT address, 'wallet'
    FROM JSON_TABLE(p_json, '$.detail.data[*].sol_bal_change[*]' COLUMNS (
        address VARCHAR(44) PATH '$.address'
    )) AS jt
    WHERE address IS NOT NULL;

    -- Collect token account addresses from token balance changes
    INSERT IGNORE INTO tmp_detail_address (address, address_type)
    SELECT DISTINCT address, 'ata'
    FROM JSON_TABLE(p_json, '$.detail.data[*].token_bal_change[*]' COLUMNS (
        address VARCHAR(44) PATH '$.address'
    )) AS jt
    WHERE address IS NOT NULL;

    -- Collect owner addresses from token balance changes
    INSERT IGNORE INTO tmp_detail_address (address, address_type)
    SELECT DISTINCT owner, 'wallet'
    FROM JSON_TABLE(p_json, '$.detail.data[*].token_bal_change[*]' COLUMNS (
        owner VARCHAR(44) PATH '$.owner'
    )) AS jt
    WHERE owner IS NOT NULL;

    -- Collect token mint addresses from token balance changes
    INSERT IGNORE INTO tmp_detail_address (address, address_type)
    SELECT DISTINCT token_address, 'mint'
    FROM JSON_TABLE(p_json, '$.detail.data[*].token_bal_change[*]' COLUMNS (
        token_address VARCHAR(44) PATH '$.token_address'
    )) AS jt
    WHERE token_address IS NOT NULL;

    -- Ensure all addresses exist in tx_address
    INSERT INTO tx_address (address, address_type)
    SELECT address, address_type FROM tmp_detail_address
    ON DUPLICATE KEY UPDATE id = id;

    -- Lookup address IDs
    UPDATE tmp_detail_address tda
    JOIN tx_address a ON a.address = tda.address
    SET tda.address_id = a.id;

    -- Create copy of address table for owner lookups (MySQL can't reopen same temp table)
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_owner_address;
    CREATE TEMPORARY TABLE tmp_detail_owner_address (
        address VARCHAR(44) NOT NULL,
        address_id INT UNSIGNED,
        PRIMARY KEY (address)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_detail_owner_address (address, address_id)
    SELECT address, address_id FROM tmp_detail_address;

    -- =========================================================================
    -- PHASE 3: Ensure tokens exist
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_token;
    CREATE TEMPORARY TABLE tmp_detail_token (
        token_address VARCHAR(44) NOT NULL,
        decimals TINYINT UNSIGNED,
        address_id INT UNSIGNED,
        token_id BIGINT,
        PRIMARY KEY (token_address)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_detail_token (token_address, decimals)
    SELECT DISTINCT token_address, decimals
    FROM JSON_TABLE(p_json, '$.detail.data[*].token_bal_change[*]' COLUMNS (
        token_address VARCHAR(44) PATH '$.token_address',
        decimals TINYINT UNSIGNED PATH '$.decimals'
    )) AS jt
    WHERE token_address IS NOT NULL;

    -- Lookup address IDs for tokens
    UPDATE tmp_detail_token tdt
    JOIN tmp_detail_address tda ON tda.address = tdt.token_address
    SET tdt.address_id = tda.address_id;

    -- Ensure tokens exist (classify fungible at insert; decimals=0 needs API for nft vs semi_fungible)
    INSERT INTO tx_token (mint_address_id, decimals, token_type)
    SELECT address_id, COALESCE(decimals, 0),
           CASE WHEN COALESCE(decimals, 0) >= 1 THEN 'fungible' ELSE NULL END
    FROM tmp_detail_token
    WHERE address_id IS NOT NULL
    ON DUPLICATE KEY UPDATE
        decimals = COALESCE(VALUES(decimals), tx_token.decimals),
        token_type = COALESCE(tx_token.token_type, VALUES(token_type));

    -- Lookup token IDs
    UPDATE tmp_detail_token tdt
    JOIN tx_token t ON t.mint_address_id = tdt.address_id
    SET tdt.token_id = t.id;

    -- =========================================================================
    -- PHASE 4: Insert SOL balance changes
    -- =========================================================================
    INSERT INTO tx_sol_balance_change (tx_id, address_id, pre_balance, post_balance, change_amount)
    SELECT
        tt.tx_id,
        a.address_id,
        jt.pre_balance,
        jt.post_balance,
        jt.change_amount
    FROM JSON_TABLE(p_json, '$.detail.data[*]' COLUMNS (
        tx_hash VARCHAR(90) PATH '$.tx_hash',
        NESTED PATH '$.sol_bal_change[*]' COLUMNS (
            address VARCHAR(44) PATH '$.address',
            pre_balance BIGINT UNSIGNED PATH '$.pre_balance',
            post_balance BIGINT UNSIGNED PATH '$.post_balance',
            change_amount BIGINT PATH '$.change_amount'
        )
    )) AS jt
    JOIN tmp_detail_tx tt ON tt.tx_hash = jt.tx_hash
    JOIN tmp_detail_address a ON a.address = jt.address
    WHERE tt.tx_id IS NOT NULL
      AND a.address_id IS NOT NULL
      AND jt.address IS NOT NULL
      AND jt.change_amount != 0  -- Skip zero-change records
    ON DUPLICATE KEY UPDATE
        pre_balance = VALUES(pre_balance),
        post_balance = VALUES(post_balance),
        change_amount = VALUES(change_amount);

    SET v_sol_count = ROW_COUNT();

    -- =========================================================================
    -- PHASE 5: Insert token balance changes
    -- =========================================================================
    INSERT INTO tx_token_balance_change (
        tx_id, token_account_address_id, owner_address_id, token_id,
        decimals, pre_balance, post_balance, change_amount, change_type
    )
    SELECT
        tt.tx_id,
        ta.address_id,
        oa.address_id,
        tok.token_id,
        COALESCE(jt.decimals, 0),
        CAST(jt.pre_balance AS DECIMAL(38,0)),
        CAST(jt.post_balance AS DECIMAL(38,0)),
        CAST(jt.change_amount AS DECIMAL(38,0)),
        CASE WHEN CAST(jt.change_amount AS DECIMAL(38,0)) >= 0 THEN 'inc' ELSE 'dec' END
    FROM JSON_TABLE(p_json, '$.detail.data[*]' COLUMNS (
        tx_hash VARCHAR(90) PATH '$.tx_hash',
        NESTED PATH '$.token_bal_change[*]' COLUMNS (
            address VARCHAR(44) PATH '$.address',
            owner VARCHAR(44) PATH '$.owner',
            token_address VARCHAR(44) PATH '$.token_address',
            decimals TINYINT UNSIGNED PATH '$.decimals',
            pre_balance VARCHAR(50) PATH '$.pre_balance',
            post_balance VARCHAR(50) PATH '$.post_balance',
            change_amount VARCHAR(50) PATH '$.change_amount'
        )
    )) AS jt
    JOIN tmp_detail_tx tt ON tt.tx_hash = jt.tx_hash
    JOIN tmp_detail_address ta ON ta.address = jt.address
    JOIN tmp_detail_owner_address oa ON oa.address = jt.owner
    JOIN tmp_detail_token tok ON tok.token_address = jt.token_address
    WHERE tt.tx_id IS NOT NULL
      AND ta.address_id IS NOT NULL
      AND oa.address_id IS NOT NULL
      AND tok.token_id IS NOT NULL
      AND jt.address IS NOT NULL
      AND CAST(jt.change_amount AS DECIMAL(38,0)) != 0  -- Skip zero-change records
    ON DUPLICATE KEY UPDATE
        pre_balance = VALUES(pre_balance),
        post_balance = VALUES(post_balance),
        change_amount = VALUES(change_amount),
        change_type = VALUES(change_type);

    SET v_token_count = ROW_COUNT();

    -- =========================================================================
    -- PHASE 6: Update tx_state bitmask - set DETAILED bit (64)
    -- tx_state bit 6 = 64 = DETAILED
    -- =========================================================================
    UPDATE tx t
    JOIN tmp_detail_tx tt ON tt.tx_id = t.id
    SET t.tx_state = t.tx_state | 64
    WHERE tt.tx_id IS NOT NULL;

    -- =========================================================================
    -- CLEANUP
    -- =========================================================================
    SET p_detail_count = v_sol_count + v_token_count;

    DROP TEMPORARY TABLE IF EXISTS tmp_detail_tx;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_address;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_owner_address;
    DROP TEMPORARY TABLE IF EXISTS tmp_detail_token;

END;;

DELIMITER ;
